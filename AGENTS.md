# AGENTS.md — agent brief for `data_pipeline_template`

This is a shared **dlt + Airflow Fivetran replacement**. The full design is in
[`data_pipeline_plan.md`](data_pipeline_plan.md); this file is a focused brief
for assistants working in the repo (Claude Code, Codex, Cursor, etc.).
`CLAUDE.md` is a symlink to this file so Claude Code reads the same content.

## MCP server

A project-local MCP server is registered in [`.mcp.json`](.mcp.json) and
exposes the four introspection subcommands below as native tools:
`sources_list`, `sources_describe`, `pipelines_validate`, `pipelines_doctor`.
MCP-aware clients should prefer these over shelling out. The CLI surface
documented below remains the source of truth and is what the MCP tools wrap.

## Where pipelines live

User-facing config lives in [`pipelines/`](pipelines/) — one YAML per pipeline.
Adding a new pipeline is almost always a YAML edit (zero Python). Schema:
[`pipelines/_schema.md`](pipelines/_schema.md) (human) +
[`pipelines/_schema.json`](pipelines/_schema.json) (JSON Schema, generated
from pydantic models). Each YAML carries a
`# yaml-language-server: $schema=./_schema.json` directive so LSP-aware
editors validate inline.

The pydantic source of truth is
[`src/data_pipeline_template/config/models.py`](src/data_pipeline_template/config/models.py).
Regenerate the JSON Schema after any model change:

```
python -m data_pipeline_template config schema
```

The `regen-pipeline-schema` pre-commit hook enforces this; it fails the commit
if the on-disk JSON Schema drifts from the pydantic models.

## Logical-connection-name → env-var convention

YAML never holds raw credentials — only a logical `connection:` name that
resolves at runtime. Env-var templates:

- Source: `SOURCES__<TYPE_UPPER>__<CONNECTION_UPPER>__CREDENTIALS`
  - e.g. `SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS=postgresql://...`
- Destination: `DESTINATION__<TYPE_UPPER>__<CONNECTION_UPPER>__CREDENTIALS`
  - e.g. `DESTINATION__POSTGRES__PG_WAREHOUSE__CREDENTIALS=postgresql://...`

dlt-native fallback: the same value can live in `.dlt/secrets.toml` under
`[sources.<type>.<connection>.credentials]` /
`[destination.<type>.<connection>.credentials]`. Local dev: prefer
`.dlt/secrets.toml` (git-ignored). Containers / CI / k8s: prefer env vars.

## Validate without running

```
python -m data_pipeline_template pipelines validate            # all
python -m data_pipeline_template pipelines validate <name>     # one
```

Parses + validates `pipelines/*.yml` against the pydantic schema. Exit 0 if
clean, exit 1 with aggregated errors otherwise. No DB connection, no Airflow
process — sub-second feedback.

## Introspect available source types

```
python -m data_pipeline_template sources list
python -m data_pipeline_template sources describe <type>
```

`describe` prints the env-var template, required + allowed `source.config`
keys, and source-specific notes. Source types are plugin-discovered via the
`data_pipeline_template.sources` entry-point group — no hardcoded list.

## Dry-run a pipeline (extract + normalize, skip destination write)

```
python -m data_pipeline_template run <name> --limit 1 --no-load
```

`--no-load` runs `pipeline.extract()` + `pipeline.normalize()` but skips
`pipeline.load()` — useful for validating source connectivity and schema
inference without touching the destination. `--limit N` caps yields per
resource via `DltSource.add_limit(N)`. Either flag works independently;
`--limit` without `--no-load` is a useful smoke-test mode against real
destinations. Known limitation: stubbed destinations (snowflake, databricks
until Segment 8) raise `NotImplementedError` during `build()` regardless of
`--no-load`.

## Diagnose missing credentials

```
python -m data_pipeline_template pipelines doctor
```

For each pipeline, derives the expected env var from the source +
destination metadata, then probes `os.environ` and `dlt.secrets`. Reports
one of `env / secrets-toml / no-creds-required / MISSING` per slot. Exit
non-zero if any pipeline has `MISSING` rows.

## Orchestrator-agnostic boundary

`pipeline_factory.py` and everything under `sources/`, `destinations/`,
`config/`, and `cli/` must remain Airflow-agnostic. Only
`src/data_pipeline_template/airflow/` and `dags/` may import Airflow. Ruff
enforces via `flake8-tidy-imports`
([`pyproject.toml`](pyproject.toml) under `[tool.ruff.lint.flake8-tidy-imports]`).
Adding `import airflow` outside the allowed subpackages fails CI.

## "Do not edit `airflow.cfg`, env-override instead"

Per Segment 6: the committed `airflow_home/airflow.cfg` is the baseline.
Environment-specific overrides (executor, metadata DB URL, remote logging)
flow through `AIRFLOW__<SECTION>__<KEY>` env vars set in
`docker-compose.yml` / k8s manifests, not by editing the cfg file. This
keeps the same image swappable between LocalExecutor (dev) and
KubernetesExecutor (prod) per Design principle #4.

## CDC operations (Segment 7, `pg_cdc` source)

`pg_cdc` wraps the vendored
[`dlt pg_replication`](src/data_pipeline_template/sources/pg_cdc/_vendor/NOTICE.md)
verified source. Required source-Postgres setup:

- `wal_level=logical` plus non-trivial `max_replication_slots` and
  `max_wal_senders` (docker-compose `postgres-source` ships with all three).
- A connection role with the `REPLICATION` attribute. Use the dedicated
  `replicator` role from
  [`docker/postgres-source-init/01_replication.sql`](docker/postgres-source-init/01_replication.sql)
  for least-privilege prod setups; the existing `source` role is also granted
  REPLICATION for dev ergonomics.
- Each replicated table must own a `REPLICA IDENTITY` (the PK default is
  sufficient; `seed_local.sh` states it explicitly for `orders`).

Slot + publication lifecycle:

- `init_replication` is called eagerly on every `pipeline_factory.build()` —
  the call is idempotent (existing slot/pub are no-ops). The vendor raises
  `RuntimeError` on subsequent calls with `persist_snapshots=True`; the
  builder swallows it.
- Set `source.config.reset: true` in YAML for a one-shot run to drop +
  recreate the slot + publication. Use after an incompatible DDL change
  (see "DDL during streaming" under known limitations).
- Deleting a pipeline YAML does NOT clean up the slot or publication —
  manual `SELECT pg_drop_replication_slot('…');` + `DROP PUBLICATION …;`
  required. A paused pipeline keeps the slot open and retains WAL on the
  source, eventually filling disk. Monitor.

`options.write_disposition` for cdc: the factory silent-promotes the default
`append` → `merge`. `merge` / `replace` pass through unchanged.

Slot-lag sensor (opt-in): wire
[`PgReplicationSlotLagSensor`](src/data_pipeline_template/airflow/sensors.py)
into a separate monitoring DAG when you want to fail on lag spikes. It's not
auto-wired into generated cdc DAGs — alerting routing lands in Segment 9.

## Known limitations

- `source.config` is a free-form `dict[str, Any]` in `PipelineConfig`, so
  the JSON Schema entry for it is an open object. `sources describe`
  reports the per-source allowed / required keys instead — consult it
  before authoring a new pipeline. Typed per-source sub-models are
  deferred (see plan, post-v1).
- `filesystem` source builder is a stub until Segment 8. Its metadata is
  registered but `python -m data_pipeline_template run` on a pipeline that
  uses it will raise `NotImplementedError`.
- **DDL during CDC streaming:** the vendored `pg_replication` halts on
  incompatible schema changes mid-stream. Recovery is a one-shot run with
  `source.config.reset: true` after the schema migration lands.
- **Multi-table publications:** each pipeline owns its own
  `publication_name`; sharing publications across pipelines is unsupported.
- **CDC alerting:** the slot-lag sensor exists but routes nowhere — it
  fails the sensor task on breach. Slack / email routing lands in
  Segment 9.
