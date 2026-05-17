# `config/` — pydantic schema, YAML loader, env overlays

Authoritative schema for `pipelines/*.yml`. Every downstream segment
(`pipeline_factory`, `dag_factory`, source builders, CLI introspection)
consumes the validated [`PipelineConfig`](models.py).

## Files

- [`models.py`](models.py) — pydantic v2 schema. `PipelineConfig` plus the
  per-block models (`SourceConfig` discriminated union, `DestinationConfig`,
  `SyncConfig`, `ScheduleConfig`, `ResourcesConfig`, `AlertsConfig`,
  `QualityConfig`, `BackfillConfig`) and the cross-field validators that
  enforce mode/cursor/primary-key combinations.
- [`loader.py`](loader.py) — discovery + validation + env overlay merge.
- [`__init__.py`](__init__.py) — public re-exports.

## Environment overlays (Segment 13)

Each environment can re-map a narrow set of fields per pipeline without
editing the base YAML. Overlay files live at
`pipelines/_env/<env>.yml` and are merged at load time.

```yaml
# pipelines/_env/prod.yml
orders_pg_to_warehouse:
  destination:
    type: snowflake
    connection: snowflake_warehouse
    dataset: prod_raw_orders
  schedule:
    enabled: true
  resources:
    cpu: "1000m"
    memory: "2Gi"
```

### Resolution

`load_pipelines(root, env=...)` resolves the active env from explicit
arg > `$DLT_ENV` > `"dev"` via `resolve_env()`. CLI subcommands accept
`--env <name>` (`run`, `run-backfill`, `pipelines validate`,
`pipelines doctor`, `pipelines delete`). The DAG entrypoint at
`dags/data_pipeline_dags.py` reads `$DLT_ENV` (one Airflow deployment
per env).

### Allowed overlay scope (v1)

- `source.connection`
- `destination.{type, connection, dataset}`
- `schedule.enabled`
- `resources.{cpu, memory}`

Out-of-scope keys (`sync.*`, `options.*`, `source.config.*`, `alerts.*`,
`quality.*`) raise `ConfigError` at load time via `PipelineOverlay`'s
`extra="forbid"` constraint. Expand the scope by editing
`PipelineOverlay` in `models.py`.

### Merge semantics

Overlays merge at the raw-dict level **before** `PipelineConfig.model_validate`,
so cross-field validators (incremental needs cursor_field, cdc needs
primary_key, merge needs primary_key, etc.) re-run on the merged result.
Leaf keys in `source` / `destination` / `schedule` replace one-by-one so
the base `source.config:` / `destination.dataset:` / `schedule.cron:`
survive unless the overlay explicitly sets them. `resources` replaces
wholesale (cpu + memory ride together).

### Inspect the diff between two envs

```
python -m dlt_data_pipeline pipelines promote <name> --from dev --to prod
```

Informational only — never edits files. The operator "applies" by
setting `DLT_ENV` in their deploy.

## Secrets-resolver precedence

`pipelines doctor` and the runtime credential resolver consult sources in
this order, taking the first hit:

1. **Process env var** — `os.environ.get(<KEY>)`. In k8s, mounted from a
   `Secret` via `envFrom: [secretRef]` on the worker pod template
   ([`deploy/k8s/base/pod-template-base.yaml`](../../../deploy/k8s/base/pod-template-base.yaml)).
   This is the v1 prod path.
2. **Airflow Secrets Backend** — when `AIRFLOW__SECRETS__BACKEND` is set,
   `doctor` reports `airflow-backend` for that slot but does NOT call the
   backend (avoids an Airflow runtime dependency in the CLI). Runtime
   resolution goes through dlt → Airflow's configured backend. This is
   the v2 path; ship details live in
   [`deploy/k8s/base/README.md`](../../../deploy/k8s/base/README.md).
3. **`.dlt/secrets.toml`** — dlt's native TOML file under
   `[sources.<type>.<connection>.credentials]` /
   `[destination.<connection>.credentials]`. Local dev only — git-ignored.

`doctor` statuses reflect this chain: `env` > `airflow-backend` >
`secrets-toml` > `MISSING` (or `no-creds-required` for sources/destinations
that don't need credentials, like local `duckdb` or local-`file://`
filesystem reads).

## Incremental knobs ([`models.py:123-124`](models.py))

`SyncConfig.tolerance_seconds` (default `0`, max `86400`) maps to dlt's
`Incremental.lag`. `SyncConfig.lookback` is an ISO-8601 duration string
(`PT1H`, `P1D`) parsed by `isodate`. Both compose — lookback re-reads a
trailing window, tolerance further widens the comparison threshold for
late writes. Defaults are zero / null — explicit opt-in per pipeline.

```yaml
sync:
  mode: incremental
  cursor_field: updated_at
  primary_key: id
  tolerance_seconds: 30        # absorb 30s of cursor-vs-commit skew
  lookback: PT1H               # always re-read the last hour
```

## Backfill ([`BackfillConfig`](models.py))

```yaml
sync:
  mode: incremental
  cursor_field: updated_at
  primary_key: id
  backfill:
    chunk_size: P7D            # ISO-8601 duration parsed by isodate
    partition_field: updated_at  # optional; defaults to cursor_field
```

Drives `python -m dlt_data_pipeline run-backfill <name> --start <ts>
--end <ts>` — each chunk runs as its own `pipeline.run()` with bounded
`initial_value` / `end_value`. Implementation contract in
[`cli/backfill_cmds.py`](../cli/backfill_cmds.py); operator-facing usage
in [`AGENTS.md`](../../../AGENTS.md).

## Quality ([`QualityConfig`](models.py))

```yaml
quality:
  row_count_check: true
  check_mode: cross_cluster    # default; PythonOperator + two engines
  # check_mode: same_cluster   # SQLCheckOperator on conn_id dlt_<dest.connection>
```

`cross_cluster` (default) opens source + destination engines via
`dlt.secrets` keyed by the YAML's logical connection names — no Airflow
Connection wiring required. `same_cluster` is cheaper but requires an
Airflow Connection `dlt_<destination.connection>` reachable to both
schemas. Generated tasks land via
[`airflow/quality.py`](../airflow/quality.py).

## Per-task SLA ([`models.py:131`](models.py))

`SyncConfig.sla_minutes` plumbs `sla=timedelta(minutes=N)` onto every
task generated inside the `PipelineTasksGroup`; breaches invoke
`sla_miss_callback` → `post_sla_miss_alert` (Segment 14). Airflow 3.x
migration path documented in
[`airflow/README.md`](../airflow/README.md).

## Known issues / troubleshooting

> **Schema evolution:** expose dlt `schema_contract` per pipeline. `evolve`
> default is convenient but silently adds columns; recommend `freeze` for
> regulated destinations. Surface schema-change events in DAG-run metadata
> via `XCom` or task logs.

### Known limitations

- Overlay scope is intentionally narrow. Re-mapping `sync.mode`,
  `options.write_disposition`, `source.config`, `alerts`, or `quality` is
  v2.
- `pipelines promote` is diff-only. Rewriting the target overlay file is
  punted to v2.
- `databricks` destination is still deferred at the factory level
  (Segment 8 scope note); overlay-flipping a pipeline to `databricks`
  fails at `build()` with the existing deferral message, not at overlay
  validation.
