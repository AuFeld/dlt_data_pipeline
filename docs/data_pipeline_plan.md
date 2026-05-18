# Plan: a dlt + Airflow Fivetran replacement

## Context

Goal: a reusable Python template/app that replaces Fivetran — sync data from many sources (REST
APIs, SQL databases, files/S3) to many destinations (Snowflake, Postgres, Databricks),
with full-refresh, incremental, and CDC sync modes.

**Deployment model:** single shared repo that the **data team** uses as a
platform. All Fivetran-replacement syncs live here; engineers add pipelines by
contributing a YAML (and rarely, a new source type). Not a per-project template
to be cloned. This drives multi-author considerations: source-registry
governance, run isolation, and config-conflict avoidance.

Decided tech (locked):
- **Foundation: dlt** (dlt-hub) — extraction, schema inference, normalization,
  incremental loading, destination writes.
- **Orchestration: Apache Airflow** (DAG-based) via the official
  `dlt.helpers.airflow_helper.PipelineTasksGroup` — scheduling, retries,
  observability, sensors. One Airflow task per dlt resource inside a
  `TaskGroup`, one `DAG` per pipeline YAML.
- **Sync modes v1:** full refresh, incremental (cursor/replication-key), CDC
  (Postgres logical replication via dlt `pg_replication`).
- **Deployment: Docker** — Dockerfile + docker-compose for local dev (Airflow
  webserver + scheduler + triggerer + metadata Postgres + source/dest Postgres).

Design principles:
1. A normal user adds a new pipeline by writing **one YAML file** — zero Python
   edits. New source *types* are the only code extension point.
2. **Orchestrator boundary is load-bearing.** `pipeline_factory.py` and
   everything under `sources/`, `destinations/`, `config/` must remain
   **Airflow-agnostic** — no `import airflow` outside `src/dlt_data_pipeline/airflow/`.
   The factory takes a `PipelineConfig` and returns a runnable `dlt.pipeline`;
   the Airflow layer wraps it. This keeps dlt core usable standalone (CLI runs,
   tests, future orchestrator swap) and lets us swap Airflow without rewriting
   the pipeline core. Enforce via:
   - Layout: Airflow code isolated to `src/dlt_data_pipeline/airflow/`.
   - Lint rule: a `ruff` `tidy-imports`/`flake8-tidy-imports` ban on `airflow*`
     imports anywhere outside that subpackage (or a small custom test that
     greps the source tree and fails CI on violations).
   - Test: a unit test runs a pipeline end-to-end via `pipeline_factory.run(name)`
     **without** importing the Airflow package at all, proving the boundary.
3. **Source types are plugins (Python entry points), not hardcoded.**
   `sources/registry.py` does **not** maintain a hardcoded `{name -> builder}`
   dict. Instead it discovers source types via the
   `dlt_data_pipeline.sources` entry-point group at startup. Each source
   type is its own subpackage under `src/dlt_data_pipeline/sources/<name>/`
   (or, later, a separate installable package) that exposes a `builder(config)
   -> dlt.Source` and registers itself via `pyproject.toml`:
   ```toml
   [project.entry-points."dlt_data_pipeline.sources"]
   rest_api = "dlt_data_pipeline.sources.rest_api:builder"
   sql_database = "dlt_data_pipeline.sources.sql_database:builder"
   filesystem = "dlt_data_pipeline.sources.filesystem:builder"
   pg_cdc = "dlt_data_pipeline.sources.pg_cdc:builder"
   ```
   Why: lets the data team add / deprecate / version source types without
   editing a shared registry file (no merge-conflict bottleneck), supports
   eventual out-of-tree source packages, and gives each source a clean
   testable boundary (`Protocol`-typed `builder`).
4. **Compute is per-run isolated via `KubernetesExecutor`.**
   The prod Airflow deployment uses `KubernetesExecutor` so every Airflow task
   runs in a **fresh pod** built from `docker/Dockerfile`. Local
   `docker compose` uses `LocalExecutor` for dev ergonomics, but the codebase
   is written to the `pod_override` contract from day one. Implications baked
   into the design:
   - Secrets reach each pod via env (k8s `Secret` env-mounted on the pod
     template) — never assumed-present on a shared host.
   - Logs ship out (stdout → cluster log aggregator; Airflow remote logging
     to S3/GCS for worker-pod logs); nothing on local disk is durable beyond
     a run.
   - A built image is the deploy unit — `docker/Dockerfile` is the run image,
     CI builds + pushes it, and the pod template references the new tag.
   - Per-pipeline CPU/RAM limits are configurable on the YAML
     (`resources:` block) and flow into per-task
     `executor_config={"pod_override": V1Pod(...)}` so one huge sync can't
     starve smaller ones.

## Pipeline YAML Schema (user-facing)

```yaml
name: orders_pg_to_warehouse
source:
  type: sql_database          # rest_api | sql_database | filesystem | pg_cdc
  connection: pg_source       # logical name -> resolved from secrets.toml / env
  config:
    schema: public
    tables: [orders, customers]
sync:
  mode: incremental           # full_refresh | incremental | cdc
  cursor_field: updated_at    # required for incremental
  primary_key: id
destination:
  type: postgres              # postgres | snowflake | databricks | duckdb
  connection: pg_warehouse
  dataset: raw_orders
schedule:
  cron: "0 */2 * * *"
  enabled: true
options:
  write_disposition: merge    # append | replace | merge
  schema_contract: evolve     # evolve | freeze | discard_row
```

**Secrets:** YAML references a logical connection name only — never raw
credentials. Logical name maps to a dlt section in `.dlt/secrets.toml` (local)
or env vars (containers/CI), e.g.
`DESTINATION__POSTGRES__CREDENTIALS=postgresql://...`. dlt reads env vars with
zero code change. Airflow `Connection`/`Variable` objects are used wherever
operator templates would otherwise render credentials, so the UI mask filter
catches them.

## "Add a pipeline in 10 minutes" workflow

1. `python scripts/new_pipeline.py my_pipeline --source rest_api --dest snowflake`
   scaffolds `pipelines/my_pipeline.yml`.
2. Fill in source/sync/destination/schedule in the YAML.
3. Add credentials to `.dlt/secrets.toml` (local) or env vars (deployed) under
   the logical connection name.
4. Restart `airflow standalone` / container — `dags/data_pipeline_dags.py`
   auto-discovers the YAML, generates a `DAG` wrapping a `PipelineTasksGroup`,
   and the new DAG appears in the Airflow UI.
5. Click "Trigger DAG" to run once; verify rows landed.

## Milestones (sequential, independently shippable)

### Segment 1 — Project skeleton & tooling
- `pyproject.toml` deps: `dlt[duckdb,postgres,snowflake,databricks,filesystem]`,
  `apache-airflow`, `apache-airflow-providers-cncf-kubernetes`,
  `apache-airflow-providers-postgres`, `pydantic`, `pyyaml`, `pytest`,
  `ruff`, `mypy`. Airflow refuses unpinned dependency resolution — document
  and use the matching constraints file:
  `pip install "apache-airflow==<v>" --constraint
  "https://raw.githubusercontent.com/apache/airflow/constraints-<v>/constraints-<python>.txt"`.
  Pin Python to a version Airflow supports (verify against Airflow release notes
  at build time). Package skeleton, `.dlt/config.toml`, `.env.example`.
- `pyproject.toml` also declares the `dlt_data_pipeline.sources`
  entry-point group with the four built-in source builders (per Design
  principle #3).
- Ruff `flake8-tidy-imports` rule banning `airflow*` imports outside
  `src/dlt_data_pipeline/airflow/` (per Design principle #2).
- **Done when:** `uv sync` succeeds (with constraints honored); `ruff check` +
  `pytest` run clean; `python -c "from importlib.metadata import entry_points;
  print(entry_points(group='dlt_data_pipeline.sources'))"` lists the four
  builders.

### Segment 2 — Config layer
- `config/models.py` (pydantic v2, discriminated unions on `type`),
  `config/loader.py` (glob + validate `pipelines/*.yml`, fail fast with
  aggregated errors). Example YAMLs, `pipelines/_schema.md`, unit tests.
- **Done when:** valid YAMLs parse to typed objects; invalid raise clear errors;
  `pytest tests/unit` green.

### Segment 3 — Pipeline factory + REST source + duckdb dest (FIRST RUNNABLE)
- `sources/_protocol.py` (Builder Protocol), `sources/registry.py`
  (entry-point discovery + cached lookup + clear error on unknown type),
  `sources/rest_api/` (builder + entry-point registration),
  `destinations/factory.py` (duckdb + postgres), `pipeline_factory.py`. CLI
  to run one pipeline by name. `pipeline_factory.py` does **not** import
  `airflow` (Design principle #2).
- **Done when:** `python -m dlt_data_pipeline.pipeline_factory run
  example_rest_to_duckdb` syncs a public API into local duckdb; integration
  test asserts row counts + schema; a separate test imports
  `pipeline_factory` and runs a pipeline end-to-end **without** `airflow`
  available (proves boundary); a unit test asserts the registry discovers
  `rest_api` purely via entry points (no hardcoded reference).

### Segment 4 — Airflow integration
- `airflow/dag_factory.py`: build one `DAG` per YAML — `schedule=<yaml.schedule.cron>`,
  `catchup=False`, `max_active_runs=1`, wrap the dlt pipeline returned by
  `pipeline_factory` in `dlt.helpers.airflow_helper.PipelineTasksGroup` so each
  dlt resource becomes its own Airflow task.
- `dags/data_pipeline_dags.py`: walks discovered YAMLs via the config loader,
  calls `dag_factory.build(yaml)`, and assigns each generated `DAG` into
  module-level `globals()` so Airflow's DagBag scans them.
- `airflow_home/airflow.cfg`: `LocalExecutor` locally, structured so the
  `[core] executor` line can be swapped to `KubernetesExecutor` in Segment 10
  without touching DAG code (Design principle #4). YAML supports per-pipeline
  `resources:` block (cpu/memory) → consumed by
  `executor_config={"pod_override": V1Pod(...)}` on each task generated inside
  the `TaskGroup`. Local executor ignores it; prod honors it.
- **Done when:** `airflow standalone` loads, examples appear in the UI as
  DAGs, manual trigger succeeds, schedule visible; `resources` materialize as
  `executor_config` on tasks even when the local executor ignores them.

### Segment 5 — SQL database source + full refresh & incremental
- `sources/sql_database.py` (Postgres + MySQL). Wire `sync.mode` →
  `write_disposition` (replace/merge) + dlt incremental hints. Postgres dest.
- **Done when:** Postgres→Postgres runs full refresh then incremental; second
  run picks up only changed rows; cursor persists between runs.

### Segment 6 — Docker / docker-compose
- `Dockerfile` (single image — webserver, scheduler, triggerer, and worker
  pods all share it; this is also the **prod run image** consumed by
  `KubernetesExecutor` in Segment 10).
- `docker-compose.yml` services: `airflow-init` (runs `airflow db migrate` and
  creates the admin user), `airflow-webserver`, `airflow-scheduler`,
  `airflow-triggerer`, `postgres-airflow` (metadata DB), `postgres-source`,
  `postgres-destination`. Persistent volumes for `dags/`, `airflow_home/logs/`,
  and each Postgres data dir. `LocalExecutor` only.
- `seed_local.sh`.
- **Done when:** `docker compose up` brings up the full stack; UI reachable;
  Postgres→Postgres runs in-container; state survives `docker compose restart`;
  same image runs a pipeline standalone via `docker run … python -m
  dlt_data_pipeline.pipeline_factory run <name>` (no Airflow process)
  — proves the image works for per-task pod invocation under
  `KubernetesExecutor`.

### Segment 6.5 — Developer ergonomics (Claude-friendly introspection)
- `CLAUDE.md` at repo root — minimal agent brief: where new pipeline YAMLs
  live, the logical-connection-name → env-var convention
  (`SOURCES__<TYPE>__<CONN>__CREDENTIALS`,
  `DESTINATION__<CONN>__CREDENTIALS`), how to validate without running, how
  to introspect available source types, the orchestrator-agnostic boundary
  (Design principle #2), and the "do not edit cfg, env-override" rule from
  Segment 6. Pure context, no automation. Read automatically by Claude Code
  on session start.
- JSON Schema export from pydantic models: new CLI subcommand
  `python -m dlt_data_pipeline config schema` dumps
  `PipelineConfig.model_json_schema()`; committed at `pipelines/_schema.json`.
  Each `pipelines/*.yml` gets a top-line
  `# yaml-language-server: $schema=./_schema.json` so LSP-aware editors
  (and Claude) get inline field validation + autocomplete. Pre-commit hook
  regenerates `_schema.json` and fails if it drifts from the models.
- Introspection CLI subcommands under existing `python -m
  dlt_data_pipeline` entry point:
  - `sources list` — dump the `dlt_data_pipeline.sources` entry-point
    group (no more `python -c "from importlib.metadata import …"` one-liners).
  - `sources describe <type>` — print that source's pydantic config schema
    + the env-var key its credentials resolve from.
  - `pipelines validate [name]` — load + pydantic-validate one or all
    YAMLs; exit non-zero on failure, no pipeline run.
  - `pipelines doctor` — for each pipeline, probe the env vars its source +
    destination would resolve and report any missing. Sub-second feedback
    vs. a full pipeline run that fails on the first missing credential.
- **Done when:** `CLAUDE.md` present at repo root; `pipelines/_schema.json`
  committed and regenerable via `python -m dlt_data_pipeline config
  schema`; pre-commit fails on drift; every `pipelines/*.yml` carries the
  `$schema` directive; `sources list / describe`, `pipelines validate /
  doctor` subcommands work; unit tests cover each subcommand's exit code on
  the happy + error paths.

### Segment 6.6 — Agent integration surface (borrowed from dlt LLM-native workflow)
- **MCP server** (`src/dlt_data_pipeline/mcp_server.py`, FastMCP) exposing
  the four Segment 6.5 introspection subcommands (`sources_list`,
  `sources_describe`, `pipelines_validate`, `pipelines_doctor`) as native MCP
  tools so Claude Code / Codex call them without subprocess overhead. Registered
  via committed `.mcp.json`. Prereq refactor: split each `cli/*_cmds.py`
  command into a pure data helper (returns structured dict) + a print-and-exit
  CLI wrapper; MCP tools call the helpers.
- **`AGENTS.md`** becomes the canonical agent brief (former `CLAUDE.md` body,
  lead paragraph genericized); `CLAUDE.md` becomes a symlink to it so Codex
  and Claude Code read identical content from one source of truth.
- **Dry-run extract** — `--limit N` (calls `DltSource.add_limit(N)`) and
  `--no-load` (calls `pipeline.extract()` + `pipeline.normalize()`, skips
  `pipeline.load()`) flags on the existing `run` subcommand. New
  `pipeline_factory.run_dry()` reuses existing `build()`. Validates source
  connectivity and schema inference without writing to destination.
- **`/add-pipeline` slash skill** at `.claude/skills/add-pipeline/SKILL.md` —
  composes scaffolder → `pipelines validate` → `pipelines doctor` into one
  agent workflow primitive. Pulls a minimal `scripts/new_pipeline.py` forward
  from Segment 9 (emits a YAML skeleton from source + destination metadata
  with TODO markers; ~80 LOC). Segment 9 still owns the polished version
  (CI workflow, alerting, scaffolder UX).
- **Done when:** `uv run python -m dlt_data_pipeline.mcp_server` lists
  four tools; existing CLI tests stay green after the helper extraction;
  `run example_rest_to_duckdb --limit 1 --no-load` exits 0 without writing a
  duckdb dataset; `CLAUDE.md` resolves to `AGENTS.md` via symlink (mode
  `120000` under git); `scripts/new_pipeline.py foo --source rest_api --dest
  duckdb` emits a parseable-but-incomplete YAML; the `add-pipeline` skill
  exists with frontmatter describing trigger phrases.

### Segment 7 — CDC source (Postgres logical replication)
- `sources/pg_cdc.py` (wraps dlt `pg_replication`), `sync.mode: cdc` handling
  (slot + publication, snapshot + streaming), short-interval schedule on the
  generated DAG, optional custom sensor under `airflow/sensors.py` to monitor
  replication-slot lag. Document `wal_level=logical` requirement.
- **Done when:** source PG configured for logical replication; INSERT/UPDATE/
  DELETE propagate to destination; replication slot persists across restarts.

### Segment 8 — Filesystem source + Snowflake destination (Databricks deferred)
**Scope clarification:** Snowflake is the active write target. Databricks
is **not** used as a direct destination by this pipeline — current
architecture loads Snowflake, which fans out to Databricks via Databricks'
External Data connection. Segment 8 implements Snowflake only; Databricks
stays stubbed with a documented "not in active use" message and is
re-evaluated only if a direct dlt → Databricks load becomes a requirement.

- `sources/filesystem/__init__.py` was a stub; now wraps
  `dlt.sources.filesystem` for local + S3/GCS/Azure reads of
  CSV/Parquet/JSONL. Metadata in
  `sources/filesystem/_metadata.py` lists allowed config keys
  (`bucket_url` required; `file_glob`, `format`, `table_name`,
  `reader_kwargs`, `files_per_page`, `extract_content`). The entry-point
  registration was already in place in `pyproject.toml` (lines 39, 45).
- `destinations/factory.py`: Snowflake branch implemented via dlt's
  native `snowflake` destination, gated on the optional
  `dlt[snowflake]` extra (clear `RuntimeError` when missing). Auth
  surface documented in `destinations/_metadata.py`: password URI for
  env-var setups; key-pair (base64 PEM via
  `[destination.snowflake.<connection>.credentials]`) for prod.
- `destinations/factory.py`: Databricks branch raises `NotImplementedError`
  with a sharper deferral message describing the Snowflake → Databricks
  External Data flow. Metadata `notes` mirror the deferral. Enum entry
  preserved so YAML schema validation still accepts the type.
- New `destinations/registry.py`: parity with `sources/registry.py`
  (`list_types`, `describe`). Unblocks the Segment 9 scaffolder upgrade.
- Resolved the orphan example: `pipelines/example_pg_cdc_to_snowflake.yml`
  now passes `pipelines doctor` via the real Snowflake builder; its
  CDC slot + publication renamed to `dlt_orders_snowflake_slot` /
  `dlt_orders_snowflake_pub` to avoid collision with
  `example_pg_cdc_to_pg.yml`.
- New committed example: `pipelines/example_filesystem_to_duckdb.yml`
  exercised by the integration test.
- Tests:
  - `tests/integration/test_filesystem_source.py` — hermetic
    filesystem→duckdb parametrized across CSV / Parquet / JSONL fixtures
    under `tests/fixtures/files/`. No network. Plus negative tests
    (unknown format, unknown config key, remote bucket missing creds).
  - `tests/integration/test_destinations_snowflake.py` — gated by
    `pytest.mark.skipif(not os.environ.get("SNOWFLAKE_TEST_ACCOUNT"))`.
    Runs against a real Snowflake account when creds present; skipped
    cleanly otherwise. **No Databricks integration test** — deferred
    with the builder.
  - `tests/unit/test_destinations_factory.py` — Snowflake assertion
    inverted from `NotImplementedError` to positive build; Databricks
    assertion updated to match the new "not in active use" message.
  - `tests/unit/test_destinations_factory_config.py` — new. Covers the
    `dlt[snowflake]` extra-missing path (`RuntimeError`), the
    destinations registry surface (`list_types`, `describe`,
    `resolve_env_var`), and the Databricks deferral metadata.
- **Done when:**
  - `pipelines doctor` reports a non-`MISSING` slot for every committed
    example YAML (Snowflake CDC example resolves through real builder).
  - `pytest tests/integration/test_filesystem_source.py` green with no
    external services.
  - Snowflake key-pair-auth path documented in
    `destinations/_metadata.py` and exercised by registry unit test.
  - Snowflake integration test runs when creds present, skips cleanly
    when absent; CI stays green either way.
  - Any pipeline YAML with `destination.type: databricks` fails at
    `build()` with the new deferral message.

### Segment 9 — Observability, alerting, scaffolder polish, CI
- **Scaffolder upgrade** (not greenfield): `scripts/new_pipeline.py` already
  exists in a minimal form (~97 LOC, hardcoded template) from Segment 6.6.
  Upgrade to the polished version:
  - Source-metadata-driven prompts using existing
    `sources.registry.describe()`.
  - Destination-metadata-driven prompts using the analogous helper added
    in `destinations/factory.py` during Segment 8.
  - Auto-write the
    `# yaml-language-server: $schema=./_schema.json` directive.
  - Post-write call to `pipelines validate <name>`; exit non-zero rather
    than leaving an invalid YAML on disk.
  - Reuse the data-helper functions already extracted under `cli/`
    during Segment 6.6 (no subprocess hops).
- **`airflow/callbacks.py`** (new) + **`observability/alerts.py`** (new
  directory and module). `on_failure_callback` from `dag_factory` calls
  the alerts module which posts to Slack webhook + SMTP. The slot-lag
  sensor already exists at `airflow/sensors.py` (Segment 7); this segment
  only adds the **routing** — no new sensor code. Add:
  - Severity routing via YAML `alerts.severity` (P1/P2 split → different
    Slack channels / email distribution lists).
  - Dedup window (suppress duplicate failures within N minutes,
    configurable per pipeline).
  - Schema-change alerts (from dlt's evolution events), severity `info`.
- **Dataset / SLA wiring**: in `dag_factory.py`, wire
  `Dataset`/`DatasetEvent` so downstream pipelines can subscribe to
  upstream completion (foundation for the freshness story).
  Airflow 2.x `SLA` is the v1 path with `sla_miss_callback`; document the
  Airflow 3.x `deadline` API switch as a follow-up, not a blocker.
- **CI** — `.github/workflows/ci.yml` with an explicit job matrix:
  - `lint` — ruff + mypy + pre-commit (`regen-pipeline-schema`).
  - `unit` — `pytest tests/unit`.
  - `integration-duckdb` — hermetic, no creds.
  - `integration-postgres` — composes up `postgres-source` +
    `postgres-destination` services.
  - `integration-snowflake` — skipped when secrets absent. (No
    Databricks job — destination deferred in Segment 8.)

  Airflow install via the matching constraints file per Segment 1.
  Cache `~/.cache/uv` + `~/.cache/pip` keyed on `pyproject.toml`.
- **Done when:**
  - Simulated failure on `example_pg_to_pg_incremental` posts a Slack
    message via a mocked webhook URL.
  - `scripts/new_pipeline.py foo --source sql_database --dest postgres`
    writes a YAML that passes `pipelines validate` on the first try.
  - CI matrix green on a PR that touches only docs.

### Segment 10 — Prod deployment + executor
**Already shipped (do not redo):** `ResourcesConfig` + `pod_override`
wiring landed in Segment 4 (`config/models.py:112`,
`airflow/dag_factory.py:40`). This segment only consumes that contract.

- Switch executor to `KubernetesExecutor` via
  `AIRFLOW__CORE__EXECUTOR=KubernetesExecutor` env override (per the
  Segment 6 "do not edit `airflow.cfg`, env-override" rule). Ship a base
  `pod_template_file` at `airflow_home/pod_templates/base.yaml` referenced
  by `AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE`. The directory
  exists today with only a `.gitkeep`.
- **`deploy/k8s/` scaffolding** — raw manifests, kustomize-vs-helm decision
  deferred. Layout:
  - `deploy/k8s/base/webserver-deployment.yaml`
  - `deploy/k8s/base/scheduler-deployment.yaml`
  - `deploy/k8s/base/triggerer-deployment.yaml`
  - `deploy/k8s/base/postgres-metadata-statefulset.yaml`
  - `deploy/k8s/base/airflow-secret.yaml`
  - `deploy/k8s/base/airflow-configmap.yaml`
  - `deploy/k8s/base/pod-template-base.yaml` (mirrors the
    `airflow_home/pod_templates/base.yaml` referenced above)
  - `deploy/k8s/overlays/{dev,staging,prod}/` for per-env values.
- **Secrets backend (v1):** k8s `Secret` env-mounted on the pod template
  with env-var keys following the existing
  `SOURCES__<TYPE>__<CONN>__CREDENTIALS` /
  `DESTINATION__<TYPE>__<CONN>__CREDENTIALS` convention (see
  `AGENTS.md`). Logical connection names in YAML resolve identically;
  only the resolver layer changes. The Airflow Secrets Backend
  integration is the v2 path — documented in the secrets component
  README, not blocking v1.
- **Remote logging:** pin v1 to S3 via
  `AIRFLOW__LOGGING__REMOTE_LOGGING=True` +
  `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://…`. Add
  `apache-airflow-providers-amazon` to `pyproject.toml`. GCS / Azure as
  documentation-only. Remove any code paths that assume local-disk
  persistence beyond a run.
- **CI image build/push** — extend the Segment 9 CI workflow:
  - `build-and-push` job on tag push: builds `docker/Dockerfile`, tags
    `:{git_sha}` + `:{git_tag}`, pushes to the registry.
  - `deploy` job patches the pod template + rolls the webserver,
    scheduler, and triggerer deployments.
- **Pod-spec smoke test** (replaces the deleted "resources flow through"
  bullet): `tests/integration/test_pod_override.py` asserts the V1Pod
  shape produced by `dag_factory._pod_override()` for representative
  `ResourcesConfig` inputs. No cluster required.
- **Done when:**
  - A scheduled pipeline runs as a fresh pod per task with isolated
    CPU/RAM limits inherited from the YAML `resources:` block.
  - Image rollout updates the run image without aborting in-flight runs.
  - Secrets resolve from the prod backend.
  - Logs are visible in the cluster aggregator.
  - Killing the webserver does not abort in-flight runs (scheduler owns
    state).
  - `kubectl get pods -n airflow` shows running webserver / scheduler /
    triggerer after deploy.
  - Webserver-deployment rolling restart during an in-flight run does
    NOT abort the run.

### Segment 10.5 — Destination env-var convention fix
Bug: destinations metadata advertises
`DESTINATION__<TYPE>__<CONNECTION>__CREDENTIALS`
(see `src/dlt_data_pipeline/destinations/_metadata.py:37,45` and
`AGENTS.md:44-45`), but `destinations/factory.py` constructs each
destination with `destination_name=connection`, so dlt's actual lookup
path is `DESTINATION__<CONNECTION>__CREDENTIALS__*` — no type segment.
`.env.example:35` already uses the working form
(`DESTINATION__PG_WAREHOUSE__CREDENTIALS=...`).
`tests/unit/test_destinations_factory.py:44-65` already documents +
locks the working behavior, so the metadata is the bug. Ship as its
own small segment ahead of the rename so the corrected strings land
once and get swept by the rename in Segment 11.

- `destinations/_metadata.py` — drop `__<TYPE>__` segment from postgres
  + snowflake `env_var_template`.
- `AGENTS.md` — update env-var template doc.
- Tests with hard-coded type-prefixed env vars:
  `tests/unit/test_destinations_factory_config.py:43,64`,
  `tests/unit/test_new_pipeline_scaffolder.py:158`,
  `tests/unit/test_cli_pipelines.py:14`,
  `tests/integration/test_destinations_snowflake.py:86`,
  `tests/integration/test_alerting_e2e.py:38`.
- `.github/workflows/ci.yml:76` — fix the `integration-postgres` env var.
- Re-run `pipelines doctor` against every example YAML; verify each
  resolves under the corrected template.
- **Done when:**
  `RUN_LIVE_PG=1 uv run pytest tests/integration -m "postgres and not cdc"`
  against `docker compose up postgres-source postgres-destination`
  passes locally + in CI; `pipelines doctor` reports the same env-var
  key the user actually sets.

### Segment 11 — Package rename
**Package rename `data_pipeline_template` → `dlt_data_pipeline`** (deferred
from Segment 10 — the GHCR image naming question surfaced the mismatch
between the python package name and the repo / GHCR image name). The
rename aligns all three: distribution name `dlt-data-pipeline` (PyPI
convention, hyphenated), import path `dlt_data_pipeline` (Python
identifier rule, matches the repo directory), GHCR image
`ghcr.io/<owner>/dlt-data-pipeline` (shipped in Segment 10). Mechanical
scope at the time of this entry: **64 files / 189 occurrences** (grep
`data_pipeline_template`), plus a directory move. Land as a single
dedicated commit so subsequent segments author against the final
names. Documentation rewrite is explicitly out of scope — see Segment 15.

Files to touch:
- **Directory move:** `src/data_pipeline_template/` →
  `src/dlt_data_pipeline/`.
- **`pyproject.toml`:** `name`, `[tool.hatch.build.targets.wheel]
  packages`, `[tool.mypy] packages`, the two entry-point GROUP names
  (`data_pipeline_template.sources` and
  `data_pipeline_template.sources.metadata`), and each entry-point
  target path. Note: renaming the group breaks any out-of-tree source
  plugins; none exist yet — call it out in `AGENTS.md` for future
  third-party authors.
- **Imports:** every `from data_pipeline_template …` and `import
  data_pipeline_template …` across `src/`, `tests/`, `dags/`,
  `scripts/`.
- **`dags/data_pipeline_dags.py`:** content references only — keep the
  filename (it's what Airflow's DagBag globs).
- **Entry-point invocations in docs / hooks / configs:** `python -m
  data_pipeline_template …` in `AGENTS.md`, `CLAUDE.md` (symlink —
  follows automatically), `README.md` (stub today; full rewrite in
  Segment 15), this plan, `.pre-commit-config.yaml`, `.mcp.json`,
  `.claude/skills/add-pipeline/SKILL.md`, every `pipelines/*.yml`'s
  `# yaml-language-server` directive (the schema path is relative so
  unaffected, but any accompanying generator comments may reference
  the module).
- **`pipelines/_schema.json`:** regen via the existing
  `regen-pipeline-schema` pre-commit hook — picks up the new module path
  automatically.
- **`uv.lock`:** regen.
- **CI workflow:** any `uv run python -m data_pipeline_template …`
  invocations in `.github/workflows/ci.yml`.

- **Critical Files section update (this plan):** after the rename
  lands, sweep the `## Critical Files` block at the bottom of this plan
  to swap every `src/data_pipeline_template/…` path to
  `src/dlt_data_pipeline/…`. Also update the design-principle examples
  in `## Context` (principle #2 cites
  `src/data_pipeline_template/airflow/`; principle #3 cites the
  entry-point group name).
- **Done when:**
  - `grep -r data_pipeline_template src/ tests/ dags/ scripts/
    pyproject.toml .github/ AGENTS.md README.md .mcp.json
    .pre-commit-config.yaml` returns zero hits (allowed only in
    historical references inside `data_pipeline_plan.md` itself, and in
    `uv.lock` cached upstream metadata).
  - `pip install -e .` resolves under the new distribution name;
    `python -c "import dlt_data_pipeline; print(dlt_data_pipeline.__file__)"`
    succeeds.
  - The `data_pipeline_template.sources` entry-point group is renamed to
    `dlt_data_pipeline.sources`; `python -c "from importlib.metadata
    import entry_points; print(entry_points(group='dlt_data_pipeline.sources'))"`
    lists the four builders.
  - `python -m dlt_data_pipeline pipelines validate` exits 0 against
    every committed example.
  - Pre-commit `regen-pipeline-schema` runs clean against the renamed
    package.
  - CI matrix green on the rename commit alone (no behavior change, only
    identifier churn).

### Segment 12 — Operational hardening
Promoted from former "Tricky Parts (gap)" items — these have grown beyond
"decide during build" and need explicit scope.

- **Backfill / initial-load strategy:** chunked / resumable loads via dlt
  `initial_value` partitioned ranges. Surface as YAML
  `sync.backfill.chunk_size` + `sync.backfill.partition_field`. Add a new
  `pipeline_factory.run_backfill(name, start, end)` callable plus a
  matching `python -m dlt_data_pipeline run-backfill <name> --start
  <ts> --end <ts>` CLI subcommand the user calls once before enabling the
  schedule.
- **Pipeline lifecycle / orphan cleanup:** a `pipelines delete <name>` CLI
  subcommand that
  (a) drops the destination dataset (with `--keep-data` opt-out),
  (b) drops the CDC replication slot + publication when
  `source.type == pg_cdc`,
  (c) removes the YAML and prints the DAG-UI cleanup instruction.
  Tombstone alternative: a `pipelines/_tombstones/` directory the loader
  treats as "do not generate a DAG but do not orphan state" — useful
  when the destination data must be retained.
- **Incremental edge cases:** explicit `sync.tolerance_seconds` /
  `sync.lookback` knobs for late-arriving rows; documented per-mode
  guarantees table in `sources/README.md`. Test fixtures covering
  timezone-skewed `updated_at`, cursor gaps, late inserts.
- **Secret-leakage hardening:** a log-scrubbing filter wired into the
  Airflow logger (regex-strip `password=`, `token=`, `key=` patterns from
  rendered task logs). Test: a deliberate `raise RuntimeError(creds_url)`
  in a fixture pipeline produces a log line where the credential is
  masked.
- **Data-quality checks (v1 scope):** source-vs-destination row-count
  reconciliation via a generated `SQLCheckOperator` task per pipeline,
  gated by YAML `quality.row_count_check: true`. Null / PK / volume-anomaly
  checks deferred to v2.
- **Done when:** each bullet has at least one integration test; CLI
  subcommands exit-code-tested; a row-count-reconciliation task surfaces
  in an example DAG with a passing check.

### Segment 13 — Multi-environment config + promotion
Promoted from former "Open Considerations".

- **Env-specific overrides:** `pipelines/_env/<env>.yml` overlay files
  merged atop each `pipelines/*.yml` at load time. Overlay scope (v1):
  `destination.connection`, `source.connection`, `schedule.enabled`,
  `resources`. Loader change in `config/loader.py` accepting `--env
  <name>`, defaulting to `$DLT_ENV` env var, defaulting to `dev`.
- **Promotion flow:** `pipelines promote <name> --from staging --to prod`
  CLI subcommand that diffs the merged config across envs and prints
  what would change before applying.
- **Secrets-resolver layering:** documented precedence — env var > Airflow
  Secrets Backend > `.dlt/secrets.toml`. Wires into Segment 10's k8s
  Secret env-mount.
- **Done when:** same pipeline YAML loads under `dev` (duckdb dest) and
  `prod` (snowflake dest) with no edit to the base YAML; `pipelines
  doctor --env prod` reports the prod env-var keys.

### Segment 14 — Health monitoring ("who watches the watcher")
Promoted from former "Open Considerations → Alert depth".

- **Scheduler/triggerer heartbeat:** a standalone `dags/heartbeat_check.py`
  DAG running every 5min that emits a metric and alerts if a heartbeat
  is missed twice in a row. Outside the YAML-generation flow.
- **Schema-change alerts:** hook into dlt's schema-evolution event
  surface, post to the alert channel with severity `info` (not `error`).
  Reuses Segment 9 alert routing.
- **SLA misses → routing:** Airflow 2.x `sla_miss_callback` reusing
  `observability/alerts.py`; document the Airflow 3.x `deadline`
  migration path.
- **Done when:** killing `airflow-scheduler` container fires an alert
  within 10min; a schema add on a `schema_contract: evolve` pipeline
  fires an info-level alert.

### Segment 15 — Documentation (final pass)
Deliberately last so READMEs capture the final v1 surface in one pass
instead of being revised after every later segment ships (`pipelines
delete`, `run-backfill`, `pipelines promote`, log scrubber, env
overlays, heartbeat DAG all land in 12-14 and need README coverage).

**README ↔ AGENTS split:** `README.md` is the **human onboarding doc**;
`AGENTS.md` is the **agent brief** (and `CLAUDE.md` symlinks to it from
Segment 6.6). Don't duplicate — README links to AGENTS.md for the env-var
convention, dry-run flow, CDC ops, MCP server, etc. Cross-link the other
direction too. `README.md` is currently a 21-byte stub.

- **Rewrite root `README.md`:** project purpose; architecture diagram
  (ASCII or mermaid); quickstart (`docker compose up` + `seed_local.sh`);
  the 10-min "add a pipeline" workflow (lift from this plan, cross-link
  to the scaffolder); link table to per-component READMEs; cross-link to
  `AGENTS.md` for the agent / introspection workflow.
- **Per-component `README.md`** in each major dir — `config/`, `sources/`,
  `destinations/`, `airflow/`, `observability/`, `docker/`, `pipelines/`,
  `tests/`, `cli/`, plus a top-level docstring for `mcp_server.py`. Each
  covers: what the component does, how to use / extend it (e.g. adding a
  new source type to `registry.py`), known issues, troubleshooting. Must
  reflect surface added in Segments 12-14:
  - `cli/README.md` covers `run-backfill`, `pipelines delete`,
    `pipelines promote` (Segments 12-13).
  - `observability/README.md` covers the log-scrubbing filter
    (Segment 12) and SLA-miss routing (Segment 14).
  - `airflow/README.md` covers the heartbeat DAG (Segment 14).
  - `config/README.md` covers env-overlay loader behavior (Segment 13).
- **Fold gaps** — explicit Tricky-Parts → component-README mapping:
  - CDC setup, replication-slot lifecycle → `sources/README.md`.
  - Snowflake creds (+ Databricks deferral rationale) → `destinations/README.md`.
  - Schema evolution → `pipelines/README.md` + `config/README.md`.
  - State persistence, `max_active_runs`, pool → `airflow/README.md`.
  - Secret leakage, log scrubbing → `observability/README.md`.
  - Backfill, pipeline lifecycle, incremental edge cases →
    `cli/README.md` + `sources/README.md` (now shipped in Segment 12,
    no longer forward-references).
  - PII → still cross-linked as post-v1.
- **Done when:**
  - `README.md` is the single entry point a new dev needs.
  - `AGENTS.md` and `README.md` cross-link explicitly.
  - Each component README ends with a "Known issues / troubleshooting"
    subsection that quotes the relevant Tricky Parts item verbatim (so a
    search hits both).
  - A new dev adds a pipeline in ~10 min from root README alone.
  - Every CLI subcommand shipped through Segment 14 has a README entry
    (no orphan surface).

## Verification / Testing

- **Unit** (`tests/unit/`): config model validation, loader discovery, registry
  lookups. Pure Python, fast, every commit.
- **Integration w/ duckdb** (`tests/integration/`): hermetic end-to-end runs
  (REST→duckdb, filesystem→duckdb). No external services, runs in CI without
  secrets. Assert row counts, schema, incremental behavior across two runs.
- **Integration w/ dockerized Postgres**: `conftest.py` fixture spins up / connects
  to compose `postgres-source` + `postgres-destination`. Local Postgres acts as
  **both source and destination** to exercise sql_database, incremental cursors,
  merge disposition, and CDC. Seed via `tests/fixtures/seed_source.sql`.
- **E2E local flow:** `docker compose up` → `seed_local.sh` → trigger each
  example DAG in UI → for incremental, mutate source + re-run, assert only
  deltas → for CDC, snapshot then INSERT/UPDATE/DELETE, confirm propagation →
  restart containers, re-run, confirm state persisted.
- **Snowflake:** gate behind env-var presence (`pytest.mark.skipif`);
  fall back to config-validation-only unit tests when creds absent so CI
  stays green. Databricks destination is deferred (Segment 8 scope note);
  no integration test until a direct dlt → Databricks pipeline materializes.
- **Airflow-layer tests:** `dag_factory` must be tested directly — assert a
  YAML produces the expected `DAG` (id, `schedule`, `max_active_runs`) and a
  `PipelineTasksGroup` containing the expected per-resource tasks. Not just
  exercised via E2E. Land alongside the Segment 9 CI matrix.
- **REST source mocking:** hermetic REST tests need a mocking strategy
  (`responses` / VCR cassettes) so unit/integration runs don't depend on a live
  public API. Land alongside the Segment 9 `integration-duckdb` job.
- **Data-quality checks:** source-vs-destination row-count reconciliation
  (`SQLCheckOperator`) is scoped under **Segment 12**. Null/PK checks and
  volume-anomaly detection are deferred to v2.

## Tricky Parts

- **CDC setup:** source PG needs `wal_level=logical`, `max_replication_slots`,
  `max_wal_senders` (compose `command:` flags; managed PG needs param-group
  change + restart). Replication slot retains WAL until consumed — a paused
  pipeline fills source disk; needs monitoring + cleanup path. Test DELETE
  propagation and snapshot→streaming handoff explicitly.
- **Snowflake creds:** key-pair auth is multi-line — base64-encode the PEM
  and set `credentials.private_key` (plus `credentials.private_key_passphrase`
  if encrypted) under `[destination.<connection>.credentials]` in
  `.dlt/secrets.toml`. Account / user / database / warehouse / role go in
  the same TOML section. Password URI for env-var setups —
  `DESTINATION__<CONNECTION>__CREDENTIALS=snowflake://user:pw@account/db?warehouse=wh&role=role`.
  Keep all out of YAML. Segment 10.5 dropped the type segment from the
  destination resolver path, so the legacy
  `[destination.snowflake.<connection>.credentials]` key no longer
  resolves — use `[destination.<connection>.credentials]`. (Databricks
  deferred — see Segment 8 scope note.)
- **Schema evolution:** expose dlt `schema_contract` per pipeline. `evolve`
  default is convenient but silently adds columns; recommend `freeze` for
  regulated destinations. Surface schema-change events in DAG-run metadata
  via `XCom` or task logs.
- **Incremental edge cases:** cursor-based incremental loses rows when the
  source writes `updated_at` slightly before commit or under timezone skew
  between source and Airflow worker. `sync.tolerance_seconds` (dlt
  `Incremental.lag`) and `sync.lookback` (ISO-8601 duration; composes with
  tolerance) re-read a trailing window each run to catch late arrivals.
  Defaults are zero — explicit opt-in per pipeline. Cursor gaps after a
  failed-and-retried run are absorbed by the same lookback window; without
  it, deletes-after-cursor and out-of-order inserts go missing. Segment 12
  owns the implementation contract; see `config/models.py:123-124` and the
  AGENTS.md "Backfill an incremental pipeline" section.
- **State persistence across containers:** dlt stores incremental cursors +
  schema in the *destination* dataset (`_dlt_*` tables) — durable, survives
  restarts. Put local `~/.dlt` working dir on a named volume too. Point
  Airflow's metadata DB at the persistent `postgres-airflow` service (not the
  default SQLite). Prevent overlapping runs of the same pipeline (cursor
  corruption) via `max_active_runs=1` on the generated `DAG`, plus a
  per-pipeline `pool` if a single pipeline can fan out into concurrent task
  instances.
- **Monitoring:** Airflow gives run-level observability via task instance
  state + DAG run state. Add `on_failure_callback` and freshness via
  `Dataset`/`DatasetEvent` (catch silently-not-running schedules). For CDC,
  monitor replication slot lag on the source DB — outside Airflow's view,
  expose via a periodic check task that emits a metric and fails the DAG when
  over threshold. Surface dlt load metrics into XCom for the UI.
- **dlt Airflow helper caveats:** `PipelineTasksGroup` runs each dlt resource
  in a separate Airflow task; ensure `pipeline_factory` returns a
  `dlt.Pipeline` whose `source()` exposes resources discoverable by the
  helper. Document this contract in `pipeline_factory.py`.
- **Airflow install fragility:** Airflow refuses unpinned dependency
  resolution. Always install with the matching
  `constraints-<version>-<python>.txt`. Document in `pyproject.toml`
  comments and the CI workflow; pin the Python version alongside.
- **Secret leakage** — context for the log-scrubbing work scoped under
  Segment 12: dlt can echo credentials in logs/tracebacks; Airflow
  renders task templates in logs (use `Connection`/`Variable` objects so
  the UI mask filter catches them); CDC needs a dedicated source DB user
  with `REPLICATION` (least privilege), not a superuser.
- **PII / governance (out of v1 scope):** field-level masking / hashing /
  column exclusion and GDPR delete propagation — `schema_contract: freeze`
  is not enough for regulated destinations. Revisit post-v1.

## Critical Files
- `src/dlt_data_pipeline/config/models.py` — YAML schema contract;
  everything depends on it.
- `src/dlt_data_pipeline/pipeline_factory.py` — core config → dlt pipeline.
  **Must not import `airflow`** (orchestrator-agnostic boundary; see Design
  principle #2).
- `src/dlt_data_pipeline/airflow/dag_factory.py` — YAML → Airflow DAG with
  `PipelineTasksGroup`.
- `src/dlt_data_pipeline/sources/registry.py` — extension point for new
  source types.
- `dags/data_pipeline_dags.py` — DagBag entry-point; assigns generated DAGs
  into module `globals()`.
- `docker/docker-compose.yml` — local end-to-end test/dev environment.
