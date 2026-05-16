# Plan: `data_pipeline_template` — a dlt + Airflow Fivetran replacement

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
   **Airflow-agnostic** — no `import airflow` outside `src/data_pipeline_template/airflow/`.
   The factory takes a `PipelineConfig` and returns a runnable `dlt.pipeline`;
   the Airflow layer wraps it. This keeps dlt core usable standalone (CLI runs,
   tests, future orchestrator swap) and lets us swap Airflow without rewriting
   the pipeline core. Enforce via:
   - Layout: Airflow code isolated to `src/data_pipeline_template/airflow/`.
   - Lint rule: a `ruff` `tidy-imports`/`flake8-tidy-imports` ban on `airflow*`
     imports anywhere outside that subpackage (or a small custom test that
     greps the source tree and fails CI on violations).
   - Test: a unit test runs a pipeline end-to-end via `pipeline_factory.run(name)`
     **without** importing the Airflow package at all, proving the boundary.
3. **Source types are plugins (Python entry points), not hardcoded.**
   `sources/registry.py` does **not** maintain a hardcoded `{name -> builder}`
   dict. Instead it discovers source types via the
   `data_pipeline_template.sources` entry-point group at startup. Each source
   type is its own subpackage under `src/data_pipeline_template/sources/<name>/`
   (or, later, a separate installable package) that exposes a `builder(config)
   -> dlt.Source` and registers itself via `pyproject.toml`:
   ```toml
   [project.entry-points."data_pipeline_template.sources"]
   rest_api = "data_pipeline_template.sources.rest_api:builder"
   sql_database = "data_pipeline_template.sources.sql_database:builder"
   filesystem = "data_pipeline_template.sources.filesystem:builder"
   pg_cdc = "data_pipeline_template.sources.pg_cdc:builder"
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

## Repo Structure

```
data_pipeline_template/
├── pyproject.toml                  # deps, ruff/mypy/pytest config (use uv)
├── .env.example                    # documents required env vars (non-secret)
├── docker/
│   ├── Dockerfile                  # single image: code + dlt + airflow
│   └── docker-compose.yml          # webserver, scheduler, triggerer,
│                                   # airflow-init, postgres-airflow (metadata),
│                                   # postgres-source, postgres-destination
├── airflow_home/
│   ├── airflow.cfg                 # instance config: executor, metadata DB
│   └── pod_templates/              # base pod template(s) for KubernetesExecutor
├── dags/
│   └── data_pipeline_dags.py       # imports loader + dag_factory; assigns
│                                   #   generated DAGs into module globals()
│                                   #   so Airflow's DagBag scans them
├── pipelines/                      # USER-FACING: one YAML per pipeline
│   ├── _schema.md
│   └── example_*.yml
├── src/data_pipeline_template/
│   ├── config/
│   │   ├── models.py               # pydantic v2 schema, discriminated unions on `type`
│   │   └── loader.py               # discover + parse + validate pipelines/*.yml
│   ├── sources/                    # one subpackage per source type
│   │   ├── registry.py             # discovers builders via entry-point group
│   │   │                           #   `data_pipeline_template.sources` (no hardcoded list)
│   │   ├── _protocol.py            # Builder Protocol: (config) -> dlt.Source
│   │   ├── rest_api/__init__.py    # wraps dlt.sources.rest_api; exposes `builder`
│   │   ├── sql_database/__init__.py# wraps dlt.sources.sql_database
│   │   ├── filesystem/__init__.py  # wraps dlt.sources.filesystem (files / S3)
│   │   └── pg_cdc/__init__.py      # wraps dlt pg_replication
│   ├── destinations/factory.py     # destination "type" -> configured dlt destination
│   ├── pipeline_factory.py         # core: PipelineConfig -> runnable dlt.pipeline
│   ├── airflow/
│   │   ├── dag_factory.py          # YAML config -> DAG with PipelineTasksGroup
│   │   ├── callbacks.py            # on_failure / on_success / SLA callbacks
│   │   └── sensors.py              # freshness / CDC custom sensors
│   └── observability/alerts.py     # run-failure hook -> Slack/email
├── tests/{unit,integration,fixtures}/
├── .dlt/
│   ├── config.toml                 # committed: non-secret dlt config
│   └── secrets.toml                # GIT-IGNORED: local credentials
└── scripts/
    ├── new_pipeline.py             # scaffold a new pipelines/<name>.yml
    └── seed_local.sh
```

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
- `pyproject.toml` also declares the `data_pipeline_template.sources`
  entry-point group with the four built-in source builders (per Design
  principle #3).
- Ruff `flake8-tidy-imports` rule banning `airflow*` imports outside
  `src/data_pipeline_template/airflow/` (per Design principle #2).
- **Done when:** `uv sync` succeeds (with constraints honored); `ruff check` +
  `pytest` run clean; `python -c "from importlib.metadata import entry_points;
  print(entry_points(group='data_pipeline_template.sources'))"` lists the four
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
- **Done when:** `python -m data_pipeline_template.pipeline_factory run
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
  data_pipeline_template.pipeline_factory run <name>` (no Airflow process)
  — proves the image works for per-task pod invocation under
  `KubernetesExecutor`.

### Segment 7 — CDC source (Postgres logical replication)
- `sources/pg_cdc.py` (wraps dlt `pg_replication`), `sync.mode: cdc` handling
  (slot + publication, snapshot + streaming), short-interval schedule on the
  generated DAG, optional custom sensor under `airflow/sensors.py` to monitor
  replication-slot lag. Document `wal_level=logical` requirement.
- **Done when:** source PG configured for logical replication; INSERT/UPDATE/
  DELETE propagate to destination; replication slot persists across restarts.

### Segment 8 — Filesystem/S3 source + Snowflake & Databricks destinations
- `sources/filesystem.py` (local + S3; CSV/Parquet/JSONL). Snowflake +
  Databricks branches in `destinations/factory.py`. Credential docs.
- **Done when:** filesystem→duckdb passes in tests; Snowflake/Databricks
  validated against real accounts if available, else config-validation +
  duckdb stand-in.

### Segment 9 — Observability, alerting, template polish
- `airflow/callbacks.py`: `on_failure_callback` posting to Slack/email
  (re-uses `observability/alerts.py`). Freshness via Airflow
  `Dataset`/`DatasetEvent` plus `SLA` misses (note Airflow 2.x SLA caveats;
  in Airflow 3.x switch to deadlines). `scripts/new_pipeline.py`, CI workflow
  (`.github/workflows/ci.yml`) installs Airflow via constraints file then runs
  unit + integration tests.
- **Done when:** simulated failure triggers alert; scaffolder produces valid
  YAML; CI green.

### Segment 10 — Prod deployment + executor
- Switch `airflow.cfg` (or `AIRFLOW__CORE__EXECUTOR` env) to
  `KubernetesExecutor`; ship a base `pod_template_file` under
  `airflow_home/pod_templates/` that references the deploy image.
- New files: `deploy/k8s/` with manifests for the webserver, scheduler,
  triggerer, metadata Postgres, and Airflow Secret/ConfigMap.
- CI builds + pushes the image from `docker/Dockerfile` to a registry; deploy
  step rolls the webserver + scheduler + triggerer and updates the pod
  template's image tag.
- Secrets: switch from `.env` / `.dlt/secrets.toml` to a real backend
  (k8s `Secret` env-mounted onto worker pods, optionally via Airflow's
  Secrets Backend integration) — logical connection names in YAML resolve
  identically; only the resolver layer changes.
- Logs: scheduler/webserver stdout shipped to the cluster log aggregator;
  worker-pod logs persisted via Airflow remote logging (S3 / GCS / etc.);
  remove any code paths that assume local-disk persistence beyond a run.
- Resource sizing: YAML `resources:` block (added in Segment 4) flows through
  `executor_config={"pod_override": V1Pod(...)}` into per-task CPU/RAM
  requests on the pod.
- **Done when:** a scheduled pipeline runs as a fresh pod per task with
  isolated CPU/RAM limits; image rollout updates the run image without
  aborting in-flight runs; secrets resolve from the prod backend; logs are
  visible in the cluster aggregator; killing the webserver does not abort
  in-flight runs (scheduler owns state).

### Segment 11 — Documentation
- Rewrite root `README.md`: project purpose, architecture overview, quickstart,
  the 10-min "add a pipeline" workflow, links to component READMEs.
- Per-component `README.md` in each major dir — `config/`, `sources/`,
  `destinations/`, `airflow/`, `observability/`, `docker/`, `pipelines/`,
  `tests/`. Each covers: what the component does, how to use / extend it
  (e.g. adding a new source type to `registry.py`), known issues, troubleshooting.
- Fold relevant **Tricky Parts** + **Open Considerations** notes into the
  matching component README (CDC setup → `sources/`, secrets → `docker/` +
  `config/`, etc.) so troubleshooting lives next to the code.
- **Done when:** new dev adds a pipeline in ~10 min from root README alone;
  every major dir has a README; troubleshooting sections cover the known
  failure modes from this plan.

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
- **Snowflake/Databricks:** gate behind env-var presence (`pytest.mark.skipif`);
  fall back to config-validation-only tests when creds absent so CI stays green.
- **Airflow-layer tests (gap):** `dag_factory` must be tested directly — assert a
  YAML produces the expected `DAG` (id, `schedule`, `max_active_runs`) and a
  `PipelineTasksGroup` containing the expected per-resource tasks. Not just
  exercised via E2E.
- **REST source mocking (gap):** hermetic REST tests need a mocking strategy
  (`responses` / VCR cassettes) so unit/integration runs don't depend on a live
  public API.
- **Data-quality checks (gap):** add Airflow task-level checks beyond "did it
  run" — source-vs-destination row-count reconciliation
  (`SQLCheckOperator`), null/PK checks, volume-anomaly detection (row count
  drops sharply but run "succeeds"). Decide which are v1.

## Tricky Parts

- **CDC setup:** source PG needs `wal_level=logical`, `max_replication_slots`,
  `max_wal_senders` (compose `command:` flags; managed PG needs param-group
  change + restart). Replication slot retains WAL until consumed — a paused
  pipeline fills source disk; needs monitoring + cleanup path. Test DELETE
  propagation and snapshot→streaming handoff explicitly.
- **Snowflake/Databricks creds:** Snowflake key-pair auth is multi-line —
  base64-encode or mount a key file. Databricks needs host + http_path + token
  *and* separate staging-location creds (S3/ADLS volume). Keep all out of YAML.
- **Schema evolution:** expose dlt `schema_contract` per pipeline. `evolve`
  default is convenient but silently adds columns; recommend `freeze` for
  regulated destinations. Surface schema-change events in DAG-run metadata
  via `XCom` or task logs.
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
- **Backfill / initial load (gap):** first full load of a large source table is
  the risky run — needs a chunked / resumable strategy (dlt incremental
  `initial_value`, partitioned ranges), not one giant transaction. No story yet.
- **Secret leakage (gap):** dlt can echo credentials in logs/tracebacks — add log
  scrubbing. Airflow renders task templates in logs; use Airflow
  `Connection`/`Variable` objects (not f-string credentials into operator
  args) so the UI mask filter catches them. CDC needs a dedicated source DB
  user with `REPLICATION` (least privilege), not a superuser.
- **Pipeline lifecycle (gap):** deleting a `pipelines/*.yml` currently orphans the
  destination dataset, CDC replication slot, and the Airflow DAG (which then
  disappears from the UI, possibly mid-run). Decide cleanup path (manual
  runbook vs. tombstone). Also: `models.py` schema versioning when the YAML
  contract changes shape.
- **Incremental edge cases (gap):** late-arriving rows, non-monotonic cursor
  fields, timezone skew on `updated_at`, cursor gaps. Document supported
  guarantees per sync mode.
- **PII / governance (gap):** field-level masking / hashing / column exclusion and
  GDPR delete propagation — `schema_contract: freeze` is not enough for regulated
  destinations.

## Open Considerations (not scoped — decide during build)

These are deferred notes, not v1 segments. Revisit before/while building.
(Prod deployment, executor, and secrets backend are now scoped under
Segment 10.)

- **Multi-environment config:** same pipeline YAML needs to point at different
  connections per env (dev/staging/prod). Logical connection names help, but
  env-specific overrides + promotion flow are unspecified. Decide before
  Segment 10 finalizes the secrets-resolver layer.
- **Alert depth:** failure callbacks + freshness via `Dataset` events cover
  the basics. Still missing: severity / routing, dedup (avoid alert storms on
  repeated failures), schema-change alerts, SLA definitions, and
  scheduler/triggerer heartbeat health ("who watches the watcher").

## Critical Files
- `src/data_pipeline_template/config/models.py` — YAML schema contract;
  everything depends on it.
- `src/data_pipeline_template/pipeline_factory.py` — core config → dlt pipeline.
  **Must not import `airflow`** (orchestrator-agnostic boundary; see Design
  principle #2).
- `src/data_pipeline_template/airflow/dag_factory.py` — YAML → Airflow DAG with
  `PipelineTasksGroup`.
- `src/data_pipeline_template/sources/registry.py` — extension point for new
  source types.
- `dags/data_pipeline_dags.py` — DagBag entry-point; assigns generated DAGs
  into module `globals()`.
- `docker/docker-compose.yml` — local end-to-end test/dev environment.
