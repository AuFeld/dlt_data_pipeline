# Features

A high-level tour of what `dlt_data_pipeline` does. For onboarding and the
architecture diagram see the [README](../README.md); for the agent / CLI
reference surface see [AGENTS.md](../AGENTS.md).

## What it is

`dlt_data_pipeline` is a self-hosted **Fivetran replacement** built on
[dlt](https://dlthub.com) and [Apache Airflow](https://airflow.apache.org).
It moves data from many sources (REST APIs, SQL databases, cloud file
storage, change-data-capture streams) into many destinations (Snowflake,
Postgres, DuckDB) on a schedule, with full-refresh, incremental, and CDC
sync modes. Engineers add a new pipeline by contributing **one YAML file**;
the platform handles schema inference, state, retries, alerting, and pod
isolation. There is no Python required in the common case.

## Highlights

- **YAML-first.** One file per pipeline; pydantic-validated with editor
  autocomplete via a generated JSON Schema.
- **Open plugin model for source types.** Add a new source without
  touching a central registry.
- **First-class change data capture.** Postgres logical replication via
  dlt's vendored `pg_replication` source, with idempotent slot management
  and one-shot reset.
- **Multi-environment overlays.** Promote the same pipeline from dev to
  prod by remapping only the fields that differ.
- **AI-agent native.** Ships a local MCP server so Claude Code and
  compatible assistants can validate, doctor, and promote pipelines as
  first-class tool calls.
- **Kubernetes-ready.** Same container image runs the dev compose stack
  and a production KubernetesExecutor deployment.

## Sources

Four source types ship in the box:

- **REST APIs** — paginated endpoints with auth, schema inference, and
  incremental cursor support.
- **SQL databases** — Postgres, MySQL, SQL Server, Snowflake, and other
  SQLAlchemy-reachable engines.
- **Filesystems** — local paths and remote buckets (S3, GCS, Azure) of
  CSV, Parquet, or JSONL files.
- **Postgres CDC** — logical-replication streaming with merge-write
  semantics and soft deletes.

New source types are Python entry-point plugins. A team can ship one in a
separate wheel without modifying this repo.

> Deep dive: [`sources/`](../src/dlt_data_pipeline/sources/README.md)

## Destinations

- **Snowflake** — primary warehouse target; supports both password and
  key-pair authentication.
- **Postgres** — native dlt adapter for warehouse-style Postgres.
- **DuckDB** — zero-config local destination, ideal for prototyping and
  CI.

Databricks consumes from Snowflake today via its External Data connection;
direct `dlt → Databricks` loads are deferred.

> Deep dive: [`destinations/`](../src/dlt_data_pipeline/destinations/README.md)

## Sync modes

- **Full refresh** — replace the destination table on every run.
- **Incremental** — cursor-based append, with late-arrival knobs
  (clock-skew tolerance and an explicit lookback window) for sources whose
  rows trickle in after their cursor timestamp.
- **CDC** — merge writes driven by logical replication, with soft deletes.

## Backfill

Any incremental pipeline can declare a backfill window in YAML and run a
chunked, resumable historical load. Each chunk is an isolated run, so a
crashed backfill resumes cleanly at the last completed chunk boundary
instead of re-reading everything.

## Configuration & environments

Pipelines are validated against a pydantic schema; the JSON Schema is
regenerated automatically and enforced by a pre-commit hook. Environment
overlays let a single base YAML target dev, staging, and prod by remapping
only a narrow set of fields (connections, dataset, schedule enablement,
worker resources). A `promote` command prints the diff between two
environments for one pipeline — informational, never mutating.

## Credentials

Credentials never live in YAML. Each pipeline references a **logical
connection name**, which resolves at runtime — in priority order — from a
process environment variable, the Airflow Secrets Backend (when
configured), or a local TOML file for development. Logs are passed
through a secret-scrubbing filter that rewrites passwords, tokens, and URI
userinfo to `***` everywhere — DAG parsing, CLI runs, and worker-pod logs.

## Operability

- **Validate without running.** Parse and schema-check every pipeline in
  sub-second time, with no database or Airflow connection required.
- **Doctor.** Probe every pipeline's expected credential sources and
  report exactly which are present and which are missing.
- **Row-count reconciliation.** Opt-in per pipeline; compares source and
  destination row counts after each load, in either cross-cluster or
  same-cluster mode.
- **CDC slot-lag monitoring.** A ready-made Airflow sensor fails on
  replication-lag spikes.
- **Idempotent teardown.** One command drops the replication slot,
  destination dataset, local state, and YAML in a single shot, with a
  dry-run preview.

## Observability & alerting

Failures, schema changes, missed SLAs, and scheduler heartbeats route
through a single alerting layer with Slack and SMTP transports, P1/P2
severity tagging, and a configurable dedup window so multi-hour incidents
don't spam channels. A dedicated heartbeat DAG polls the Airflow scheduler
every five minutes and pages on stale liveness.

## Orchestration

The same container image runs Airflow in `LocalExecutor` mode for local
development and `KubernetesExecutor` mode in production — no code or
Dockerfile divergence between the two. Per-pipeline CPU and memory limits
are declared in YAML and flow through to per-task pod overrides, so one
heavy sync can't starve smaller ones. SLAs declared in YAML wire
automatically into Airflow task-level SLAs and a `sla_miss_callback`.

## Deployment

- **Local.** A `docker compose` stack brings up Airflow (webserver,
  scheduler, triggerer), a metadata Postgres, a CDC-capable source
  Postgres, and a destination Postgres in one command.
- **Production.** Raw Kubernetes manifests under `deploy/k8s/base/`
  describe the full KubernetesExecutor deployment, including RBAC,
  ConfigMaps, and the worker pod template.
- **Image distribution.** CI builds and pushes a hardened production
  image to GHCR on every tag.

## Developer experience

- A scaffolder generates new pipeline YAMLs with TODO placeholders and
  the editor-autocomplete directive pre-filled.
- A ruff import-boundary rule keeps pipeline logic Airflow-agnostic so
  the same code works from the CLI, from tests, and from a future
  non-Airflow orchestrator.
- Pre-commit hooks enforce formatting, schema regeneration, and YAML
  validity.
- Comprehensive unit and integration test suites, with live-credential
  tests gated behind environment variables so CI stays green when external
  systems aren't available.

## AI-agent friendly

The repo ships a local MCP server registered in `.mcp.json` that exposes
the introspection surface — listing source types, describing one source's
configuration shape, validating pipelines, diagnosing missing credentials,
and diffing environment overlays — as native tool calls. A bundled
slash-skill walks an agent through the scaffold → validate → doctor flow
for adding a new pipeline. Together these let agents like Claude Code
operate the platform without shelling out.

> Deep dive: [AGENTS.md](../AGENTS.md)

## Comparison with Fivetran

Honest side-by-side. Where Fivetran wins, we say so.

| Capability | Fivetran | `dlt_data_pipeline` | Notes |
|---|---|---|---|
| Connector breadth | 500+ managed connectors | 4 source types in-tree, plugin-extensible | Fivetran wins on out-of-the-box coverage; we win on customization. |
| Source onboarding | UI wizard, click-through | YAML edit + scaffolder + validator | Fivetran is faster for non-engineers; YAML is faster for engineers who already have the connection string. |
| Sync modes | Full / incremental / CDC | Full / incremental / CDC | Functional parity. |
| Postgres CDC | Managed, abstracted | Self-hosted logical replication via dlt `pg_replication` | We require `wal_level=logical` and a replication role on the source. |
| Schema evolution | Auto-handled silently | dlt inference + opt-in schema-change alert | We surface evolution as an event rather than silently widening tables. |
| Backfill | "Resync" / historical sync | Chunked, resumable historical load via CLI | Comparable — ours is explicit about chunk boundaries. |
| Monitoring & alerting | Hosted dashboard + email | Slack + SMTP routing + Airflow UI + heartbeat DAG | Different surfaces; same outcomes. |
| Quality checks | Row-count history per table | Opt-in row-count reconciliation per pipeline | Comparable; ours is per-pipeline opt-in to control cost. |
| Secrets management | Fivetran-managed | Env vars, Airflow Secrets Backend, or local TOML | Auto-scrubbed in logs. |
| Deployment model | SaaS | Self-hosted on Docker / Kubernetes | Trade-off: ops burden vs. data residency, network egress, and control. |
| Cost model | Per-MAR (Monthly Active Rows) | Infrastructure-only | Becomes attractive at scale; less so for small volumes. |
| Customization | Limited to connector settings | Full Python access to dlt sources and destinations | We can fix or fork anything. |
| Governance / PII masking | Built-in masking / hashing | **Not in v1** | Tracked as a deliberate post-v1 item. |
| Vendor lock-in | Proprietary connectors and pipelines | Open-source dlt + Airflow + YAML | Pipelines are portable artifacts. |

**Where Fivetran is the right call:** small data team without ops capacity,
need for a long tail of niche SaaS connectors, or no engineering appetite
for self-hosting Airflow.

**Where this platform is the right call:** existing Airflow + Kubernetes
investment, mostly-warehouse + Postgres + S3 + REST shapes, sensitivity to
per-MAR pricing, or a need for source-level customization that managed
connectors don't allow.

## Feasibility with our current tech stack

How the platform maps onto the systems we already run:

- **Snowflake** — first-class destination; both password and key-pair
  authentication are supported. **Fit: green.**
- **Databricks** — direct `dlt → Databricks` is stubbed and will raise at
  build time. The shipping pattern is `dlt → Snowflake → Databricks
  External Data`, which matches our current architecture. **Fit: yellow
  (works via Snowflake; no direct path).**
- **Postgres** — supported as both a source (regular SQL and CDC) and a
  destination. CDC requires `wal_level=logical` and a replication-capable
  role on the source database. **Fit: green.**
- **Airflow** — already our chosen orchestrator; one container image
  serves both LocalExecutor (dev) and KubernetesExecutor (prod). **Fit:
  green.**
- **Kubernetes** — production deploy target; raw kubectl manifests are
  shipped under `deploy/k8s/base/`. **Fit: green.**
- **REST APIs, S3, GCS, Azure files** — covered by the `rest_api` and
  `filesystem` source types out of the box. **Fit: green.**
- **Secrets** — works with Kubernetes `Secret` env-mounts, the Airflow
  Secrets Backend, or local TOML, so it slots into common org patterns.
  **Fit: green.**

**Summary.** Green across the Snowflake + Postgres + Airflow + Kubernetes
shape that already exists today. Yellow on direct Databricks loads (use
the Snowflake-then-External-Data pattern instead). Yellow on PII /
governance tooling, which is deferred to post-v1.

## Further reading

| Document | Purpose |
|---|---|
| [README](../README.md) | Architecture diagram, quickstart, 10-minute add-a-pipeline flow. |
| [AGENTS.md](../AGENTS.md) | CLI surface, env-var convention, CDC operations, known limitations. |
| [`data_pipeline_plan.md`](../data_pipeline_plan.md) | Design log, segment history, post-v1 considerations. |
| [`config/`](../src/dlt_data_pipeline/config/README.md) | Pydantic models, YAML loader, environment overlays, secrets resolver. |
| [`sources/`](../src/dlt_data_pipeline/sources/README.md) | Source-type plugin registry. |
| [`destinations/`](../src/dlt_data_pipeline/destinations/README.md) | Destination factory and metadata. |
| [`airflow/`](../src/dlt_data_pipeline/airflow/README.md) | DAG factory, callbacks, sensors, quality tasks. |
| [`observability/`](../src/dlt_data_pipeline/observability/README.md) | Alerts and secret-scrubbing log filter. |
| [`cli/`](../src/dlt_data_pipeline/cli/README.md) | CLI subcommands reference. |
| [`pipelines/`](../pipelines/README.md) | User-facing YAMLs, schema, environment overlays. |
| [`docker/`](../docker/README.md) | Local development compose stack. |
| [`deploy/k8s/base/`](../deploy/k8s/base/README.md) | Production KubernetesExecutor manifests. |
| [`tests/`](../tests/README.md) | Unit and integration test layout, live-credential gating. |
