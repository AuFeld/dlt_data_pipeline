# `docker/` — single shared image + local-dev compose stack

One [`Dockerfile`](Dockerfile) builds two targets (`dev` and `prod`);
both share a `base` stage. Same image runs the webserver, scheduler,
triggerer, and per-task worker pods — and per Segment 6 done-when, runs
the CLI standalone for one-shot pipeline invocations
(`docker run … python -m dlt_data_pipeline run <name>`). This is the
prod run image consumed by KubernetesExecutor.

## Build targets

| Target | When | Notes |
|---|---|---|
| `base` | (intermediate) | `python:3.12-slim-bookworm` + OS deps (libpq5, tini, curl, ca-certificates) + pinned `uv`. |
| `dev` | Compose default; local pytest + live-PG fixtures. | `uv sync --extra dev`. Bakes `airflow.cfg` + pod templates. |
| `prod` | Pushed to GHCR by CI on tag. | `uv sync --no-dev`. No dev extras; slimmer venv. |

Build locally:

```bash
docker build -f docker/Dockerfile --target dev -t dlt_data_pipeline:local .
docker build -f docker/Dockerfile --target prod -t dlt_data_pipeline:prod .
```

## Compose stack ([`docker-compose.yml`](docker-compose.yml))

Project name: `dlt-data-pipeline`. Services:

- `postgres-airflow` — metadata DB for the scheduler / webserver.
  Persistent named volume `postgres_airflow_data`.
- `postgres-source` — Postgres 16 with `wal_level=logical` +
  `max_replication_slots=4` + `max_wal_senders=4` for CDC. Exposed on
  `localhost:5433`. Init scripts under
  [`postgres-source-init/`](postgres-source-init/) seed the `replicator`
  role.
- `postgres-destination` — Postgres 16 warehouse. Exposed on
  `localhost:5434`.
- `airflow-init` — runs `airflow db migrate` and creates the admin user
  (admin / admin). One-shot — webserver / scheduler / triggerer depend
  on it via `service_completed_successfully`.
- `airflow-webserver` — UI on `localhost:8080`. Healthcheck against
  `/health`.
- `airflow-scheduler` — DAG scheduling + (with LocalExecutor) task
  execution.
- `airflow-triggerer` — deferrable-operator host.

Persistent volumes: `postgres_airflow_data`, `postgres_source_data`,
`postgres_destination_data`, `airflow_logs`, `dlt_data` (mounted at
`/opt/app/.dlt` to persist incremental cursors + working state across
restarts).

Bring up:

```bash
docker compose -f docker/docker-compose.yml up -d
scripts/seed_local.sh
open http://localhost:8080
```

## LocalExecutor vs KubernetesExecutor

Compose runs `LocalExecutor` for dev ergonomics. The committed
`airflow.cfg` is the baseline; production overrides
`AIRFLOW__CORE__EXECUTOR=KubernetesExecutor` via env, per the
"do not edit `airflow.cfg`, env-override instead" rule
([`AGENTS.md`](../AGENTS.md)). The same image serves both — no per-env
image divergence. Prod manifests in
[`deploy/k8s/base/`](../deploy/k8s/base/README.md).

## Known issues / troubleshooting

> **Airflow install fragility:** Airflow refuses unpinned dependency
> resolution. Always install with the matching
> `constraints-<version>-<python>.txt`. Document in `pyproject.toml`
> comments and the CI workflow; pin the Python version alongside.

Inside this Dockerfile the pinned-Python guarantee is `python:3.12-slim-bookworm`
on the `FROM` line, and `uv sync --frozen` enforces the `uv.lock`
contract on every layer. Bumping the Python base image without
regenerating `uv.lock` against the new interpreter will produce a
broken Airflow install at runtime — re-lock in the same commit.
