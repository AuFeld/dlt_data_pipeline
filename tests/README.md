# `tests/` — unit + integration test suite

Two-tier layout: `unit/` is pure Python (no external services, runs on
every commit); `integration/` is hermetic-by-default with opt-in live
modes via env-var gates.

## Layout

- [`unit/`](unit/) — config validation, loader discovery, registry
  lookups, CLI subcommand exit codes, dag_factory output assertions,
  alerts dispatch + dedup. Fast — every commit.
- [`integration/`](integration/) — end-to-end runs against duckdb
  (hermetic), pod-override shape assertions, alerting E2E with mocked
  webhooks, and the live-creds suites listed below.
- [`fixtures/`](fixtures/) — sample files, SQL seed scripts, recorded
  cassettes.
- [`conftest.py`](conftest.py) — session-wide autouse fixture that
  stubs `SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS` with a SQLite
  path so the DagBag scan in `test_dagbag.py` works hermetically.

## Live-creds gating

Tests that need real external services skip cleanly when env vars are
absent — CI stays green either way.

| Suite | Env gate | Notes |
|---|---|---|
| [`integration/test_destinations_snowflake.py`](integration/test_destinations_snowflake.py) | `SNOWFLAKE_TEST_ACCOUNT` (+ `_USER`, `_PASSWORD`, etc.) | Runs the snowflake destination factory + a roundtrip load. |
| [`integration/test_sql_database_incremental.py`](integration/test_sql_database_incremental.py) | `RUN_LIVE_PG=1` + `SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS` + `DESTINATION__PG_WAREHOUSE__CREDENTIALS` | Uses docker-compose `postgres-source` + `postgres-destination`. |
| [`integration/test_pg_cdc.py`](integration/test_pg_cdc.py) | `RUN_LIVE_PG_CDC=1` + the same Postgres creds | Needs `wal_level=logical` on the source. |
| [`integration/test_pipelines_delete_pg_cdc.py`](integration/test_pipelines_delete_pg_cdc.py) | `RUN_LIVE_PG_CDC=1` | Verifies slot / publication / dataset / state / YAML teardown. |

## Run a subset

```bash
# Pure unit suite — fast, no services
uv run pytest tests/unit

# Hermetic integration (duckdb, mocked webhooks)
uv run pytest tests/integration -m "not postgres and not cdc"

# Live Postgres (compose up first)
docker compose -f docker/docker-compose.yml up -d postgres-source postgres-destination
RUN_LIVE_PG=1 \
  SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS=postgresql://source:source@localhost:5433/source_db \
  DESTINATION__PG_WAREHOUSE__CREDENTIALS=postgresql://warehouse:warehouse@localhost:5434/warehouse_db \
  uv run pytest tests/integration -m "postgres and not cdc"

# Live CDC
RUN_LIVE_PG_CDC=1 ... uv run pytest tests/integration/test_pg_cdc.py
```

## Known issues / troubleshooting

- **Live-creds tests do NOT auto-pull-up compose**: each live suite
  expects you to have `docker compose up` running already. Running them
  cold prints the gate's skip message rather than spinning up services.
- **`conftest.py` SQLite stub is autouse**: a test that wants the real
  `SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS` must set the env var
  before pytest starts. The session-scoped fixture is idempotent — it
  only injects when the var is unset.
- **CDC tests are stateful by design**: aborting a CDC test mid-run can
  leave a replication slot + publication on the source. Cleanup via
  `python -m dlt_data_pipeline pipelines delete <name> --yes` or drop
  the slot manually with `SELECT pg_drop_replication_slot('...');` +
  `DROP PUBLICATION ...;`.
- **Snowflake integration test cost**: each run does a real account
  roundtrip. Don't enable `SNOWFLAKE_TEST_ACCOUNT` in CI without a
  dedicated test warehouse / dataset.
