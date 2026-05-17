#!/usr/bin/env bash
# Bring up the Segment 6 local stack and seed the source Postgres.
#
# Idempotent: safe to re-run after `docker compose down` or `restart`. The
# DDL uses CREATE TABLE IF NOT EXISTS and inserts use ON CONFLICT DO NOTHING,
# so reruns won't fail or duplicate rows.
#
# Usage: bash scripts/seed_local.sh   (from repo root or anywhere)

set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker/docker-compose.yml)

# 1. .env bootstrap
if [ ! -f .env ]; then
  echo "[seed] creating .env from .env.example"
  cp .env.example .env
fi

# Auto-populate Fernet key on first run so saved Airflow Connections survive
# a webserver restart. A Fernet key is just 32 random bytes encoded as
# url-safe base64 — generated here via stdlib so there's no `cryptography`
# package requirement on the host python.
if grep -qE '^AIRFLOW__CORE__FERNET_KEY=$' .env; then
  echo "[seed] generating AIRFLOW__CORE__FERNET_KEY in .env"
  if command -v python3 >/dev/null 2>&1; then
    PY=python3
  elif command -v python >/dev/null 2>&1; then
    PY=python
  else
    echo "[seed] no python on PATH; can't generate Fernet key. Install python3 and re-run." >&2
    exit 1
  fi
  fernet=$("$PY" -c 'import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())')
  # Portable in-place edit (works on BSD + GNU sed without -i quirks).
  awk -v k="$fernet" '/^AIRFLOW__CORE__FERNET_KEY=$/ {print "AIRFLOW__CORE__FERNET_KEY=" k; next} {print}' \
    .env > .env.tmp && mv .env.tmp .env
fi

# 2. Bring up Postgres services first.
"${COMPOSE[@]}" up -d postgres-airflow postgres-source postgres-destination

# 3. Wait for postgres-source.
echo "[seed] waiting for postgres-source to be healthy…"
for _ in $(seq 1 30); do
  if "${COMPOSE[@]}" exec -T postgres-source pg_isready -U source -d source_db >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# 4. Apply schema + seed rows (idempotent). DDL lives in scripts/seed_source.sql
#    so CI (.github/workflows/ci.yml integration-postgres job) and local dev
#    share one source of truth.
"${COMPOSE[@]}" exec -T postgres-source psql -U source -d source_db < scripts/seed_source.sql

echo "[seed] source DB ready."

# 5. Bring up the airflow services (init runs once, then webserver/scheduler/
#    triggerer come up).
"${COMPOSE[@]}" up -d airflow-init airflow-webserver airflow-scheduler airflow-triggerer

cat <<'NEXT'

[seed] done.

Next steps:
  - Airflow UI:  http://localhost:8080   (admin / admin)
  - Trigger any pipeline DAG from the UI, OR run a pipeline standalone:
      docker compose -f docker/docker-compose.yml run --rm airflow-scheduler \
        python -m data_pipeline_template run example_rest_to_duckdb
  - Run the live-Postgres integration test (tests/ is not baked into the
    image — bind-mount it for this one invocation):
      docker compose -f docker/docker-compose.yml run --rm \
        -e RUN_LIVE_PG=1 \
        -v "$(pwd)/tests:/opt/app/tests" \
        airflow-scheduler \
        pytest tests/integration/test_sql_database_incremental.py::test_live_pg_to_pg_incremental
  - Run the live-Postgres CDC integration test (also bind-mounts tests/):
      docker compose -f docker/docker-compose.yml run --rm \
        -e RUN_LIVE_PG_CDC=1 \
        -v "$(pwd)/tests:/opt/app/tests" \
        airflow-scheduler \
        pytest tests/integration/test_pg_cdc.py
  - Inspect replication slot lag from the host:
      docker compose -f docker/docker-compose.yml exec -T postgres-source \
        psql -U source -d source_db -c \
        "SELECT slot_name, confirmed_flush_lsn, \
         pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes \
         FROM pg_replication_slots;"

After editing .env (e.g. swapping credentials), `docker compose restart` does
NOT pick up the new values — env_file is only read at container creation.
Recreate the airflow services:
  docker compose -f docker/docker-compose.yml up -d --force-recreate \
    --no-deps airflow-scheduler airflow-webserver airflow-triggerer
NEXT
