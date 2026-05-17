-- Source-DB schema + seed rows for the Postgres -> Postgres integration test
-- (and the Airflow UI quickstart). Single source of truth shared between
-- scripts/seed_local.sh (docker compose dev) and .github/workflows/ci.yml
-- (GHA services postgres in integration-postgres job).
--
-- Idempotent: CREATE TABLE IF NOT EXISTS + ON CONFLICT DO NOTHING. Safe to
-- re-run on a pre-seeded DB.

CREATE TABLE IF NOT EXISTS public.orders (
  id INTEGER PRIMARY KEY,
  amount NUMERIC(12, 2) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS public.customers (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.orders (id, amount, updated_at) VALUES
  (1, 10.00, '2025-01-01 00:00:00+00'),
  (2, 20.00, '2025-01-01 00:01:00+00'),
  (3, 30.00, '2025-01-01 00:02:00+00')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.customers (id, name, updated_at) VALUES
  (1, 'alice', '2025-01-01 00:00:00+00'),
  (2, 'bob',   '2025-01-01 00:01:00+00')
ON CONFLICT (id) DO NOTHING;

-- Segment 7 / CDC: every cdc pipeline owns its own publication via
-- init_replication(). The example_pg_cdc_to_pg pipeline references
-- publication 'dlt_orders_pub' + slot 'dlt_orders_slot' — predeclare the
-- publication idempotently so first-run snapshot creation succeeds even if
-- the `source` role's CREATE privilege has been narrowed. Re-running the
-- seed leaves an existing publication untouched. Harmless on a
-- non-replication-configured DB (e.g. GHA services postgres without
-- wal_level=logical) — CREATE PUBLICATION does not require wal_level=logical.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dlt_orders_pub') THEN
    CREATE PUBLICATION dlt_orders_pub FOR TABLE public.orders WITH (publish = 'insert, update, delete');
  END IF;
END
$$;

-- Required by Postgres logical replication: every replicated table must
-- declare a REPLICA IDENTITY so UPDATE/DELETE rows carry the old PK in
-- the WAL stream. The PK index satisfies the DEFAULT identity, but state
-- it explicitly so the requirement is grep-able when CDC tests fail.
ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;
