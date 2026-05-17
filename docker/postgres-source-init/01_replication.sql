-- Segment 7 — postgres-source role setup for logical replication.
--
-- The official postgres image runs every .sql under /docker-entrypoint-initdb.d
-- exactly once, against the freshly initialized cluster, before the server
-- accepts external connections. That means re-running this file requires
-- `docker compose down -v` (wipes the postgres_source_data volume).
--
-- Two roles are granted REPLICATION:
--   * `source` (login user from POSTGRES_USER) — keeps Segment 5 tests + the
--     example_pg_to_pg_incremental pipeline working without a separate
--     connection, and is the user docker-compose's healthcheck already uses.
--   * `replicator` (dedicated, least-privilege) — the canonical user the prod
--     story will document. REPLICATION + pg_read_all_data only; no write
--     access. Use this in YAML logical connections that only need CDC.
--
-- The publication is intentionally NOT created here — pipelines create their
-- own publications via the vendored init_replication() call so each pipeline
-- owns its slot + publication lifecycle independently.

ALTER ROLE source WITH REPLICATION;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD 'replicator';
  END IF;
END
$$;

GRANT pg_read_all_data TO replicator;
GRANT CREATE ON DATABASE source_db TO replicator;
