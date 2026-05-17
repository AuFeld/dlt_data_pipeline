# `sources/` — source-type plugin registry

Each source type is a subpackage that exposes a `builder(config) -> DltSource`
plus a `metadata` object describing its config surface. Discovery happens at
import time via two `pyproject.toml` entry-point groups — no hardcoded
`{name -> builder}` map.

## Files

- [`_protocol.py`](_protocol.py) — `Builder` Protocol. Every builder takes a
  validated `SourceConfig` (discriminated-union member) and returns a runnable
  `dlt.DltSource`.
- [`_metadata.py`](_metadata.py) — `SourceTypeMetadata` shape used by
  `sources describe` / `sources_describe` MCP tool.
- [`registry.py`](registry.py) — entry-point loader + cached lookup. Raises
  `UnknownSourceTypeError` on unknown `type:`; `MissingSourceMetadataError`
  when a builder is registered without matching metadata.
- [`rest_api/`](rest_api/), [`sql_database/`](sql_database/),
  [`filesystem/`](filesystem/), [`pg_cdc/`](pg_cdc/) — built-in source-type
  subpackages.

## Add a new source type

1. Create `src/dlt_data_pipeline/sources/<name>/__init__.py` exposing a
   `builder(config: SourceConfig) -> DltSource`.
2. Create `src/dlt_data_pipeline/sources/<name>/_metadata.py` exposing a
   `metadata: SourceTypeMetadata` constant.
3. Register both in `pyproject.toml`:

   ```toml
   [project.entry-points."dlt_data_pipeline.sources"]
   my_source = "dlt_data_pipeline.sources.my_source:builder"

   [project.entry-points."dlt_data_pipeline.sources.metadata"]
   my_source = "dlt_data_pipeline.sources.my_source._metadata:metadata"
   ```

4. Add a discriminated-union member to
   [`config/models.py`](../config/models.py) so pydantic accepts `type:
   my_source` in YAML.
5. `uv sync` to re-install entry points; verify with
   `python -m dlt_data_pipeline sources list`.

Out-of-tree plugins follow the same contract — install any wheel that
registers the two groups and the type becomes available.

## CDC source ([`pg_cdc/`](pg_cdc/))

Wraps the vendored
[`dlt pg_replication`](pg_cdc/_vendor/NOTICE.md) verified source. The
builder calls `init_replication` eagerly (idempotent — the vendor raises
`RuntimeError` on subsequent calls with `persist_snapshots=True`; the
builder swallows it).

Source-Postgres setup:

- `wal_level=logical` plus `max_replication_slots` / `max_wal_senders`
  (docker-compose `postgres-source` ships with all three).
- Connection role with `REPLICATION` attribute. Least-privilege:
  [`docker/postgres-source-init/01_replication.sql`](../../../docker/postgres-source-init/01_replication.sql)
  creates a dedicated `replicator` role.
- Each replicated table needs a `REPLICA IDENTITY` (PK default is enough).

Slot + publication lifecycle:

- `source.config.reset: true` for a one-shot run that drops + recreates
  the slot + publication. Use after an incompatible DDL change.
- `options.write_disposition` is silent-promoted from default `append` →
  `merge` for CDC; `merge` / `replace` pass through unchanged.
- Each pipeline owns its own `slot_name` AND `publication_name`; sharing
  either across pipelines is unsupported.
- Tear down via `python -m dlt_data_pipeline pipelines delete <name>
  --yes` — drops slot, publication, destination dataset, local state,
  and the YAML in one shot.

Slot-lag monitoring (opt-in):
[`PgReplicationSlotLagSensor`](../airflow/sensors.py) — wire into a
separate monitoring DAG. Not auto-wired into generated CDC DAGs; routing
to alerts is observability's job (see
[`observability/README.md`](../observability/README.md)).

## Incremental edge-case knobs

`SyncConfig.tolerance_seconds` and `SyncConfig.lookback` (per
[`config/README.md`](../config/README.md#incremental-knobs)) absorb
cursor-vs-commit skew and out-of-order late arrivals. Opt-in per
pipeline — defaults are zero.

## Known issues / troubleshooting

> **CDC setup:** source PG needs `wal_level=logical`, `max_replication_slots`,
> `max_wal_senders` (compose `command:` flags; managed PG needs param-group
> change + restart). Replication slot retains WAL until consumed — a paused
> pipeline fills source disk; needs monitoring + cleanup path. Test DELETE
> propagation and snapshot→streaming handoff explicitly.

> **Incremental edge cases:** cursor-based incremental loses rows when the
> source writes `updated_at` slightly before commit or under timezone skew
> between source and Airflow worker. `sync.tolerance_seconds` (dlt
> `Incremental.lag`) and `sync.lookback` (ISO-8601 duration; composes with
> tolerance) re-read a trailing window each run to catch late arrivals.
> Defaults are zero — explicit opt-in per pipeline. Cursor gaps after a
> failed-and-retried run are absorbed by the same lookback window; without
> it, deletes-after-cursor and out-of-order inserts go missing. Segment 12
> owns the implementation contract; see `config/models.py:123-124` and the
> AGENTS.md "Backfill an incremental pipeline" section.

> **Monitoring (CDC slot lag):** For CDC, monitor replication slot lag on
> the source DB — outside Airflow's view, expose via a periodic check
> task that emits a metric and fails the DAG when over threshold. (The
> Airflow-half of this Tricky Part — `on_failure_callback`, freshness,
> XCom — lives in [`airflow/README.md`](../airflow/README.md).)
