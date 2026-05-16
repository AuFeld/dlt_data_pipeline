# Pipeline YAML schema

One YAML per pipeline lives in this directory. The loader at
`src/data_pipeline_template/config/loader.py` discovers every `*.yml` (files
prefixed with `_` are skipped, like this one), validates against
[`PipelineConfig`](../src/data_pipeline_template/config/models.py), and
aggregates all errors before raising.

## Top-level fields

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | Must match `^[a-z][a-z0-9_]*$`. Used as dataset / DAG id. |
| `source` | mapping | yes | Discriminated on `source.type`. |
| `sync` | mapping | yes | Mode and cursor / primary-key hints. |
| `destination` | mapping | yes | Target dlt destination. |
| `schedule` | mapping | yes | Cron + enabled flag. |
| `options` | mapping | no | Write disposition / schema contract. Defaults below. |

Unknown keys at any level are rejected (`extra="forbid"`) so typos fail loudly.

## Source types

| `source.type` | Builder lands in | Notes |
| --- | --- | --- |
| `rest_api` | Segment 3 | Wraps `dlt.sources.rest_api`. |
| `sql_database` | Segment 5 | Wraps `dlt.sources.sql_database` (Postgres in v1; any SQLAlchemy URL). See `sql_database` config below. |
| `filesystem` | Segment 8 | Local + S3, CSV/Parquet/JSONL. |
| `pg_cdc` | Segment 7 | Postgres logical replication via `dlt.sources.pg_replication`. |

`source.connection` is a **logical name** resolved at runtime from
`.dlt/secrets.toml` (local) or env (`DESTINATION__<TYPE>__CREDENTIALS=...`,
`SOURCES__<NAMESPACE>__CREDENTIALS=...`). Never put raw credentials in YAML.

`source.config` is a free-form mapping today; typed sub-models will be added
per source type as each builder lands.

### `sql_database` source config

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `tables` | yes | — | Non-empty list of table names. Drives per-table Airflow tasks. |
| `schema` | no | dialect default | DB schema (e.g. `public`). |
| `defer_table_reflect` | no | `true` | When `true`, the source does not open a DB connection at DAG-parse time. Required if you want strict reflected column types — set `false` to reflect on each parse. |
| `backend` | no | `sqlalchemy` | Pass-through to dlt: `sqlalchemy` / `pyarrow` / `pandas` / `connectorx`. |
| `chunk_size` | no | dlt default | Pass-through. |
| `include_views` | no | `false` | Pass-through. |
| `reflection_level` | no | `minimal` | Pass-through (`minimal` / `full` / `full_with_precision`). |

Credentials resolve from `SOURCES__SQL_DATABASE__<CONNECTION_NAME_UPPER>__CREDENTIALS`
(env) or `[sources.sql_database.<connection>.credentials]` in
`.dlt/secrets.toml`. Value is any SQLAlchemy URL
(e.g. `postgresql://user:pw@host:5432/db`, `sqlite:///path/to.db`).

**Cursor constraint:** every table listed in `tables` must expose a column
with the name set in `sync.cursor_field`. Per-table cursor overrides are a
future enhancement.

## Sync modes — cross-field rules

| `sync.mode` | Required fields | Forbidden | Other constraints |
| --- | --- | --- | --- |
| `full_refresh` | — | `cursor_field` | |
| `incremental` | `cursor_field` | — | `primary_key` recommended for merge. |
| `cdc` | `primary_key` | — | `source.type` must be `pg_cdc`. |

Independent of mode: `options.write_disposition: merge` requires
`sync.primary_key`.

## Destination types

`postgres`, `snowflake`, `databricks`, `duckdb`. `destination.connection` is a
logical name (same resolution rules as `source.connection`).

## Options defaults

| Field | Default | Allowed |
| --- | --- | --- |
| `options.write_disposition` | `append` | `append`, `replace`, `merge` |
| `options.schema_contract` | `evolve` | `evolve`, `freeze`, `discard_row` |

## Schedule

`schedule.cron` must be five space-separated fields using `[0-9*/,\-?]`.
Semantic validity (e.g. minute in `0–59`) is checked by Airflow at DAG parse
in Segment 4.

`schedule.enabled` defaults to `true`.

## Minimal example

```yaml
name: orders_pg_to_warehouse
source:
  type: sql_database
  connection: pg_source
  config:
    schema: public
    tables: [orders]
sync:
  mode: incremental
  cursor_field: updated_at
  primary_key: id
destination:
  type: postgres
  connection: pg_warehouse
  dataset: raw_orders
schedule:
  cron: "0 */2 * * *"
options:
  write_disposition: merge
```
