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
| `sql_database` | Segment 5 | Postgres + MySQL via `dlt.sources.sql_database`. |
| `filesystem` | Segment 8 | Local + S3, CSV/Parquet/JSONL. |
| `pg_cdc` | Segment 7 | Postgres logical replication via `dlt.sources.pg_replication`. |

`source.connection` is a **logical name** resolved at runtime from
`.dlt/secrets.toml` (local) or env (`DESTINATION__<TYPE>__CREDENTIALS=...`,
`SOURCES__<NAMESPACE>__CREDENTIALS=...`). Never put raw credentials in YAML.

`source.config` is a free-form mapping today; typed sub-models will be added
per source type as each builder lands.

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
