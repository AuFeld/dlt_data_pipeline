# `pipelines/` — user-facing YAML configs

One YAML per pipeline. Adding a pipeline is a YAML edit in the common
case (no Python). The pydantic schema in
[`src/dlt_data_pipeline/config/models.py`](../src/dlt_data_pipeline/config/models.py)
is the source of truth; [`_schema.json`](_schema.json) is regenerated
from it via the `regen-pipeline-schema` pre-commit hook.

## Files

- [`_schema.md`](_schema.md) — human-readable YAML reference.
- [`_schema.json`](_schema.json) — JSON Schema for LSP-aware editors and
  the `yaml-language-server` directive (do NOT edit by hand — regenerate
  with `python -m dlt_data_pipeline config schema`).
- [`_env/dev.yml`](_env/dev.yml), [`_env/prod.yml`](_env/prod.yml) —
  per-environment overlays (Segment 13).
- `example_*.yml` — six worked examples covering REST→duckdb, pg→pg
  (incremental + quality reconciliation), pg→pg CDC, pg→snowflake CDC,
  filesystem→duckdb.

## Editor autocomplete

Every YAML carries the directive:

```yaml
# yaml-language-server: $schema=./_schema.json
```

The scaffolder ([`scripts/new_pipeline.py`](../scripts/new_pipeline.py))
writes this header automatically. Manual edits should preserve it.

## Env overlays ([`_env/`](_env/))

Re-map a narrow set of fields per environment without editing the base
YAML. Keyed by pipeline name:

```yaml
# pipelines/_env/prod.yml
orders_pg_to_warehouse:
  destination:
    type: snowflake
    connection: snowflake_warehouse
    dataset: prod_raw_orders
  resources:
    cpu: "1000m"
    memory: "2Gi"
```

Allowed scope (v1): `source.connection`,
`destination.{type,connection,dataset}`, `schedule.enabled`,
`resources.{cpu,memory}`. Out-of-scope keys raise `ConfigError`. Full
semantics in [`config/README.md`](../src/dlt_data_pipeline/config/README.md#environment-overlays-segment-13).

## Add a pipeline

```bash
uv run python scripts/new_pipeline.py my_pipeline \
    --source rest_api --dest duckdb
```

Writes `pipelines/my_pipeline.yml` with required keys as TODO placeholders.
Validate immediately:

```bash
uv run python -m dlt_data_pipeline pipelines validate my_pipeline
uv run python -m dlt_data_pipeline pipelines doctor
```

Full 10-min flow in the root [`README.md`](../README.md#add-a-pipeline-in-10-minutes).

## Tear down a pipeline

```bash
uv run python -m dlt_data_pipeline pipelines delete <name> --yes
```

Drops CDC slot + publication (pg_cdc), drops the destination dataset
(unless `--keep-data`), wipes local state, unlinks the YAML. Idempotent
— see [`cli/README.md`](../src/dlt_data_pipeline/cli/README.md#python--m-dlt_data_pipeline-pipelines-delete-name).

## Known issues / troubleshooting

> **Schema evolution:** expose dlt `schema_contract` per pipeline. `evolve`
> default is convenient but silently adds columns; recommend `freeze` for
> regulated destinations. Surface schema-change events in DAG-run metadata
> via `XCom` or task logs.
