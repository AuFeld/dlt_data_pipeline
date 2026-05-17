# `cli/` — `python -m dlt_data_pipeline …` subcommands

Argparse-driven CLI. Every subcommand is a thin wrapper around a **pure
data helper** that returns a dict — the helpers are the layer the
[MCP server](../mcp_server.py) imports directly so MCP tools call them
without subprocess hops.

Entry: [`src/dlt_data_pipeline/__main__.py`](../__main__.py). Helpers:
[`backfill_cmds.py`](backfill_cmds.py), [`config_cmds.py`](config_cmds.py),
[`delete_cmds.py`](delete_cmds.py),
[`pipelines_cmds.py`](pipelines_cmds.py), [`sources_cmds.py`](sources_cmds.py).

## Helper-vs-CLI split

Each command lives as two layers:

- **Pure helper** (e.g. `validate_pipelines`, `doctor_pipelines`,
  `list_sources`) returns a structured `dict[str, object]` with a `status`
  key. Never calls `sys.exit` or `print`. Importable from tests and the
  MCP server.
- **CLI wrapper** (e.g. `_cmd_pipelines_validate`) calls the helper,
  prints human-friendly output, and translates `status` → exit code.

MCP tool authors should import from the helper layer, not shell out. See
[`mcp_server.py`](../mcp_server.py) for the existing five wrappers.

## Subcommand reference

All pipelines-reading subcommands accept `--env <name>`; defaults to
`$DLT_ENV` then `dev` via `config.loader.resolve_env`. Examples covered
more fully in [`AGENTS.md`](../../../AGENTS.md).

### `python -m dlt_data_pipeline run <name>`

Execute one pipeline end-to-end. Flags: `--limit N` (per-resource cap via
`DltSource.add_limit`), `--no-load` (extract + normalize, skip
destination write), `--pipelines-root`, `--env`. Exit 0 on success.

### `python -m dlt_data_pipeline run-backfill <name>`

Chunked, resumable historical load for an `incremental`-mode pipeline that
declares `sync.backfill` (Segment 12). Required: `--start <ts>`, `--end
<ts>` (ISO-8601). Each chunk runs as an isolated `pipeline.run()` with
bounded `initial_value` / `end_value`. Re-invoke with `--start` matching
the last completed chunk boundary to resume.

### `python -m dlt_data_pipeline config schema`

Dump `pipelines/_schema.json` from the pydantic models. Flags: `--check`
(exit 1 if on-disk JSON drifts from the live model — used by the
`regen-pipeline-schema` pre-commit hook), `--out <path>` (write to a
custom location).

### `python -m dlt_data_pipeline sources list`

Print registered source types (entry-point group
`dlt_data_pipeline.sources`). No flags.

### `python -m dlt_data_pipeline sources describe <type>`

Print the source's env-var template, required + allowed `source.config`
keys, and source-specific notes. Exits non-zero on unknown type.

### `python -m dlt_data_pipeline pipelines validate [name]`

Parse + validate one or all `pipelines/*.yml` against the pydantic schema.
No DB connection, no Airflow process. Exit 0 if clean, exit 1 with
aggregated errors. Flag: `--env`.

### `python -m dlt_data_pipeline pipelines doctor`

For each pipeline, derive the expected env var from source + destination
metadata, then probe `os.environ` and `dlt.secrets`. Reports one of `env
/ airflow-backend / secrets-toml / no-creds-required / MISSING` per slot.
Exits non-zero if any pipeline has `MISSING` rows. Flag: `--env`.

### `python -m dlt_data_pipeline pipelines delete <name>`

Tear down: drops CDC replication slot + publication (pg_cdc only), drops
the destination dataset (unless `--keep-data`), wipes local
`.dlt/pipelines/<name>/`, and unlinks the YAML. Without `--yes` prints
the dry-run plan and exits 0 (CI-safe). With `--yes` runs idempotently —
each step captures its own error and the full report prints at the end.
Flags: `--yes`, `--keep-data`, `--env`.

### `python -m dlt_data_pipeline pipelines promote <name>`

Diff the merged config across two envs for one pipeline (Segment 13).
Required: `--from <env>`, `--to <env>`. Informational only — never edits
files. Exits 0 if either env has no changes, or with a summary.

## Known issues / troubleshooting

- **Backfill resume granularity:** `run-backfill` resumes at chunk
  boundaries, not row boundaries. A run that died mid-chunk re-processes
  the entire chunk on resume — pick `chunk_size` so the worst-case
  re-process fits the rollback window.
- **`pipelines delete` is not transactional:** the four teardown steps
  run independently and each captures its own error. A partial-failure
  end-state needs manual reconciliation (typically: slot still present on
  source, dataset already dropped on destination). Re-invoke with `--yes`
  to retry — the remaining steps are idempotent.
- **`pipelines promote` is diff-only:** it does NOT write to
  `pipelines/_env/<to>.yml`. The operator "applies" by setting `DLT_ENV`
  in the target deploy. Overlay file authoring stays manual.
- **`pipelines doctor` does not call the Airflow Secrets Backend:** when
  `AIRFLOW__SECRETS__BACKEND` is set, doctor reports `airflow-backend`
  for that slot but doesn't probe the backend — keeps the CLI free of an
  Airflow runtime dependency. Runtime resolution still goes through the
  backend.
