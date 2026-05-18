# Demo: `dlt_data_pipeline` end-to-end

A 15-minute runbook for live-demoing the project. Every step has two flavours:

- **Human** — shell command, copy-paste into a terminal at the repo root.
- **Agent** — natural-language prompt for an MCP-aware assistant (Claude Code, Cursor, etc.) or the explicit MCP tool call. The project's MCP server is registered in [`.mcp.json`](.mcp.json) and exposes five tools under `mcp__dlt_data_pipeline__*`.

Pick one flavour per step, or alternate to showcase parity.

## Prereqs

- Docker Desktop running.
- `uv` installed on the host — Human CLI commands run on the host via `uv run` (faster than `docker compose exec`, and uses the same code path as the MCP server). The container is only required for the Airflow UI (Step 8) and the pg pipelines in Step 9 (need container DNS to `postgres-source` / `postgres-destination`).
- Repo cloned and `cd`-ed into.
- Port `8080` free on the host. Check with `lsof -i :8080` — output must be empty, else Airflow webserver in Step 1 fails to bind.
- For Agent steps: an MCP-aware client connected to this repo (Claude Code auto-loads `.mcp.json`).
- **Rebuild the image if `pyproject.toml` changed since the last build** — the local image bakes deps at build time, so a stale image silently misses any newly added package:

    ```bash
    docker compose -f docker/docker-compose.yml build airflow-scheduler
    ```

Full architecture + design context: [`README.md`](README.md), [`AGENTS.md`](AGENTS.md), [`data_pipeline_plan.md`](data_pipeline_plan.md).

---

## 1. Bring up the local stack

Spins up `postgres-airflow`, `postgres-source`, `postgres-destination`, and three Airflow services from one shared image.

### Human

```bash
docker compose -f docker/docker-compose.yml up -d
bash scripts/seed_local.sh
```

Then wait for the Airflow scheduler to become healthy (scheduler init can take 60–120s; exec calls hang until it's up):

```bash
until docker compose -f docker/docker-compose.yml ps airflow-scheduler \
    --format '{{.Status}}' | grep -q healthy; do sleep 3; done
```

Open the Airflow UI at http://localhost:8080 (admin / admin). DagBag re-parse takes ~30s after that; example DAGs appear automatically.

### Agent

No equivalent — infra step. The agent can verify health afterwards: *"Run `docker compose -f docker/docker-compose.yml ps` and confirm all services report `healthy` or `running`."*

---

## 2. Tour the source catalog

Show what source types ship out-of-box. Plugin-discovered via entry points — no hardcoded list.

### Human

```bash
uv run python -m dlt_data_pipeline sources list
uv run python -m dlt_data_pipeline sources describe rest_api
```

`describe` prints the env-var template (e.g. `SOURCES__REST_API__<CONN>__CREDENTIALS`), required + allowed `source.config` keys, and source-specific notes.

### Agent

Call:

- `mcp__dlt_data_pipeline__sources_list` (no args)
- `mcp__dlt_data_pipeline__sources_describe` with `source_type="rest_api"`

Or prompt: *"List the registered source types via the dlt_data_pipeline MCP server, then describe the `rest_api` source."*

---

## 3. Tour the example pipelines

Six bundled examples cover every sync mode + a representative source-destination matrix.

### Human

```bash
ls pipelines/*.yml
```

| File | Source → destination | Demonstrates |
|---|---|---|
| `example_rest_to_duckdb.yml` | REST (PokeAPI) → DuckDB | Full-refresh, no credentials required |
| `example_pg_to_pg_incremental.yml` | Postgres → Postgres | Incremental sync with cursor + primary key |
| `example_pg_to_pg_quality.yml` | Postgres → Postgres | Incremental + row-count reconciliation |
| `example_pg_cdc_to_pg.yml` | Postgres CDC → Postgres | Logical-replication slot, merge disposition |
| `example_pg_cdc_to_snowflake.yml` | Postgres CDC → Snowflake | CDC into a cloud warehouse (creds required) |
| `example_filesystem_to_duckdb.yml` | Local CSV → DuckDB | `filesystem` source with `file://` paths |

### Agent

*"List the YAML files under `pipelines/` and summarise each one's source type, destination type, and sync mode in a single table."*

---

## 4. Validate config (no DB connection)

Sub-second feedback. Parses every YAML against the pydantic schema.

### Human

```bash
uv run python -m dlt_data_pipeline pipelines validate
```

Expected: exit 0, one `OK: <name>` line per pipeline. Exit 1 with aggregated errors otherwise.

### Agent

Call `mcp__dlt_data_pipeline__pipelines_validate` with no arguments. Or prompt: *"Validate every pipeline in this repo and report any errors."*

---

## 5. Doctor credentials

Probes `os.environ` and `dlt.secrets` for every source + destination slot.

### Human

```bash
uv run python -m dlt_data_pipeline pipelines doctor
```

Expected on a fresh host (no Postgres / Snowflake creds exported to your shell):

- `example_rest_to_duckdb`, `example_filesystem_to_duckdb` → `no-creds-required` ✓
- `example_pg_to_pg_*`, `example_pg_cdc_to_pg`, `example_pg_cdc_to_snowflake` → `MISSING`

Doctor exits **1** when any slot is `MISSING`. That's the demo point: it caught the gap. The Postgres pipelines work inside the container because the compose `.env` file injects `SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS` + `DESTINATION__PG_WAREHOUSE__CREDENTIALS` there. To make doctor green on the host too, `source .env` first (or export those two vars manually).

### Agent

Call `mcp__dlt_data_pipeline__pipelines_doctor`. The MCP server runs on the host, so it reports the same `MISSING` slots — same caveat applies. Prompt: *"Run pipelines doctor and tell me which pipelines have missing credentials."*

---

## 6. Dry-run a pipeline

Extract + normalize, **skip** destination write. Fastest possible smoke test of source connectivity and schema inference.

### Human

```bash
uv run python -m dlt_data_pipeline run example_rest_to_duckdb --limit 1 --no-load
```

`--limit N` caps **page yields**, not records — for the REST source each page = 20 records, so `--limit 1` produces 20 rows in the normalize summary. `--no-load` runs `pipeline.extract()` + `pipeline.normalize()` but skips `pipeline.load()`.

### Agent

*"Run the `example_rest_to_duckdb` pipeline with `--limit 1 --no-load` and report the row counts per resource from the output."*

---

## 7. Real load to DuckDB

Drop `--no-load`. Writes to the `raw_demo` dataset inside DuckDB.

### Human

```bash
uv run python -m dlt_data_pipeline run example_rest_to_duckdb --limit 5
```

dlt prints a load summary at the end. The destination is `raw_demo.pokemon` inside a DuckDB file at `.dlt/local_duckdb.duckdb` (relative to repo root, because the CLI runs on the host). Inspect with:

```bash
uv run python -c "import duckdb; print(duckdb.connect('.dlt/local_duckdb.duckdb').sql('select count(*) from raw_demo.pokemon').fetchall())"
```

### Agent

*"Run `example_rest_to_duckdb` with `--limit 5`, then print the dlt load summary so I can show row counts to the audience."*

---

## 8. Walk the Airflow UI

Pure visual step.

1. http://localhost:8080 → log in (admin / admin).
2. Find the `example_rest_to_duckdb` DAG. Toggle it on.
3. Click **Trigger DAG** → watch the task graph turn green.
4. Click into a task → **Logs** tab → show the dlt extract/normalize/load output streaming live.

Call out: the DAG is generated from the YAML by `dags/data_pipeline_dags.py` at DagBag parse time — no per-pipeline Python.

No agent equivalent.

---

## 9. Incremental load + backfill

Pick `example_pg_to_pg_incremental`. The seeded `orders` table on `postgres-source` already has rows from `seed_local.sh`.

This step **must** run inside the container — the pipeline reaches `postgres-source` / `postgres-destination` over compose DNS, which the host shell can't resolve. The scheduler container must be `healthy` (per Step 1's readiness check) and the image must include current deps (per Prereqs rebuild).

### Human

```bash
# Initial incremental run — picks up everything new since the last cursor.
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
    python -m dlt_data_pipeline run example_pg_to_pg_incremental

# Chunked historical backfill over a fixed window.
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
    python -m dlt_data_pipeline run-backfill example_pg_to_pg_incremental \
    --start 2025-01-01T00:00:00Z \
    --end 2025-02-01T00:00:00Z
```

`run-backfill` drives chunked, resumable `pipeline.run()` calls over `[start, end)`. Chunk size + partition field come from `sync.backfill` in the YAML.

### Agent

*"Run `example_pg_to_pg_incremental` once for the live incremental, then run `run-backfill` over Jan 2025 and report the chunk count + total rows from the summary."*

---

## 10. Env overlay diff

Show how the same pipeline differs between `dev` and `prod` without ever editing the base YAML. Informational — never writes files.

### Human

```bash
uv run python -m dlt_data_pipeline pipelines promote example_rest_to_duckdb \
    --from dev --to prod
```

Output is a diff of the resolved config across the two environments (allowed scope: source connection, destination type/connection/dataset, schedule.enabled, resources). See `pipelines/_env/` for overlay files.

### Agent

Call `mcp__dlt_data_pipeline__pipelines_promote` with `name="example_rest_to_duckdb"`, `from_env="dev"`, `to_env="prod"`. Or prompt: *"Diff the dev vs prod resolved config for `example_rest_to_duckdb` using the MCP server."*

---

## 11. Teardown

> **Warning:** `docker compose down -v` deletes all named volumes — Airflow metadata DB, source Postgres data, destination Postgres data. Irreversible. Only run when the demo is over.

### Human

```bash
# Dry-run first (no --yes) to preview the plan.
uv run python -m dlt_data_pipeline pipelines delete example_rest_to_duckdb

# Drop the pipeline's destination dataset, local state, and YAML.
# Idempotent. Drops CDC slot + publication if present.
uv run python -m dlt_data_pipeline pipelines delete example_rest_to_duckdb --yes

# Tear down the whole stack and wipe all volumes.
docker compose -f docker/docker-compose.yml down -v
```

### Agent

No MCP tool wraps `pipelines delete`. Prompt the agent: *"Run `uv run python -m dlt_data_pipeline pipelines delete example_rest_to_duckdb --yes` and report each step's result."*

The `docker compose down -v` step is destructive — keep it human-driven and confirm before running.

---

## Going further

- **Add a new pipeline in ~10 min:** [`README.md#add-a-pipeline-in-10-minutes`](README.md).
- **CDC operations** (slot lifecycle, DDL caveats, lag sensor): [`AGENTS.md`](AGENTS.md) under "CDC operations".
- **Snowflake / Databricks targets:** set the destination creds env vars and re-run validate + doctor before any run. Templates in [`AGENTS.md`](AGENTS.md) under "Logical-connection-name → env-var convention".
- **Prod deployment** (KubernetesExecutor manifests): [`deploy/k8s/base/README.md`](deploy/k8s/base/README.md).
