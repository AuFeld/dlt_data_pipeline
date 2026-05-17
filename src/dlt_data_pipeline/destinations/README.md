# `destinations/` — destination factory + metadata

Destinations are **enum-dispatched**, not plugin-pluggable like sources —
the set is closed at the codebase boundary. Adding a destination type
means editing `DestinationType` in [`config/models.py`](../config/models.py)
+ [`factory.py`](factory.py) + [`_metadata.py`](_metadata.py).

## Files

- [`factory.py`](factory.py) — `build(destination)` returns a configured
  dlt destination. Wraps `destination_name=connection` so dlt resolves
  credentials under `destination.<connection>` exclusively (no type
  segment — see "Env-var convention" below).
- [`_metadata.py`](_metadata.py) — per-type `DestinationTypeMetadata`
  (description, env-var template, notes). Single source of truth for
  `destinations describe` flows.
- [`registry.py`](registry.py) — thin facade over `_metadata.METADATA`
  mirroring the `sources.registry` API (`list_types`, `describe`,
  `resolve_env_var`). Powers the scaffolder + MCP server.

## Env-var convention

```
DESTINATION__<CONNECTION_UPPER>__CREDENTIALS
```

No type segment. Logical connection name is the only differentiator —
two pipelines sharing `connection: pg_warehouse` resolve the same
credentials. Fallback: `[destination.<connection>.credentials]` in
[`.dlt/secrets.toml`](../../../.dlt/) for local dev. Full resolver
precedence in [`config/README.md`](../config/README.md#secrets-resolver-precedence).

## Per-type notes

### `duckdb`

Local file destination anchored under
`.dlt/<connection>.duckdb`. No credential env var. Override storage dir
via `DATA_PIPELINE_DUCKDB_DIR=...` for tests / non-default layouts.

### `postgres`

dlt's native postgres adapter. `DESTINATION__<CONNECTION>__CREDENTIALS`
takes any libpq connection URI. TOML fallback under
`[destination.<connection>.credentials]`.

### `snowflake`

Requires the optional `snowflake` extra:

```bash
uv sync --extra snowflake
```

Auth options (see "Snowflake creds" below for the canonical TOML key):

1. **Password URI** — env var value
   `snowflake://<user>:<password>@<account>/<database>?warehouse=<wh>&role=<role>`.
2. **Key-pair** — set `credentials.private_key` (base64-encoded PEM) and
   `credentials.private_key_passphrase` under
   `[destination.<connection>.credentials]` in `.dlt/secrets.toml`.
   Account / user / database / warehouse / role go in the same section.

Staging: dlt creates an internal named stage by default — no external
S3/ADLS staging credentials needed for v1.

### `databricks` — deferred

Not in active use. Current architecture: dlt loads Snowflake; Databricks
consumes from Snowflake via its External Data connection. Any pipeline
with `destination.type: databricks` fails at `build()` with a deferral
message. The enum entry + metadata stub stay so YAML schema validation
still accepts the type — re-evaluate if a direct dlt → Databricks load
becomes a requirement.

## Known issues / troubleshooting

> **Snowflake creds:** key-pair auth is multi-line — base64-encode the PEM
> and set `credentials.private_key` (plus `credentials.private_key_passphrase`
> if encrypted) under `[destination.<connection>.credentials]` in
> `.dlt/secrets.toml`. Account / user / database / warehouse / role go in
> the same TOML section. Password URI for env-var setups —
> `DESTINATION__<CONNECTION>__CREDENTIALS=snowflake://user:pw@account/db?warehouse=wh&role=role`.
> Keep all out of YAML. Segment 10.5 dropped the type segment from the
> destination resolver path, so the legacy
> `[destination.snowflake.<connection>.credentials]` key no longer
> resolves — use `[destination.<connection>.credentials]`. (Databricks
> deferred — see Segment 8 scope note.)
