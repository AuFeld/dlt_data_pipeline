# Vendored: dlt-hub/verified-sources `pg_replication`

- **Upstream:** https://github.com/dlt-hub/verified-sources
- **Path:** `sources/pg_replication/`
- **Pinned SHA:** `75b3ec17eab99d0079d9f61b7f47fc8b899a5738`
- **License:** Apache-2.0 (see [`LICENSE`](LICENSE))
- **Vendored on:** 2026-05-16

## Why vendored

The `pg_replication` verified source is not published to PyPI. The only
alternatives are `pip install git+https://github.com/dlt-hub/verified-sources@<sha>#subdirectory=sources/pg_replication`
(breaks offline / mirrored installs and pins through a resolver) or this
checkout. Vendoring gives a stable pin, a patch surface, and zero new top-level
dependencies (`dlt[postgres]` already pulls `psycopg2-binary`).

## Layout

| File | Notes |
| --- | --- |
| [`__init__.py`](__init__.py) | Project-owned shim re-exporting `init_replication` + `replication_resource`. |
| [`resource.py`](resource.py) | Verbatim upstream `__init__.py` (renamed; defines `replication_resource`). |
| [`helpers.py`](helpers.py) | Verbatim upstream — defines `init_replication`, slot/publication management, `ItemGenerator`. |
| [`schema_types.py`](schema_types.py) | Verbatim upstream — Postgres type → dlt type mapping. |
| [`decoders.py`](decoders.py) | Verbatim upstream — pgoutput message decoder (vendored further from `pypgoutput`). |
| [`exceptions.py`](exceptions.py) | Verbatim upstream — `IncompatiblePostgresVersionException` + `NoPrimaryKeyException`. |
| [`LICENSE`](LICENSE) | Verbatim upstream Apache-2.0 license. |

The only project-authored file is `__init__.py`. `resource.py` is upstream
`sources/pg_replication/__init__.py` renamed to avoid colliding with this
shim — relative imports inside it (`from .helpers`, `from .decoders`, etc.)
still resolve correctly because the directory layout is preserved.

## Upgrading

1. Pick a new upstream SHA.
2. `gh api repos/dlt-hub/verified-sources/contents/sources/pg_replication/<file>?ref=<SHA> --jq '.content' | base64 -d > <file>` for each of `__init__.py` (→ `resource.py`), `helpers.py`, `schema_types.py`, `decoders.py`, `exceptions.py`. Refresh `LICENSE` too.
3. Update the pinned SHA in this file and in `__init__.py` docstring.
4. Run `pytest tests/unit/test_pg_cdc_builder.py tests/integration/test_pg_cdc.py` (the integration test needs `RUN_LIVE_PG_CDC=1` + a running compose stack).
