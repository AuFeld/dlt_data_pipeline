from __future__ import annotations

from dlt_data_pipeline.sources._metadata import SourceTypeMetadata

# Mirrors _ALLOWED_KEYS in sources/sql_database/__init__.py; keep in sync.
_ALLOWED = (
    "schema",
    "tables",
    "backend",
    "chunk_size",
    "include_views",
    "reflection_level",
    "defer_table_reflect",
)

metadata = SourceTypeMetadata(
    description=(
        "SQL database source. Wraps dlt.sources.sql_database; accepts any "
        "SQLAlchemy URL via the resolved credential."
    ),
    env_var_template="SOURCES__SQL_DATABASE__<CONNECTION>__CREDENTIALS",
    allowed_config_keys=_ALLOWED,
    required_config_keys=("tables",),
    notes=(
        "Credential fallback: [sources.sql_database.<connection>.credentials] "
        "in .dlt/secrets.toml. Every table listed must expose the column "
        "named in sync.cursor_field for incremental mode."
    ),
)
