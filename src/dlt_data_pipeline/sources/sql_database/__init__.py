"""SQL database source builder.

Wraps ``dlt.sources.sql_database`` and maps the YAML ``source.config`` block
to its kwargs. Credentials resolve from
``SOURCES__SQL_DATABASE__<CONNECTION>__CREDENTIALS`` (env) or
``[sources.sql_database.<connection>.credentials]`` (``.dlt/secrets.toml``)
keyed by the YAML ``source.connection`` logical name.

``defer_table_reflect`` defaults to ``True`` so the source never opens a DB
connection at DAG-parse time. YAML can flip it to ``false`` for strict
reflected types. ``tables`` is required and non-empty — drives both the
per-resource Airflow task topology and the cursor contract (every listed
table must expose the column named in ``sync.cursor_field``).
"""

from __future__ import annotations

from typing import Any

import dlt
from dlt.extract import DltSource
from dlt.sources.sql_database import sql_database

from dlt_data_pipeline.config.models import SourceConfig, SourceSqlDatabase

_ALLOWED_KEYS: frozenset[str] = frozenset(
    {
        "schema",
        "tables",
        "backend",
        "chunk_size",
        "include_views",
        "reflection_level",
        "defer_table_reflect",
    }
)


def _resolve_credentials(connection_name: str) -> str:
    key = f"sources.sql_database.{connection_name}.credentials"
    try:
        value = dlt.secrets[key]
    except Exception as exc:
        env_var = f"SOURCES__SQL_DATABASE__{connection_name.upper()}__CREDENTIALS"
        raise ValueError(
            f"sql_database source {connection_name!r}: missing credentials. "
            f"Set env var {env_var} or add [{key}] to .dlt/secrets.toml"
        ) from exc
    if value is None:
        env_var = f"SOURCES__SQL_DATABASE__{connection_name.upper()}__CREDENTIALS"
        raise ValueError(
            f"sql_database source {connection_name!r}: missing credentials. "
            f"Set env var {env_var} or add [{key}] to .dlt/secrets.toml"
        )
    return str(value)


def _to_dlt_kwargs(name: str, user_cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(user_cfg)
    unknown = set(cfg) - _ALLOWED_KEYS
    if unknown:
        raise ValueError(
            f"sql_database source {name!r}: unknown config keys {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_KEYS)}"
        )
    tables = cfg.pop("tables", None)
    if not tables:
        raise ValueError(
            f"sql_database source {name!r}: source.config.tables is required and non-empty"
        )
    kwargs: dict[str, Any] = {
        "table_names": list(tables),
        "defer_table_reflect": cfg.pop("defer_table_reflect", True),
    }
    if "schema" in cfg:
        kwargs["schema"] = cfg.pop("schema")
    kwargs.update(cfg)
    return kwargs


def builder(config: SourceConfig) -> DltSource:
    if not isinstance(config, SourceSqlDatabase):
        raise TypeError(f"sql_database builder received wrong config type: {type(config).__name__}")
    kwargs = _to_dlt_kwargs(config.connection, config.config)
    credentials = _resolve_credentials(config.connection)
    factory = sql_database.with_args(name=config.connection)
    return factory(credentials=credentials, **kwargs)
