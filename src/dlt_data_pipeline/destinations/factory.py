"""Destination dispatcher.

YAML ``destination.type`` -> configured dlt destination object. Credentials
resolution is left to dlt's native machinery (env vars / ``.dlt/secrets.toml``
sections keyed by the logical ``connection`` name) so YAML never holds raw
credentials.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

from dlt.common.destination import Destination
from dlt.destinations import duckdb, postgres

from dlt_data_pipeline.config.models import DestinationConfig, DestinationType

# Anchor duckdb files on the repo root by default (stable across CLI runs,
# Airflow tasks, and restarts). Under Airflow, ``PipelineTasksGroup`` switches
# CWD/DLT_DATA_DIR to a per-task temp dir, so a relative path would resolve
# under that temp dir (which gets wiped). Tests override via the env var.
_DUCKDB_DIR_ENV = "DATA_PIPELINE_DUCKDB_DIR"
_DEFAULT_DUCKDB_DIR = Path(__file__).resolve().parents[3] / ".dlt"

_DATABRICKS_DEFERRED_MSG = (
    "destination type 'databricks' is not in active use: current architecture loads "
    "Snowflake and Databricks consumes from Snowflake via its External Data connection. "
    "Open an issue if a direct dlt -> Databricks load becomes a requirement."
)


def _duckdb_dir() -> Path:
    override = os.environ.get(_DUCKDB_DIR_ENV)
    return Path(override) if override else _DEFAULT_DUCKDB_DIR


def _resolve_duckdb_path(connection: str) -> str:
    target = _duckdb_dir()
    target.mkdir(parents=True, exist_ok=True)
    return str(target / f"{connection}.duckdb")


def _build_snowflake(connection: str) -> Destination[Any, Any]:
    try:
        from dlt.destinations import snowflake
    except ImportError as exc:
        raise RuntimeError(
            "Snowflake destination requires the optional 'snowflake' extra. "
            "Install with: uv sync --extra snowflake "
            "(or: pip install 'data-pipeline-template[snowflake]')."
        ) from exc
    return cast(
        Destination[Any, Any],
        snowflake(destination_name=connection),
    )


def build_destination(cfg: DestinationConfig) -> Destination[Any, Any]:
    if cfg.type == DestinationType.duckdb:
        path = _resolve_duckdb_path(cfg.connection)
        return cast(
            Destination[Any, Any],
            duckdb(credentials=path, destination_name=cfg.connection),
        )
    if cfg.type == DestinationType.postgres:
        return cast(
            Destination[Any, Any],
            postgres(destination_name=cfg.connection),
        )
    if cfg.type == DestinationType.snowflake:
        return _build_snowflake(cfg.connection)
    if cfg.type == DestinationType.databricks:
        raise NotImplementedError(_DATABRICKS_DEFERRED_MSG)
    raise ValueError(f"unhandled destination type: {cfg.type!r}")
