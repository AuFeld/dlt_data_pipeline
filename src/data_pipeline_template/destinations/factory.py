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

from data_pipeline_template.config.models import DestinationConfig, DestinationType

# Anchor duckdb files on the repo root by default (stable across CLI runs,
# Airflow tasks, and restarts). Under Airflow, ``PipelineTasksGroup`` switches
# CWD/DLT_DATA_DIR to a per-task temp dir, so a relative path would resolve
# under that temp dir (which gets wiped). Tests override via the env var.
_DUCKDB_DIR_ENV = "DATA_PIPELINE_DUCKDB_DIR"
_DEFAULT_DUCKDB_DIR = Path(__file__).resolve().parents[3] / ".dlt"


def _duckdb_dir() -> Path:
    override = os.environ.get(_DUCKDB_DIR_ENV)
    return Path(override) if override else _DEFAULT_DUCKDB_DIR


def _resolve_duckdb_path(connection: str) -> str:
    target = _duckdb_dir()
    target.mkdir(parents=True, exist_ok=True)
    return str(target / f"{connection}.duckdb")


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
    if cfg.type in (DestinationType.snowflake, DestinationType.databricks):
        raise NotImplementedError(f"destination type {cfg.type.value!r} lands in Segment 8")
    raise ValueError(f"unhandled destination type: {cfg.type!r}")
