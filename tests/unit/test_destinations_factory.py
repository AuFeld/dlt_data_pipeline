"""Destination factory tests — type dispatch + duckdb path resolution."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from dlt.common.configuration import resolve_configuration

from data_pipeline_template.config.models import DestinationConfig, DestinationType
from data_pipeline_template.destinations.factory import build_destination


def test_duckdb_returns_dlt_destination(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    cfg = DestinationConfig(type=DestinationType.duckdb, connection="local_duckdb", dataset="raw")
    dest = build_destination(cfg)
    assert dest is not None
    assert os.path.isdir(tmp_path / ".dlt")


def test_postgres_returns_dlt_destination() -> None:
    cfg = DestinationConfig(type=DestinationType.postgres, connection="warehouse", dataset="raw")
    dest = build_destination(cfg)
    assert dest is not None


def test_snowflake_returns_dlt_destination() -> None:
    cfg = DestinationConfig(
        type=DestinationType.snowflake, connection="snowflake_warehouse", dataset="raw"
    )
    dest = build_destination(cfg)
    assert dest is not None
    assert dest.destination_name == "snowflake_warehouse"


def test_databricks_raises_deferral_message() -> None:
    cfg = DestinationConfig(type=DestinationType.databricks, connection="dbx", dataset="raw")
    with pytest.raises(NotImplementedError, match="not in active use"):
        build_destination(cfg)


def test_postgres_connection_name_scoping_overrides_type_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirms DESTINATION__<CONNECTION>__CREDENTIALS wins over the type-scoped
    DESTINATION__POSTGRES__CREDENTIALS when the destination is constructed with
    ``destination_name=<connection>``. Locks the convention documented in
    ``.env.example``.
    """
    monkeypatch.setenv(
        "DESTINATION__POSTGRES__CREDENTIALS",
        "postgresql://default:pw@hostdefault:5432/d",
    )
    monkeypatch.setenv(
        "DESTINATION__PG_WAREHOUSE__CREDENTIALS",
        "postgresql://scoped:pw@hostscoped:5432/mydb",
    )
    cfg = DestinationConfig(type=DestinationType.postgres, connection="pg_warehouse", dataset="raw")
    dest = build_destination(cfg)
    spec = dest.spec()
    spec.dataset_name = "raw"
    resolved = resolve_configuration(spec, sections=("destination", "pg_warehouse"))
    assert resolved.credentials.host == "hostscoped"
    assert resolved.credentials.username == "scoped"
    assert resolved.credentials.database == "mydb"
