"""Destination factory tests — type dispatch + duckdb path resolution."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

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


def test_snowflake_raises_not_implemented() -> None:
    cfg = DestinationConfig(type=DestinationType.snowflake, connection="sf", dataset="raw")
    with pytest.raises(NotImplementedError, match="Segment 8"):
        build_destination(cfg)


def test_databricks_raises_not_implemented() -> None:
    cfg = DestinationConfig(type=DestinationType.databricks, connection="dbx", dataset="raw")
    with pytest.raises(NotImplementedError, match="Segment 8"):
        build_destination(cfg)
