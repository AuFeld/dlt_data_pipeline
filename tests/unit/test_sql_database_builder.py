"""Unit tests for the sql_database source builder.

Hermetic: uses a temp-file SQLite database as the credential target, so the
builder can construct a real dlt source without any external service.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from dlt.extract import DltSource

from dlt_data_pipeline.config.models import (
    SourceRestApi,
    SourceSqlDatabase,
)
from dlt_data_pipeline.sources.sql_database import builder

CREDS_ENV = "SOURCES__SQL_DATABASE__SQLITE_TEST__CREDENTIALS"


def _seed(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, updated_at TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, updated_at TIMESTAMP)"
        )
        conn.commit()


def _set_creds(monkeypatch: pytest.MonkeyPatch, db_path: Path) -> None:
    monkeypatch.setenv(CREDS_ENV, f"sqlite:///{db_path}")


def test_rejects_wrong_config_type() -> None:
    cfg = SourceRestApi(
        type="rest_api",
        connection="x",
        config={"base_url": "https://example.com/", "endpoints": ["e"]},
    )
    with pytest.raises(TypeError, match="sql_database builder received wrong config type"):
        builder(cfg)


def test_missing_tables_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db = tmp_path / "src.db"
    _seed(db)
    _set_creds(monkeypatch, db)
    cfg = SourceSqlDatabase(type="sql_database", connection="sqlite_test", config={})
    with pytest.raises(ValueError, match="source.config.tables is required"):
        builder(cfg)


def test_empty_tables_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db = tmp_path / "src.db"
    _seed(db)
    _set_creds(monkeypatch, db)
    cfg = SourceSqlDatabase(type="sql_database", connection="sqlite_test", config={"tables": []})
    with pytest.raises(ValueError, match="source.config.tables is required"):
        builder(cfg)


def test_unknown_config_key_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db = tmp_path / "src.db"
    _seed(db)
    _set_creds(monkeypatch, db)
    cfg = SourceSqlDatabase(
        type="sql_database",
        connection="sqlite_test",
        config={"tables": ["orders"], "bogus": 1},
    )
    with pytest.raises(ValueError, match="unknown config keys"):
        builder(cfg)


def test_missing_credentials_names_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CREDS_ENV, raising=False)
    cfg = SourceSqlDatabase(
        type="sql_database", connection="sqlite_test", config={"tables": ["orders"]}
    )
    with pytest.raises(ValueError, match=CREDS_ENV):
        builder(cfg)


def test_happy_path_returns_dlt_source_with_table_resources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db = tmp_path / "src.db"
    _seed(db)
    _set_creds(monkeypatch, db)
    cfg = SourceSqlDatabase(
        type="sql_database",
        connection="sqlite_test",
        config={"tables": ["orders", "customers"]},
    )
    source = builder(cfg)
    assert isinstance(source, DltSource)
    assert set(source.resources.keys()) == {"orders", "customers"}
    assert source.name == "sqlite_test"


def test_defer_table_reflect_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db = tmp_path / "src.db"
    _seed(db)
    _set_creds(monkeypatch, db)
    cfg = SourceSqlDatabase(
        type="sql_database",
        connection="sqlite_test",
        config={"tables": ["orders"], "defer_table_reflect": False},
    )
    source = builder(cfg)
    assert isinstance(source, DltSource)
    assert "orders" in source.resources
