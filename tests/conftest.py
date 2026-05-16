"""Session-wide test fixtures.

Bootstraps source credentials for the ``example_pg_to_pg_incremental`` YAML
so the real ``dags/data_pipeline_dags.py`` DagBag scan in
``tests/unit/test_dagbag.py`` can build the DAG hermetically (no live
Postgres needed). Individual tests override via ``monkeypatch.setenv``.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

_PG_SOURCE_ENV = "SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS"


def _seed_sqlite(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "id INTEGER PRIMARY KEY, amount REAL, updated_at TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS customers ("
            "id INTEGER PRIMARY KEY, name TEXT, updated_at TIMESTAMP)"
        )
        conn.commit()


@pytest.fixture(scope="session", autouse=True)
def _stub_pg_source_credentials(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Provide a SQLite stand-in for the pg_source connection at session scope.

    Only injects when the env var is unset — CI / local with real Postgres
    will retain their own value. SQLite satisfies dlt's sql_database builder
    at DAG-parse time (it constructs an Engine but does not connect with
    ``defer_table_reflect=True``).
    """
    if os.environ.get(_PG_SOURCE_ENV):
        return
    db_path = tmp_path_factory.mktemp("conftest_pg_source") / "pg_source.db"
    _seed_sqlite(db_path)
    os.environ[_PG_SOURCE_ENV] = f"sqlite:///{db_path}"
