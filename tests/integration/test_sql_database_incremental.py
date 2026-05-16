"""End-to-end: sql_database (SQLite) -> duckdb via pipeline_factory.

Hermetic stand-in for the Segment-5 "Done when" criterion (PG->PG full
refresh then incremental, cursor persists across runs). Uses SQLite as the
source and DuckDB as the destination so the cursor + merge logic gets
exercised in CI without Docker. A separate gated test runs the same flow
against live Postgres when ``RUN_LIVE_PG=1`` is set, and Segment 6 flips
that on once docker-compose lands.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
import yaml

from data_pipeline_template import pipeline_factory

REPO_ROOT = Path(__file__).resolve().parents[2]
LIVE_PG_YAML = REPO_ROOT / "pipelines" / "example_pg_to_pg_incremental.yml"

PIPELINE_NAME = "test_sqlite_to_duckdb_incremental"
CONNECTION = "sqlite_int_test"
CREDS_ENV = f"SOURCES__SQL_DATABASE__{CONNECTION.upper()}__CREDENTIALS"


def _seed_source(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, updated_at TIMESTAMP)"
        )
        conn.commit()


def _insert(db_path: Path, rows: list[tuple[int, float, datetime]]) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO orders (id, amount, updated_at) VALUES (?, ?, ?)",
            [(r[0], r[1], r[2].isoformat(sep=" ")) for r in rows],
        )
        conn.commit()


def _update(db_path: Path, row_id: int, amount: float, ts: datetime) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE orders SET amount = ?, updated_at = ? WHERE id = ?",
            (amount, ts.isoformat(sep=" "), row_id),
        )
        conn.commit()


def _stage_yaml(pipelines_dir: Path, source_db: Path) -> None:
    cfg = {
        "name": PIPELINE_NAME,
        "source": {
            "type": "sql_database",
            "connection": CONNECTION,
            "config": {"tables": ["orders"]},
        },
        "sync": {
            "mode": "incremental",
            "cursor_field": "updated_at",
            "primary_key": "id",
        },
        "destination": {
            "type": "duckdb",
            "connection": "local_duckdb",
            "dataset": "raw_sql_int",
        },
        "schedule": {"cron": "0 */2 * * *", "enabled": True},
        "options": {"write_disposition": "merge"},
    }
    (pipelines_dir / f"{PIPELINE_NAME}.yml").write_text(yaml.safe_dump(cfg))


def _duckdb_select(db_path: Path, query: str) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        return list(conn.execute(query).fetchall())


def test_full_refresh_then_incremental_then_cursor_persists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_db = tmp_path / "source.db"
    _seed_source(src_db)
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    _stage_yaml(pipelines_dir, src_db)

    monkeypatch.setenv(CREDS_ENV, f"sqlite:///{src_db}")
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    monkeypatch.chdir(tmp_path)

    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    _insert(
        src_db,
        [
            (1, 10.0, t0),
            (2, 20.0, t0 + timedelta(minutes=1)),
            (3, 30.0, t0 + timedelta(minutes=2)),
        ],
    )

    # Phase 1 — initial load picks up all 3.
    pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)

    duckdb_path = tmp_path / ".dlt" / "local_duckdb.duckdb"
    assert duckdb_path.exists(), f"expected duckdb at {duckdb_path}"

    rows_after_1 = _duckdb_select(
        duckdb_path, "SELECT id, amount FROM raw_sql_int.orders ORDER BY id"
    )
    assert rows_after_1 == [(1, 10.0), (2, 20.0), (3, 30.0)]

    # Phase 2 — mutate (update row 2, insert row 4) with newer cursors.
    t1 = t0 + timedelta(hours=1)
    _update(src_db, row_id=2, amount=999.0, ts=t1)
    _insert(src_db, [(4, 40.0, t1 + timedelta(minutes=1))])

    pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)

    rows_after_2 = _duckdb_select(
        duckdb_path, "SELECT id, amount FROM raw_sql_int.orders ORDER BY id"
    )
    # merge dedupes the update, appends the insert -> 4 distinct rows.
    assert rows_after_2 == [(1, 10.0), (2, 999.0), (3, 30.0), (4, 40.0)]

    # Phase 3 — no source changes, cursor persisted, second run is a no-op.
    pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)

    rows_after_3 = _duckdb_select(
        duckdb_path, "SELECT id, amount FROM raw_sql_int.orders ORDER BY id"
    )
    assert rows_after_3 == rows_after_2


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_PG") != "1",
    reason="set RUN_LIVE_PG=1 and provide SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS "
    "+ DESTINATION__PG_WAREHOUSE__CREDENTIALS to exercise the real Postgres pipeline",
)
def test_live_pg_to_pg_incremental(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Drives the real ``example_pg_to_pg_incremental.yml`` end-to-end.

    Segment 6 will flip RUN_LIVE_PG=1 in CI once docker-compose provides the
    postgres-source / postgres-destination services and a seed script that
    creates the orders + customers tables with updated_at columns.
    """
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    target = pipelines_dir / LIVE_PG_YAML.name
    target.write_text(LIVE_PG_YAML.read_text())
    monkeypatch.chdir(tmp_path)
    pipeline_factory.run("example_pg_to_pg_incremental", pipelines_root=pipelines_dir)
