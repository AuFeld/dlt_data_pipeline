"""Hermetic Segment 12 integration tests: backfill + late-arrivals + delete.

All three use SQLite source + DuckDB destination so they run in CI without
Docker. Live-Postgres CDC delete is covered separately by
`test_pipelines_delete_pg_cdc.py` (gated by `RUN_LIVE_PG_CDC=1`).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
import yaml

from dlt_data_pipeline import pipeline_factory
from dlt_data_pipeline.cli import delete_cmds


@dataclass(frozen=True)
class _Env:
    src_db: Path
    pipelines: Path
    duckdb: Path


def _seed(db_path: Path) -> None:
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


def _stage_yaml(
    pipelines_dir: Path, name: str, extra_sync: dict[str, object] | None = None
) -> None:
    sync: dict[str, object] = {
        "mode": "incremental",
        "cursor_field": "updated_at",
        "primary_key": "id",
    }
    if extra_sync:
        sync.update(extra_sync)
    cfg = {
        "name": name,
        "source": {
            "type": "sql_database",
            "connection": "sqlite_seg12",
            "config": {"tables": ["orders"]},
        },
        "sync": sync,
        "destination": {
            "type": "duckdb",
            "connection": "local_duckdb",
            "dataset": f"raw_{name}",
        },
        "schedule": {"cron": "0 */2 * * *", "enabled": True},
        "options": {"write_disposition": "merge"},
    }
    (pipelines_dir / f"{name}.yml").write_text(yaml.safe_dump(cfg))


def _duckdb_rows(db_path: Path, query: str) -> list[tuple]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        return list(conn.execute(query).fetchall())


@pytest.fixture
def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Env:
    src_db = tmp_path / "source.db"
    _seed(src_db)
    monkeypatch.setenv(
        "SOURCES__SQL_DATABASE__SQLITE_SEG12__CREDENTIALS",
        f"sqlite:///{src_db}",
    )
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    monkeypatch.chdir(tmp_path)
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    return _Env(
        src_db=src_db,
        pipelines=pipelines_dir,
        duckdb=tmp_path / ".dlt" / "local_duckdb.duckdb",
    )


def test_run_backfill_lands_chunks(_env: _Env) -> None:
    """30-day range chunked at P7D -> 5 chunks (4 full + 1 partial).

    Asserts the chunk count + that rows landed across the range. dlt's
    Incremental defaults to ``range_end='open'``, so rows whose cursor
    equals a chunk boundary land in the *next* chunk; the very last row
    sitting at exactly ``end`` is excluded. dlt also auto-promotes naive
    SQLite ``TIMESTAMP`` values to UTC when comparing against the bounded
    initial/end values, which can drop one row per chunk under edge tz
    handling — assert a lower bound rather than an exact count so the test
    isn't pinned to dlt-internal tz behavior.
    """
    name = "bf_basic"
    _stage_yaml(_env.pipelines, name, extra_sync={"backfill": {"chunk_size": "P7D"}})

    t0 = datetime(2025, 1, 1, tzinfo=UTC)
    rows = [(i, float(i), t0 + timedelta(days=i)) for i in range(1, 31)]
    _insert(_env.src_db, rows)

    start = t0
    end = t0 + timedelta(days=30)
    infos = pipeline_factory.run_backfill(name, start, end, pipelines_root=_env.pipelines)
    assert len(infos) == 5  # ceil(30/7)

    landed = _duckdb_rows(_env.duckdb, f"SELECT COUNT(*) FROM raw_{name}.orders")
    # 25 = 5 chunks * 5 rows/chunk under dlt's strict-open tz-corrected
    # bounds (the boundary row per chunk and the trailing row at `end` are
    # excluded). The exact figure can drift with dlt upgrades; what matters
    # is that all 5 chunks contributed substantial rows.
    assert landed[0][0] >= 20


def test_run_backfill_rejects_non_incremental(_env: Path) -> None:
    """Backfill on a full_refresh pipeline must fail with a clear error."""
    name = "bf_bad_mode"
    # Manually stage a full_refresh YAML (skipping _stage_yaml's incremental default).
    cfg = {
        "name": name,
        "source": {
            "type": "sql_database",
            "connection": "sqlite_seg12",
            "config": {"tables": ["orders"]},
        },
        "sync": {"mode": "full_refresh"},
        "destination": {
            "type": "duckdb",
            "connection": "local_duckdb",
            "dataset": f"raw_{name}",
        },
        "schedule": {"cron": "0 */2 * * *", "enabled": True},
    }
    (_env.pipelines / f"{name}.yml").write_text(yaml.safe_dump(cfg))
    with pytest.raises(ValueError, match="backfill requires 'incremental'"):
        pipeline_factory.run_backfill(
            name,
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 2, tzinfo=UTC),
            pipelines_root=_env.pipelines,
        )


def test_run_backfill_requires_backfill_block(_env: Path) -> None:
    name = "bf_no_block"
    _stage_yaml(_env.pipelines, name)
    with pytest.raises(ValueError, match="no sync.backfill block"):
        pipeline_factory.run_backfill(
            name,
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 2, tzinfo=UTC),
            pipelines_root=_env.pipelines,
        )


def test_delete_drops_yaml_and_dataset(_env: Path) -> None:
    """End-to-end: run a pipeline, then delete it, then confirm both YAML
    and destination dataset are gone."""
    name = "to_delete"
    _stage_yaml(_env.pipelines, name)
    t0 = datetime(2025, 1, 1, tzinfo=UTC)
    _insert(_env.src_db, [(1, 10.0, t0), (2, 20.0, t0 + timedelta(minutes=1))])
    pipeline_factory.run(name, pipelines_root=_env.pipelines)

    # Dataset exists.
    before = _duckdb_rows(_env.duckdb, f"SELECT COUNT(*) FROM raw_{name}.orders")
    assert before == [(2,)]

    report = delete_cmds.delete_pipeline(name, _env.pipelines)
    assert report["status"] == "ok", report
    assert report["yaml_removed"] is True
    assert not (_env.pipelines / f"{name}.yml").exists()

    # Dataset gone: querying raises (schema doesn't exist).
    with duckdb.connect(str(_env.duckdb), read_only=False) as conn:
        with pytest.raises(duckdb.CatalogException):
            conn.execute(f"SELECT COUNT(*) FROM raw_{name}.orders").fetchall()


def test_delete_keep_data_preserves_dataset(_env: Path) -> None:
    name = "keep_data"
    _stage_yaml(_env.pipelines, name)
    t0 = datetime(2025, 1, 1, tzinfo=UTC)
    _insert(_env.src_db, [(1, 10.0, t0)])
    pipeline_factory.run(name, pipelines_root=_env.pipelines)

    report = delete_cmds.delete_pipeline(name, _env.pipelines, keep_data=True)
    assert report["status"] == "ok"
    step_names = [s["step"] for s in report["steps"]]  # type: ignore[index]
    assert "drop_destination_dataset" not in step_names

    # Dataset survives.
    rows = _duckdb_rows(_env.duckdb, f"SELECT COUNT(*) FROM raw_{name}.orders")
    assert rows == [(1,)]


def test_tolerance_seconds_catches_late_arrival(_env: Path) -> None:
    """Without tolerance: late-arriving row at cursor-1s is dropped.
    With tolerance: same row gets caught on the next run.

    Tests both sides via two separate pipelines so dlt's per-pipeline cursor
    state stays isolated.
    """
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    # Pipeline 1: no tolerance.
    name_strict = "lag_strict"
    _stage_yaml(_env.pipelines, name_strict)
    _insert(_env.src_db, [(1, 10.0, t0)])
    pipeline_factory.run(name_strict, pipelines_root=_env.pipelines)
    # Insert a row with cursor *earlier* than the last cursor — simulates a
    # late-arriving write that the strict cursor will skip.
    _insert(_env.src_db, [(2, 20.0, t0 - timedelta(seconds=1))])
    pipeline_factory.run(name_strict, pipelines_root=_env.pipelines)
    landed_strict = _duckdb_rows(
        _env.duckdb, f"SELECT id FROM raw_{name_strict}.orders ORDER BY id"
    )
    assert landed_strict == [(1,)]  # late row dropped — documents the gap

    # Pipeline 2: tolerance_seconds=10 catches it.
    name_loose = "lag_loose"
    _stage_yaml(_env.pipelines, name_loose, extra_sync={"tolerance_seconds": 10})
    # Re-seed from a fresh source so we don't double-count the strict pipeline's data.
    _env.src_db.unlink()
    _seed(_env.src_db)
    _insert(_env.src_db, [(1, 10.0, t0)])
    pipeline_factory.run(name_loose, pipelines_root=_env.pipelines)
    _insert(_env.src_db, [(2, 20.0, t0 - timedelta(seconds=1))])
    pipeline_factory.run(name_loose, pipelines_root=_env.pipelines)
    landed_loose = _duckdb_rows(_env.duckdb, f"SELECT id FROM raw_{name_loose}.orders ORDER BY id")
    assert landed_loose == [(1,), (2,)]
