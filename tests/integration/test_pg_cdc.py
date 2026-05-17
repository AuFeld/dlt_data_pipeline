"""Live Postgres CDC end-to-end test for Segment 7.

Gated by ``RUN_LIVE_PG_CDC=1`` to keep CI green without a docker-compose
stack — same opt-in pattern as ``test_live_pg_to_pg_incremental`` (Segment
5). Drives the example_pg_cdc_to_pg pipeline against the compose
``postgres-source`` (must be running with ``wal_level=logical`` and the
``replicator`` role from ``docker/postgres-source-init/01_replication.sql``).

Required env (set when ``RUN_LIVE_PG_CDC=1``):
- ``SOURCES__PG_CDC__PG_SOURCE__CREDENTIALS``     postgres URL with REPLICATION
- ``DESTINATION__PG_WAREHOUSE__CREDENTIALS``      postgres URL for destination

What the test exercises:
1. Snapshot — first run picks up pre-existing rows.
2. INSERT propagation.
3. UPDATE propagation.
4. DELETE propagation (vendor emits a row with ``_dlt_deleted_ts`` set; merge
   disposition retains the row but flags it).
5. Slot persistence — issuing a restart between runs is out of scope for the
   in-process test (no docker control). Instead, this run does a second
   no-change run to assert the cursor (LSN) is persisted and the second run
   is a no-op. The full restart drill is documented in the seed_local.sh
   next-steps block for manual verification.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import psycopg2
import pytest

from data_pipeline_template import pipeline_factory

REPO_ROOT = Path(__file__).resolve().parents[2]
LIVE_YAML = REPO_ROOT / "pipelines" / "example_pg_cdc_to_pg.yml"

SOURCE_ENV = "SOURCES__PG_CDC__PG_SOURCE__CREDENTIALS"
DEST_ENV = "DESTINATION__PG_WAREHOUSE__CREDENTIALS"

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_LIVE_PG_CDC") != "1",
    reason=(
        "set RUN_LIVE_PG_CDC=1 and bring up docker/docker-compose.yml "
        f"(needs {SOURCE_ENV} and {DEST_ENV})"
    ),
)


def _connect_source() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ[SOURCE_ENV])


def _connect_dest() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ[DEST_ENV])


def _reset_source() -> None:
    """Drop the slot + publication + table so each test starts from empty."""
    with _connect_source() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = 'dlt_orders_slot';"
            )
            cur.execute("DROP PUBLICATION IF EXISTS dlt_orders_pub;")
            cur.execute("DROP TABLE IF EXISTS public.orders CASCADE;")
            cur.execute(
                "CREATE TABLE public.orders ("
                "  id INTEGER PRIMARY KEY,"
                "  amount NUMERIC(12, 2) NOT NULL,"
                "  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
                ");"
            )
            cur.execute("ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;")


def _reset_destination() -> None:
    with _connect_dest() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw_orders_cdc CASCADE;")


def _insert(rows: list[tuple[int, float]]) -> None:
    with _connect_source() as conn, conn.cursor() as cur:
        cur.executemany("INSERT INTO public.orders (id, amount) VALUES (%s, %s);", rows)
        conn.commit()


def _update(row_id: int, amount: float) -> None:
    with _connect_source() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE public.orders SET amount = %s, updated_at = NOW() WHERE id = %s;",
            (amount, row_id),
        )
        conn.commit()


def _delete(row_id: int) -> None:
    with _connect_source() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM public.orders WHERE id = %s;", (row_id,))
        conn.commit()


def _select_destination() -> dict[int, tuple[float, bool]]:
    """Return id -> (amount, is_deleted). Deletes carry _dlt_deleted_ts set."""
    with _connect_dest() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'raw_orders_cdc' AND table_name = 'orders';"
        )
        cols = {r[0] for r in cur.fetchall()}
        delete_col = "_dlt_deleted_ts" if "_dlt_deleted_ts" in cols else None
        select_delete = f", {delete_col}" if delete_col else ", NULL"
        cur.execute(f"SELECT id, amount{select_delete} FROM raw_orders_cdc.orders ORDER BY id;")
        rows = cur.fetchall()
    return {int(r[0]): (float(r[1]), r[2] is not None) for r in rows}


def test_pg_cdc_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Clean slate on both ends so re-runs are deterministic.
    _reset_source()
    _reset_destination()

    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    shutil.copy(LIVE_YAML, pipelines_dir / LIVE_YAML.name)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    monkeypatch.chdir(tmp_path)

    # Phase 1 — seed source then run; init_replication creates slot +
    # publication on this call. Snapshot resources land the pre-existing
    # rows in the destination.
    _insert([(1, 10.0), (2, 20.0)])
    pipeline_factory.run("example_pg_cdc_to_pg", pipelines_root=pipelines_dir)
    state = _select_destination()
    assert {1, 2} <= state.keys()
    assert state[1][0] == 10.0
    assert state[2][0] == 20.0

    # Phase 2 — INSERT + UPDATE + DELETE flow through the streaming resource.
    _insert([(3, 30.0)])
    _update(row_id=1, amount=111.0)
    _delete(row_id=2)
    pipeline_factory.run("example_pg_cdc_to_pg", pipelines_root=pipelines_dir)
    state = _select_destination()
    assert state[1][0] == 111.0, "UPDATE should propagate via merge"
    assert state[3][0] == 30.0, "INSERT should propagate"
    # DELETE: row 2 either drops out entirely (hard delete) or carries the
    # vendor's tombstone column. Accept either; the contract is "no live row".
    if 2 in state:
        assert state[2][1] is True, "row 2 should be flagged deleted"

    # Phase 3 — no source changes; cursor persists, second run is a no-op.
    before = _select_destination()
    pipeline_factory.run("example_pg_cdc_to_pg", pipelines_root=pipelines_dir)
    after = _select_destination()
    assert before == after, "second run with no source changes must be a no-op"
