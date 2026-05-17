"""Live PG CDC ``pipelines delete`` end-to-end (Segment 12).

Gated by ``RUN_LIVE_PG_CDC=1`` — mirrors ``test_pg_cdc.py``'s opt-in.
Verifies the destructive steps that hermetic duckdb tests can't reach:

  * CDC replication slot drop on the source DB.
  * CDC publication drop on the source DB.
  * Destination dataset drop on the destination DB.
  * YAML unlinked + second invocation reports ``not-found`` (idempotent).

Required env (same as ``test_pg_cdc.py``):
  ``SOURCES__PG_CDC__PG_SOURCE__CREDENTIALS``  — postgres URL with REPLICATION
  ``DESTINATION__PG_WAREHOUSE__CREDENTIALS``   — postgres URL for destination

Uses the committed ``pipelines/example_pg_cdc_to_pg.yml`` so the slot
(``dlt_orders_slot``), publication (``dlt_orders_pub``), and dataset
(``raw_orders_cdc``) names match what production tooling actually creates.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import psycopg2
import pytest

from dlt_data_pipeline import pipeline_factory
from dlt_data_pipeline.cli import delete_cmds

REPO_ROOT = Path(__file__).resolve().parents[2]
LIVE_YAML = REPO_ROOT / "pipelines" / "example_pg_cdc_to_pg.yml"
PIPELINE_NAME = "example_pg_cdc_to_pg"
SLOT_NAME = "dlt_orders_slot"
PUB_NAME = "dlt_orders_pub"
DEST_SCHEMA = "raw_orders_cdc"

SOURCE_ENV = "SOURCES__PG_CDC__PG_SOURCE__CREDENTIALS"
DEST_ENV = "DESTINATION__PG_WAREHOUSE__CREDENTIALS"

pytestmark = [
    pytest.mark.cdc,
    pytest.mark.postgres,
    pytest.mark.skipif(
        os.getenv("RUN_LIVE_PG_CDC") != "1",
        reason=(
            "set RUN_LIVE_PG_CDC=1 and bring up docker/docker-compose.yml "
            f"(needs {SOURCE_ENV} and {DEST_ENV})"
        ),
    ),
]


def _connect_source() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ[SOURCE_ENV])


def _connect_dest() -> psycopg2.extensions.connection:
    return psycopg2.connect(os.environ[DEST_ENV])


def _slot_exists() -> bool:
    with _connect_source() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;",
            (SLOT_NAME,),
        )
        return cur.fetchone() is not None


def _publication_exists() -> bool:
    with _connect_source() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s;",
            (PUB_NAME,),
        )
        return cur.fetchone() is not None


def _dataset_exists() -> bool:
    with _connect_dest() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s;",
            (DEST_SCHEMA,),
        )
        return cur.fetchone() is not None


def _reset_source_seed() -> None:
    """Drop any prior slot/pub/table then create a fresh orders table.

    Mirrors ``test_pg_cdc.py::_reset_source`` so this test starts from the
    same clean state regardless of run order.
    """
    with _connect_source() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s;",
                (SLOT_NAME,),
            )
            cur.execute(f"DROP PUBLICATION IF EXISTS {PUB_NAME};")
            cur.execute("DROP TABLE IF EXISTS public.orders CASCADE;")
            cur.execute(
                "CREATE TABLE public.orders ("
                "  id INTEGER PRIMARY KEY,"
                "  amount NUMERIC(12, 2) NOT NULL,"
                "  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
                ");"
            )
            cur.execute("ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;")
            cur.execute("INSERT INTO public.orders (id, amount) VALUES (1, 10.0), (2, 20.0);")


def _reset_destination_schema() -> None:
    with _connect_dest() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {DEST_SCHEMA} CASCADE;")


def test_delete_cleans_slot_publication_and_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _reset_source_seed()
    _reset_destination_schema()

    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    shutil.copy(LIVE_YAML, pipelines_dir / LIVE_YAML.name)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    monkeypatch.chdir(tmp_path)

    # Run the pipeline once so init_replication creates slot + publication,
    # then snapshot lands the seed rows into the destination dataset.
    pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)

    # Sanity-check the preconditions the delete should tear down.
    assert _slot_exists(), "slot must exist before delete"
    assert _publication_exists(), "publication must exist before delete"
    assert _dataset_exists(), "destination dataset must exist before delete"

    report = delete_cmds.delete_pipeline(PIPELINE_NAME, pipelines_dir)
    assert report["status"] == "ok", report

    assert not _slot_exists(), "slot should be dropped"
    assert not _publication_exists(), "publication should be dropped"
    assert not _dataset_exists(), "destination dataset should be dropped"
    assert report["yaml_removed"] is True
    assert not (pipelines_dir / LIVE_YAML.name).exists()


def test_delete_idempotent_second_call_is_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _reset_source_seed()
    _reset_destination_schema()

    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    shutil.copy(LIVE_YAML, pipelines_dir / LIVE_YAML.name)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    monkeypatch.chdir(tmp_path)

    pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)
    first = delete_cmds.delete_pipeline(PIPELINE_NAME, pipelines_dir)
    assert first["status"] == "ok"

    # Second call: YAML already unlinked, loader doesn't see the name.
    second = delete_cmds.delete_pipeline(PIPELINE_NAME, pipelines_dir)
    assert second["status"] == "not-found"


def test_delete_keep_data_preserves_dataset_drops_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--keep-data`` skips the destination drop but still tears down the
    source-side slot + publication (the source DB risk is paused-pipeline
    WAL retention, not destination data)."""
    _reset_source_seed()
    _reset_destination_schema()

    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    shutil.copy(LIVE_YAML, pipelines_dir / LIVE_YAML.name)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    monkeypatch.chdir(tmp_path)

    pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)
    report = delete_cmds.delete_pipeline(PIPELINE_NAME, pipelines_dir, keep_data=True)
    assert report["status"] == "ok"

    assert not _slot_exists(), "slot should be dropped even with --keep-data"
    assert not _publication_exists(), "publication should be dropped even with --keep-data"
    assert _dataset_exists(), "destination dataset must survive --keep-data"
