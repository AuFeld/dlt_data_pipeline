"""End-to-end Snowflake destination smoke test.

Gated on ``SNOWFLAKE_TEST_ACCOUNT`` to keep CI green without a live Snowflake
account. When the env var is set, the test loads a tiny in-memory dataset
via the pipeline factory and asserts row counts via a SELECT against the
destination, then cleans up.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import yaml

from data_pipeline_template import pipeline_factory

pytestmark = [
    pytest.mark.snowflake,
    pytest.mark.skipif(
        not os.environ.get("SNOWFLAKE_TEST_ACCOUNT"),
        reason="Snowflake creds absent; set SNOWFLAKE_TEST_ACCOUNT (+ companion env vars) to run",
    ),
]

REPO_ROOT = Path(__file__).resolve().parents[2]


def _build_creds_uri() -> str:
    """Assemble the snowflake:// URI from per-field env vars.

    Required:
      SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PASSWORD,
      SNOWFLAKE_TEST_DATABASE, SNOWFLAKE_TEST_WAREHOUSE
    Optional:
      SNOWFLAKE_TEST_ROLE
    """
    user = os.environ["SNOWFLAKE_TEST_USER"]
    password = os.environ["SNOWFLAKE_TEST_PASSWORD"]
    account = os.environ["SNOWFLAKE_TEST_ACCOUNT"]
    database = os.environ["SNOWFLAKE_TEST_DATABASE"]
    warehouse = os.environ["SNOWFLAKE_TEST_WAREHOUSE"]
    role = os.environ.get("SNOWFLAKE_TEST_ROLE")
    qs = f"warehouse={warehouse}"
    if role:
        qs += f"&role={role}"
    return f"snowflake://{user}:{password}@{account}/{database}?{qs}"


def _stage_pipeline(tmp_path: Path, dataset: str) -> Path:
    """Write a minimal REST->Snowflake YAML the factory can load."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    yaml_path = pipelines_dir / "smoketest.yml"
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "name": "smoketest",
                "source": {
                    "type": "rest_api",
                    "connection": "public_demo_api",
                    "config": {
                        "base_url": "https://pokeapi.co/api/v2/",
                        "endpoints": ["pokemon"],
                    },
                },
                "sync": {"mode": "full_refresh"},
                "destination": {
                    "type": "snowflake",
                    "connection": "snowflake_test",
                    "dataset": dataset,
                },
                "schedule": {"cron": "0 0 * * *", "enabled": False},
                "options": {"write_disposition": "replace"},
            }
        )
    )
    return pipelines_dir


def test_snowflake_smoke_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = f"dlt_test_{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv(
        "DESTINATION__SNOWFLAKE_TEST__CREDENTIALS",
        _build_creds_uri(),
    )
    pipelines_dir = _stage_pipeline(tmp_path, dataset)
    monkeypatch.chdir(tmp_path)
    load_info = pipeline_factory.run("smoketest", pipelines_root=pipelines_dir)
    assert load_info is not None

    import snowflake.connector

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_TEST_USER"],
        password=os.environ["SNOWFLAKE_TEST_PASSWORD"],
        account=os.environ["SNOWFLAKE_TEST_ACCOUNT"],
        database=os.environ["SNOWFLAKE_TEST_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_TEST_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_TEST_ROLE"),
    )
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {dataset}.pokemon")
            (count,) = cur.fetchone()
            assert int(count) > 0
        finally:
            cur.close()
        cleanup = conn.cursor()
        try:
            cleanup.execute(f"DROP SCHEMA IF EXISTS {dataset} CASCADE")
        finally:
            cleanup.close()
    finally:
        conn.close()
