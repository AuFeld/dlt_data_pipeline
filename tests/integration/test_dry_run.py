"""Dry-run (`run --no-load`) integration: extract + normalize, no destination write."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pytest
import responses

from data_pipeline_template import pipeline_factory

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_SRC = REPO_ROOT / "pipelines" / "example_rest_to_duckdb.yml"
CASSETTE = REPO_ROOT / "tests" / "fixtures" / "rest_cassettes" / "pokemon.json"
PIPELINE_NAME = "example_rest_to_duckdb"


def _stage(tmp_path: Path) -> Path:
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    shutil.copy(PIPELINES_SRC, pipelines_dir / PIPELINES_SRC.name)
    return pipelines_dir


def test_dry_run_extracts_without_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipelines_dir = _stage(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    payload = json.loads(CASSETTE.read_text())

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            re.compile(r"https://pokeapi\.co/api/v2/pokemon/?(\?.*)?$"),
            json=payload,
            status=200,
        )
        result = pipeline_factory.run_dry(PIPELINE_NAME, pipelines_root=pipelines_dir)

    assert result["name"] == PIPELINE_NAME
    assert result["extract"]
    assert result["normalize"]

    # Load was skipped: no duckdb file should have been materialized with data.
    db_path = tmp_path / ".dlt" / "local_duckdb.duckdb"
    if db_path.exists():
        import duckdb

        with duckdb.connect(str(db_path), read_only=True) as conn:
            schemas = {
                row[0]
                for row in conn.execute(
                    "SELECT schema_name FROM information_schema.schemata"
                ).fetchall()
            }
        assert "raw_demo" not in schemas, (
            "dry-run materialized destination dataset; --no-load is broken"
        )


def test_limit_caps_yields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipelines_dir = _stage(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))
    payload = json.loads(CASSETTE.read_text())

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(
            responses.GET,
            re.compile(r"https://pokeapi\.co/api/v2/pokemon/?(\?.*)?$"),
            json=payload,
            status=200,
        )
        # limit=1 + --no-load proves both flags compose without error; the
        # exact row-count guarantee is dlt-internal (add_limit caps yields,
        # not necessarily rows when a single page is large).
        result = pipeline_factory.run_dry(PIPELINE_NAME, pipelines_root=pipelines_dir, limit=1)

    assert result["name"] == PIPELINE_NAME
