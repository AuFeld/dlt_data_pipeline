"""End-to-end REST -> duckdb via the pipeline factory.

Default run is hermetic: ``responses`` intercepts the PokeAPI HTTP calls and
replays a fixture. Opt in to hitting the live API with ``RUN_LIVE_REST=1``.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path

import duckdb
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


def _duckdb_rowcount(db_path: Path, dataset: str, table: str) -> int:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        (count,) = conn.execute(f"SELECT COUNT(*) FROM {dataset}.{table}").fetchone()
    return int(count)


def test_rest_to_duckdb_mocked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipelines_dir = _stage(tmp_path)
    monkeypatch.chdir(tmp_path)
    payload = json.loads(CASSETTE.read_text())

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            re.compile(r"https://pokeapi\.co/api/v2/pokemon/?(\?.*)?$"),
            json=payload,
            status=200,
        )
        load_info = pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)

    assert load_info is not None

    db_path = tmp_path / ".dlt" / "local_duckdb.duckdb"
    assert db_path.exists(), f"expected duckdb at {db_path}"

    rows = _duckdb_rowcount(db_path, "raw_demo", "pokemon")
    assert rows == len(payload["results"])


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_REST") != "1",
    reason="set RUN_LIVE_REST=1 to hit the live PokeAPI",
)
def test_rest_to_duckdb_live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipelines_dir = _stage(tmp_path)
    monkeypatch.chdir(tmp_path)

    load_info = pipeline_factory.run(PIPELINE_NAME, pipelines_root=pipelines_dir)
    assert load_info is not None

    db_path = tmp_path / ".dlt" / "local_duckdb.duckdb"
    assert db_path.exists()
    rows = _duckdb_rowcount(db_path, "raw_demo", "pokemon")
    assert rows > 0
