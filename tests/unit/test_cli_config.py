"""Tests for `python -m data_pipeline_template config schema`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_pipeline_template.__main__ import main


def test_schema_writes_valid_json(tmp_path: Path) -> None:
    out = tmp_path / "_schema.json"
    rc = main(["config", "schema", "--out", str(out)])
    assert rc == 0
    assert out.exists()
    parsed = json.loads(out.read_text())
    # PipelineConfig is the top-level title; sanity-check structure.
    assert parsed["title"] == "PipelineConfig"
    assert "name" in parsed["required"]
    assert "$defs" in parsed


def test_schema_check_passes_when_fresh(tmp_path: Path) -> None:
    out = tmp_path / "_schema.json"
    assert main(["config", "schema", "--out", str(out)]) == 0
    assert main(["config", "schema", "--check", "--out", str(out)]) == 0


def test_schema_check_fails_when_stale(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "_schema.json"
    out.write_text('{"stale": true}\n')
    rc = main(["config", "schema", "--check", "--out", str(out)])
    assert rc == 1
    assert "stale" in capsys.readouterr().err.lower()


def test_schema_check_fails_when_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "does_not_exist.json"
    rc = main(["config", "schema", "--check", "--out", str(out)])
    assert rc == 1
    assert "does not exist" in capsys.readouterr().err.lower()


def test_committed_schema_is_in_sync() -> None:
    """Repo-level invariant: pipelines/_schema.json matches current models."""
    repo_schema = Path(__file__).resolve().parents[2] / "pipelines" / "_schema.json"
    rc = main(["config", "schema", "--check", "--out", str(repo_schema)])
    assert rc == 0, (
        "pipelines/_schema.json is out of sync with config/models.py. "
        "Run `python -m data_pipeline_template config schema` to regenerate."
    )
