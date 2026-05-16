"""Scaffolder round-trip: emit YAML, assert structure + parseability."""

from __future__ import annotations

from pathlib import Path

import pytest
import scripts.new_pipeline as scaffolder
import yaml


def test_emits_file_with_schema_directive(tmp_path: Path) -> None:
    rc = scaffolder.main(
        [
            "test_pipe",
            "--source",
            "rest_api",
            "--dest",
            "duckdb",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = tmp_path / "test_pipe.yml"
    assert out.exists()
    body = out.read_text()
    assert body.startswith("# yaml-language-server: $schema=./_schema.json")
    assert "name: test_pipe" in body
    assert "type: rest_api" in body
    assert "type: duckdb" in body


def test_emitted_yaml_parses_as_mapping(tmp_path: Path) -> None:
    scaffolder.main(
        [
            "scaffold_parse",
            "--source",
            "sql_database",
            "--dest",
            "postgres",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    parsed = yaml.safe_load((tmp_path / "scaffold_parse.yml").read_text())
    assert isinstance(parsed, dict)
    assert parsed["name"] == "scaffold_parse"
    assert parsed["source"]["type"] == "sql_database"
    assert parsed["destination"]["type"] == "postgres"


def test_required_keys_appear_in_todo_comment(tmp_path: Path) -> None:
    scaffolder.main(
        [
            "needs_tables",
            "--source",
            "sql_database",
            "--dest",
            "duckdb",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    body = (tmp_path / "needs_tables.yml").read_text()
    # sql_database metadata declares tables as required.
    assert "tables" in body


def test_refuses_to_overwrite(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = [
        "twice",
        "--source",
        "rest_api",
        "--dest",
        "duckdb",
        "--pipelines-root",
        str(tmp_path),
    ]
    assert scaffolder.main(args) == 0
    rc = scaffolder.main(args)
    assert rc == 1
    assert "already exists" in capsys.readouterr().err


def test_rejects_unknown_source(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        scaffolder.main(
            [
                "bad",
                "--source",
                "not_real",
                "--dest",
                "duckdb",
                "--pipelines-root",
                str(tmp_path),
            ]
        )
