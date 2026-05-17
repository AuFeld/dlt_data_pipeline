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


def test_post_write_validate_returns_zero_for_sql_postgres(tmp_path: Path) -> None:
    rc = scaffolder.main(
        [
            "demo_sql_pg",
            "--source",
            "sql_database",
            "--dest",
            "postgres",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = tmp_path / "demo_sql_pg.yml"
    assert out.exists()
    from data_pipeline_template.cli.pipelines_cmds import validate_pipelines

    report = validate_pipelines(tmp_path, "demo_sql_pg")
    assert report["status"] == "ok", report


def test_post_write_validate_failure_unlinks_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        scaffolder,
        "validate_pipelines",
        lambda root, name: {"status": "error", "errors": ["broken"], "pipelines": []},
    )
    rc = scaffolder.main(
        [
            "wont_keep",
            "--source",
            "rest_api",
            "--dest",
            "duckdb",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    assert rc == 2
    assert not (tmp_path / "wont_keep.yml").exists()


def test_destination_metadata_env_var_hint_present(tmp_path: Path) -> None:
    scaffolder.main(
        [
            "with_dest_meta",
            "--source",
            "rest_api",
            "--dest",
            "postgres",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    body = (tmp_path / "with_dest_meta.yml").read_text()
    assert "DESTINATION__POSTGRES_CONN__CREDENTIALS" in body


def test_alerts_block_present_and_parses(tmp_path: Path) -> None:
    import yaml

    scaffolder.main(
        [
            "has_alerts",
            "--source",
            "rest_api",
            "--dest",
            "duckdb",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    parsed = yaml.safe_load((tmp_path / "has_alerts.yml").read_text())
    assert parsed["alerts"]["severity"] == "P2"
    assert parsed["alerts"]["dedup_window_minutes"] == 15
    assert parsed["alerts"]["on_schema_change"] is True


def test_required_source_keys_emitted_as_yaml_values(tmp_path: Path) -> None:
    import yaml

    scaffolder.main(
        [
            "yaml_values",
            "--source",
            "sql_database",
            "--dest",
            "duckdb",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    parsed = yaml.safe_load((tmp_path / "yaml_values.yml").read_text())
    assert parsed["source"]["config"]["tables"] == []


def test_source_env_var_hint_present_for_sql_database(tmp_path: Path) -> None:
    scaffolder.main(
        [
            "with_src_env",
            "--source",
            "sql_database",
            "--dest",
            "duckdb",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    body = (tmp_path / "with_src_env.yml").read_text()
    assert "SOURCES__SQL_DATABASE__SQL_DATABASE_CONN__CREDENTIALS" in body
