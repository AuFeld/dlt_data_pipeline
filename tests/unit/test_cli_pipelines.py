"""Tests for `python -m data_pipeline_template pipelines ...`."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_pipeline_template.__main__ import main

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

_PG_SOURCE_ENV = "SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS"
_PG_DEST_ENV = "DESTINATION__PG_WAREHOUSE__CREDENTIALS"


def test_validate_all_valid_fixtures(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["pipelines", "validate", "--pipelines-root", str(FIXTURES / "pipelines_valid")])
    assert rc == 0
    out = capsys.readouterr().out
    for name in ("alpha", "beta", "gamma"):
        assert f"OK: {name}" in out


def test_validate_one_by_name(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        [
            "pipelines",
            "validate",
            "alpha",
            "--pipelines-root",
            str(FIXTURES / "pipelines_valid"),
        ]
    )
    assert rc == 0
    assert "OK: alpha" in capsys.readouterr().out


def test_validate_unknown_name_exit_1(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        [
            "pipelines",
            "validate",
            "nonexistent",
            "--pipelines-root",
            str(FIXTURES / "pipelines_valid"),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "nonexistent" in err


def test_validate_invalid_fixtures_exit_1(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        [
            "pipelines",
            "validate",
            "--pipelines-root",
            str(FIXTURES / "pipelines_invalid"),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "bad_yaml.yml" in err or "missing_cursor.yml" in err


def test_validate_empty_dir_exit_0(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        [
            "pipelines",
            "validate",
            "--pipelines-root",
            str(FIXTURES / "pipelines_empty"),
        ]
    )
    assert rc == 0
    assert "No pipelines" in capsys.readouterr().out


def test_doctor_ok_when_no_creds_required(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """rest_api + duckdb needs no credentials -> doctor reports OK + exit 0."""
    (tmp_path / "noop.yml").write_text(
        "name: noop\n"
        "source:\n"
        "  type: rest_api\n"
        "  connection: demo_api\n"
        "  config:\n"
        "    base_url: https://example.com/\n"
        "    endpoints: [things]\n"
        "sync:\n"
        "  mode: full_refresh\n"
        "destination:\n"
        "  type: duckdb\n"
        "  connection: local_duckdb\n"
        "  dataset: raw_noop\n"
        "schedule:\n"
        '  cron: "0 6 * * *"\n'
    )
    rc = main(["pipelines", "doctor", "--pipelines-root", str(tmp_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "noop: OK" in out
    assert "no-creds-required" in out


def test_doctor_missing_credentials_exit_1(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Pipeline needing pg_source creds — unset env, expect MISSING + exit 1."""
    monkeypatch.delenv(_PG_SOURCE_ENV, raising=False)
    monkeypatch.delenv(_PG_DEST_ENV, raising=False)
    # Block dlt.secrets fallback to .dlt/secrets.toml by pointing dlt at an
    # empty config dir for the duration of this test.
    monkeypatch.setenv("DLT_PROJECT_DIR", str(tmp_path))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "data"))

    pipelines_root = Path(__file__).resolve().parents[2] / "pipelines"
    rc = main(["pipelines", "doctor", "--pipelines-root", str(pipelines_root)])
    assert rc == 1
    out = capsys.readouterr().out
    assert "example_pg_to_pg_incremental: MISSING" in out
    assert _PG_SOURCE_ENV in out
