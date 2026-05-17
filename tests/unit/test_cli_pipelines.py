"""Tests for `python -m dlt_data_pipeline pipelines ...`."""

from __future__ import annotations

from pathlib import Path

import pytest

from dlt_data_pipeline.__main__ import main

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
    monkeypatch.delenv("AIRFLOW__SECRETS__BACKEND", raising=False)
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


def test_doctor_env_flag_reports_overlaid_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--env prod` applies pipelines/_env/prod.yml — destination env-var
    changes to match the overlaid connection name."""
    base = """\
name: alpha
source:
  type: rest_api
  connection: demo_api
  config:
    base_url: https://example.com/
sync:
  mode: full_refresh
destination:
  type: duckdb
  connection: local_duckdb
  dataset: raw_alpha
schedule:
  cron: "0 6 * * *"
"""
    (tmp_path / "alpha.yml").write_text(base)
    (tmp_path / "_env").mkdir()
    (tmp_path / "_env" / "prod.yml").write_text(
        "alpha:\n"
        "  destination:\n"
        "    type: postgres\n"
        "    connection: prod_warehouse\n"
        "    dataset: prod_raw_alpha\n"
    )
    # No env vars set for either dev or prod connections.
    monkeypatch.delenv("DESTINATION__LOCAL_DUCKDB__CREDENTIALS", raising=False)
    monkeypatch.delenv("DESTINATION__PROD_WAREHOUSE__CREDENTIALS", raising=False)
    monkeypatch.delenv("AIRFLOW__SECRETS__BACKEND", raising=False)
    monkeypatch.setenv("DLT_PROJECT_DIR", str(tmp_path))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "data"))

    rc = main(
        [
            "pipelines",
            "doctor",
            "--env",
            "prod",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    out = capsys.readouterr().out
    # Postgres destination needs creds; doctor reports the prod env var.
    assert "DESTINATION__PROD_WAREHOUSE__CREDENTIALS" in out
    assert "DESTINATION__LOCAL_DUCKDB__CREDENTIALS" not in out
    # Exit 1 because the prod env var is unset.
    assert rc == 1


def test_doctor_airflow_backend_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AIRFLOW__SECRETS__BACKEND set + no env / secrets.toml -> airflow-backend."""
    monkeypatch.delenv(_PG_SOURCE_ENV, raising=False)
    monkeypatch.delenv(_PG_DEST_ENV, raising=False)
    monkeypatch.setenv(
        "AIRFLOW__SECRETS__BACKEND",
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
    )
    monkeypatch.setenv("DLT_PROJECT_DIR", str(tmp_path))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "data"))

    pipelines_root = Path(__file__).resolve().parents[2] / "pipelines"
    rc = main(["pipelines", "doctor", "--pipelines-root", str(pipelines_root)])
    out = capsys.readouterr().out
    # No MISSING status — airflow-backend trumps it.
    assert "MISSING" not in out
    assert "airflow-backend" in out
    assert rc == 0
