"""Tests for `python -m dlt_data_pipeline sources ...`."""

from __future__ import annotations

import pytest

from dlt_data_pipeline.__main__ import main


def test_sources_list_includes_all_builtins(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["sources", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    for name in ("rest_api", "sql_database", "filesystem", "pg_cdc"):
        assert name in out


def test_sources_describe_sql_database(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["sources", "describe", "sql_database"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "SOURCES__SQL_DATABASE__<CONNECTION>__CREDENTIALS" in out
    assert "tables" in out  # required key
    assert "schema" in out  # allowed key


def test_sources_describe_rest_api_no_creds(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["sources", "describe", "rest_api"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "no credentials" in out.lower() or "(none" in out


def test_sources_describe_unknown_type(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["sources", "describe", "definitely_not_a_real_source"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "definitely_not_a_real_source" in err
