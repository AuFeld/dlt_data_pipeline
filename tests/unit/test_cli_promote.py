"""Tests for `python -m dlt_data_pipeline pipelines promote` (Segment 13)."""

from __future__ import annotations

from pathlib import Path

import pytest

from dlt_data_pipeline.__main__ import main

_BASE_ALPHA = """\
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
  enabled: true
options:
  write_disposition: replace
"""


@pytest.fixture
def promote_root(tmp_path: Path) -> Path:
    (tmp_path / "alpha.yml").write_text(_BASE_ALPHA)
    (tmp_path / "_env").mkdir()
    (tmp_path / "_env" / "dev.yml").write_text("alpha: {}\n")
    (tmp_path / "_env" / "prod.yml").write_text(
        "alpha:\n"
        "  destination:\n"
        "    type: snowflake\n"
        "    connection: snowflake_warehouse\n"
        "    dataset: prod_raw_alpha\n"
        "  resources:\n"
        '    cpu: "1000m"\n'
        '    memory: "2Gi"\n'
    )
    return tmp_path


def test_promote_dev_to_prod_prints_diff(
    promote_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "pipelines",
            "promote",
            "alpha",
            "--from",
            "dev",
            "--to",
            "prod",
            "--pipelines-root",
            str(promote_root),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "dev -> prod" in out
    assert "destination.type" in out
    assert "duckdb -> snowflake" in out
    assert "destination.connection" in out
    assert "local_duckdb -> snowflake_warehouse" in out
    assert "destination.dataset" in out
    assert "resources.cpu" in out
    assert "1000m" in out
    assert "resources.memory" in out
    assert "2Gi" in out
    # Five fields differ (type, connection, dataset, cpu, memory).
    assert "5 fields differ" in out


def test_promote_identical_envs_reports_no_diff(
    promote_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "pipelines",
            "promote",
            "alpha",
            "--from",
            "dev",
            "--to",
            "dev",
            "--pipelines-root",
            str(promote_root),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "no differences" in out


def test_promote_unknown_pipeline_exit_1(
    promote_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "pipelines",
            "promote",
            "ghost",
            "--from",
            "dev",
            "--to",
            "prod",
            "--pipelines-root",
            str(promote_root),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "ghost" in err
    assert "not found" in err


def test_promote_unset_to_set_renders_unset_marker(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Resources unset in dev (base default) -> set in prod renders (unset) -> value."""
    (tmp_path / "alpha.yml").write_text(_BASE_ALPHA)
    (tmp_path / "_env").mkdir()
    (tmp_path / "_env" / "prod.yml").write_text('alpha:\n  resources:\n    cpu: "500m"\n')
    rc = main(
        [
            "pipelines",
            "promote",
            "alpha",
            "--from",
            "dev",
            "--to",
            "prod",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "resources.cpu" in out
    assert "(unset) -> 500m" in out
