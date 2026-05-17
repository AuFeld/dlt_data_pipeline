"""`python -m dlt_data_pipeline pipelines delete <name>` tests (Segment 12).

Exercise the dry-run / --yes gating, the not-found path, and the idempotent
no-op semantics. Live-Postgres slot/publication teardown lives in
`tests/integration/test_pipelines_delete_pg_cdc.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dlt_data_pipeline.__main__ import main
from dlt_data_pipeline.cli import delete_cmds

_NOOP_YAML = (
    "name: noop_delete\n"
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


def test_delete_dry_run_without_yes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "noop_delete.yml").write_text(_NOOP_YAML)
    rc = main(["pipelines", "delete", "noop_delete", "--pipelines-root", str(tmp_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Would delete" in out
    assert "Re-run with --yes" in out
    # YAML still on disk.
    assert (tmp_path / "noop_delete.yml").exists()


def test_delete_not_found_exits_1(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["pipelines", "delete", "missing", "--yes", "--pipelines-root", str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "not found" in err


def test_delete_keep_data_skips_dataset_drop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "noop_delete.yml").write_text(_NOOP_YAML)
    called: list[str] = []
    monkeypatch.setattr(
        delete_cmds, "_drop_destination_dataset", lambda cfg: called.append("dest") or []
    )
    monkeypatch.setattr(delete_cmds, "_drop_local_state", lambda cfg: [])
    rc = main(
        [
            "pipelines",
            "delete",
            "noop_delete",
            "--yes",
            "--keep-data",
            "--pipelines-root",
            str(tmp_path),
        ]
    )
    assert rc == 0
    assert called == []  # --keep-data must skip the dataset step
    assert not (tmp_path / "noop_delete.yml").exists()
    assert "no longer generated" in capsys.readouterr().out


def test_delete_executes_all_steps_when_yes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "noop_delete.yml").write_text(_NOOP_YAML)
    log: list[str] = []
    monkeypatch.setattr(
        delete_cmds, "_drop_destination_dataset", lambda cfg: log.append("dest") or []
    )
    monkeypatch.setattr(delete_cmds, "_drop_local_state", lambda cfg: log.append("state") or [])
    rc = main(["pipelines", "delete", "noop_delete", "--yes", "--pipelines-root", str(tmp_path)])
    assert rc == 0
    assert log == ["dest", "state"]
    assert not (tmp_path / "noop_delete.yml").exists()


def test_delete_idempotent_second_call_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "noop_delete.yml").write_text(_NOOP_YAML)
    monkeypatch.setattr(delete_cmds, "_drop_destination_dataset", lambda cfg: [])
    monkeypatch.setattr(delete_cmds, "_drop_local_state", lambda cfg: [])
    rc1 = main(["pipelines", "delete", "noop_delete", "--yes", "--pipelines-root", str(tmp_path)])
    assert rc1 == 0
    rc2 = main(["pipelines", "delete", "noop_delete", "--yes", "--pipelines-root", str(tmp_path)])
    assert rc2 == 1  # second time it's not-found


def test_delete_reports_per_step_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "noop_delete.yml").write_text(_NOOP_YAML)
    monkeypatch.setattr(
        delete_cmds, "_drop_destination_dataset", lambda cfg: ["DROP SCHEMA: connection refused"]
    )
    monkeypatch.setattr(delete_cmds, "_drop_local_state", lambda cfg: [])
    rc = main(["pipelines", "delete", "noop_delete", "--yes", "--pipelines-root", str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "connection refused" in err
    # YAML should still be unlinked even if a teardown step errored — the
    # operator wants the source-of-truth removed so they can re-author.
    assert not (tmp_path / "noop_delete.yml").exists()


def test_delete_pure_helper_returns_steps(tmp_path: Path) -> None:
    (tmp_path / "noop_delete.yml").write_text(_NOOP_YAML)
    # Patch the heavy steps via monkeypatch.context to avoid leaking outside
    # this test; use plain attribute assignment with try/finally.
    saved_dest = delete_cmds._drop_destination_dataset
    saved_state = delete_cmds._drop_local_state
    delete_cmds._drop_destination_dataset = lambda cfg: []  # type: ignore[assignment]
    delete_cmds._drop_local_state = lambda cfg: []  # type: ignore[assignment]
    try:
        report: dict[str, Any] = delete_cmds.delete_pipeline("noop_delete", tmp_path)
    finally:
        delete_cmds._drop_destination_dataset = saved_dest  # type: ignore[assignment]
        delete_cmds._drop_local_state = saved_state  # type: ignore[assignment]
    assert report["status"] == "ok"
    assert report["yaml_removed"] is True
    step_names = [s["step"] for s in report["steps"]]  # type: ignore[index]
    assert "drop_destination_dataset" in step_names
    assert "drop_local_state" in step_names
