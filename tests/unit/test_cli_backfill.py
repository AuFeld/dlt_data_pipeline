"""`python -m dlt_data_pipeline run-backfill` CLI tests (Segment 12).

Stubs `pipeline_factory.run_backfill` so we can exercise the CLI argparsing
and exit-code paths without spinning up a real source/destination. The real
chunked-load behavior is covered by the integration suite.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dlt_data_pipeline import pipeline_factory
from dlt_data_pipeline.__main__ import main

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture
def stub_run_backfill(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _stub(name: str, start: datetime, end: datetime, pipelines_root: Path | str = "pipelines"):
        calls.append({"name": name, "start": start, "end": end, "pipelines_root": pipelines_root})
        # Pretend two chunks landed.
        return [object(), object()]

    monkeypatch.setattr(pipeline_factory, "run_backfill", _stub)
    return calls


def test_run_backfill_happy_path(
    capsys: pytest.CaptureFixture[str], stub_run_backfill: list[dict[str, Any]]
) -> None:
    rc = main(
        [
            "run-backfill",
            "demo",
            "--start",
            "2025-01-01T00:00:00+00:00",
            "--end",
            "2025-01-15T00:00:00+00:00",
            "--pipelines-root",
            str(FIXTURES / "pipelines_valid"),
        ]
    )
    assert rc == 0
    assert "in 2 chunk(s)" in capsys.readouterr().out
    assert stub_run_backfill[0]["name"] == "demo"
    assert stub_run_backfill[0]["start"] == datetime(2025, 1, 1, tzinfo=UTC)
    assert stub_run_backfill[0]["end"] == datetime(2025, 1, 15, tzinfo=UTC)


def test_run_backfill_rejects_naive_timestamp(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(
        [
            "run-backfill",
            "demo",
            "--start",
            "2025-01-01T00:00:00",
            "--end",
            "2025-01-15T00:00:00+00:00",
            "--pipelines-root",
            str(FIXTURES / "pipelines_valid"),
        ]
    )
    assert rc == 1
    assert "timezone-aware" in capsys.readouterr().err


def test_run_backfill_propagates_factory_error(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(*_: Any, **__: Any) -> None:
        raise ValueError(
            "run_backfill: pipeline 'demo' sync.mode is 'cdc'; backfill requires 'incremental'"
        )

    monkeypatch.setattr(pipeline_factory, "run_backfill", _raise)
    rc = main(
        [
            "run-backfill",
            "demo",
            "--start",
            "2025-01-01T00:00:00+00:00",
            "--end",
            "2025-01-15T00:00:00+00:00",
            "--pipelines-root",
            str(FIXTURES / "pipelines_valid"),
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "backfill requires 'incremental'" in err


def test_run_backfill_requires_start_and_end() -> None:
    with pytest.raises(SystemExit):
        main(["run-backfill", "demo", "--start", "2025-01-01T00:00:00+00:00"])
