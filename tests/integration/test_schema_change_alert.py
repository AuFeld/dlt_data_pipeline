"""Segment 14 done-when #2: schema-change alert fires for evolve pipelines.

End-to-end: run a real dlt pipeline twice into duckdb, second run adds a
column (``schema_contract='evolve'``). Then invoke the same
``schema_change_probe`` callable that ``dag_factory`` wires into a trailing
task and assert exactly one info-level webhook POST went out.

Hermetic — no live services, no network. Slack webhook is mocked at the
``urllib.request.urlopen`` boundary, identical to ``test_alerting_e2e``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import dlt
import pytest

from dlt_data_pipeline.airflow import callbacks as cb_mod
from dlt_data_pipeline.config.models import AlertsConfig, AlertSeverity
from dlt_data_pipeline.observability import alerts as alerts_mod


@pytest.fixture
def slack_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPT_ALERTS_SLACK_WEBHOOK_URL", "http://hooks.test/webhook")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_INFO", "#data-info")


@pytest.fixture
def isolate_dedup(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, str] = {}
    monkeypatch.setattr(alerts_mod, "_dedup_get", lambda k: store.get(k))
    monkeypatch.setattr(alerts_mod, "_dedup_set", lambda k, v: store.__setitem__(k, v))


@pytest.fixture
def capture_urlopen(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    calls: list[Any] = []

    def fake_urlopen(req: Any, timeout: float = 0) -> MagicMock:
        calls.append(req)
        resp = MagicMock()
        resp.read.return_value = b"ok"
        return resp

    monkeypatch.setattr(alerts_mod.urllib.request, "urlopen", fake_urlopen)
    return calls


def test_schema_evolution_emits_info_alert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slack_env: None,
    isolate_dedup: None,
    capture_urlopen: list[Any],
) -> None:
    # Let dlt derive pipelines_dir from DLT_DATA_DIR so the probe's
    # parameter-less ``dlt.pipeline(pipeline_name=...)`` lookup resolves to
    # the same on-disk dir we wrote to.
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path))
    monkeypatch.chdir(tmp_path)

    pipeline_name = "test_schema_evolve_alert"
    duckdb_path = tmp_path / "evolve.duckdb"

    @dlt.resource(name="rows", write_disposition="merge", primary_key="id")
    def rows_v1() -> Any:
        yield {"id": 1, "name": "alice"}
        yield {"id": 2, "name": "bob"}

    @dlt.resource(name="rows", write_disposition="merge", primary_key="id")
    def rows_v2() -> Any:
        # New column ``email`` should trigger a schema-evolution event under
        # schema_contract='evolve'.
        yield {"id": 1, "name": "alice", "email": "alice@example.com"}
        yield {"id": 3, "name": "carol", "email": "carol@example.com"}

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(duckdb_path)),
        dataset_name="raw_evolve",
    )

    pipeline.run(rows_v1(), schema_contract="evolve")
    pipeline.run(rows_v2(), schema_contract="evolve")

    # Sanity: the second run's trace must surface schema deltas to the helper
    # the probe uses. If this assertion fires it's a dlt-version regression,
    # not a test bug.
    updates = cb_mod._extract_schema_updates(pipeline.last_trace)
    assert updates, "expected non-empty schema updates after evolving the source"

    cb_mod.schema_change_probe(
        AlertsConfig(severity=AlertSeverity.P2, slack_channel=None),
        pipeline_name,
    )

    assert len(capture_urlopen) == 1, (
        f"expected exactly one webhook POST, got {len(capture_urlopen)}"
    )
    payload = json.loads(capture_urlopen[0].data.decode("utf-8"))
    assert payload["channel"] == "#data-info"
    assert payload["text"].startswith("[info]")
    assert pipeline_name in json.dumps(payload)
