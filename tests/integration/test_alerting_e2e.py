"""Done-when check for Segment 9: failure callback posts a Slack webhook.

Builds the DAG from ``example_pg_to_pg_incremental.yml`` (with alerts.severity
overridden to P1 for visible routing), invokes ``dag.on_failure_callback``
directly with a synthesized Context (running the scheduler would require a
metadata DB + worker process), and asserts that exactly one webhook POST went
out with the expected payload shape and channel routing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from data_pipeline_template.airflow.dag_factory import build_dag
from data_pipeline_template.config.loader import load_pipelines
from data_pipeline_template.observability import alerts as alerts_mod

PIPELINES_ROOT = Path(__file__).resolve().parents[2] / "pipelines"


@pytest.fixture
def configured_alerts_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPT_ALERTS_SLACK_WEBHOOK_URL", "http://hooks.test/webhook")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_P1", "#oncall-data")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_P2", "#data-alerts")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_INFO", "#data-info")
    # Required for build_pipeline; bogus URL is fine because we never run it.
    monkeypatch.setenv(
        "SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS",
        "sqlite:///:memory:",
    )
    monkeypatch.setenv(
        "DESTINATION__POSTGRES__PG_WAREHOUSE__CREDENTIALS",
        "postgresql://u:p@h:5432/db",
    )


@pytest.fixture
def isolate_dedup(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, str] = {}
    monkeypatch.setattr(alerts_mod, "_dedup_get", lambda k: store.get(k))
    monkeypatch.setattr(alerts_mod, "_dedup_set", lambda k, v: store.__setitem__(k, v))


def test_failure_callback_posts_slack_for_pg_to_pg_incremental(
    configured_alerts_env: None,
    isolate_dedup: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configs = load_pipelines(PIPELINES_ROOT)
    cfg = configs["example_pg_to_pg_incremental"].model_copy(deep=True)
    cfg.alerts.severity = type(cfg.alerts.severity).P1
    dag = build_dag(cfg)

    captured: list[Any] = []

    def fake_urlopen(req: Any, timeout: float = 0) -> MagicMock:
        captured.append(req)
        resp = MagicMock()
        resp.read.return_value = b"ok"
        return resp

    monkeypatch.setattr(alerts_mod.urllib.request, "urlopen", fake_urlopen)

    ti = MagicMock()
    ti.dag_id = cfg.name
    ti.task_id = f"{cfg.name}.tg.run"
    ti.run_id = "manual__2026-05-17T00:00:00+00:00"
    ti.log_url = "http://airflow.test/log/1"
    context = {"task_instance": ti, "exception": RuntimeError("simulated load failure")}

    assert dag.on_failure_callback is not None
    dag.on_failure_callback(context)

    assert len(captured) == 1, f"expected exactly one webhook POST, got {len(captured)}"
    req = captured[0]
    assert req.full_url == "http://hooks.test/webhook"
    payload = json.loads(req.data.decode("utf-8"))
    assert payload["channel"] == "#oncall-data"
    assert payload["text"].startswith("[P1]")
    rendered = json.dumps(payload)
    assert cfg.name in rendered
    assert "simulated load failure" in rendered
