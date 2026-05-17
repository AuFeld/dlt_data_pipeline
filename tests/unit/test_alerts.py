from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from data_pipeline_template.config.models import AlertsConfig, AlertSeverity
from data_pipeline_template.observability import alerts as alerts_mod


@pytest.fixture(autouse=True)
def _isolate_dedup(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Replace the Airflow Variable dedup backend with an in-memory dict."""
    store: dict[str, str] = {}
    monkeypatch.setattr(alerts_mod, "_dedup_get", lambda k: store.get(k))
    monkeypatch.setattr(alerts_mod, "_dedup_set", lambda k, v: store.__setitem__(k, v))
    return store


@pytest.fixture
def _slack_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPT_ALERTS_SLACK_WEBHOOK_URL", "http://hooks.test/webhook")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_P1", "#oncall-data")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_P2", "#data-alerts")
    monkeypatch.setenv("DPT_ALERTS_SLACK_CHANNEL_INFO", "#data-info")


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


def _payload_of(req: Any) -> dict[str, Any]:
    return json.loads(req.data.decode("utf-8"))


def test_post_failure_alert_calls_webhook(_slack_env: None, capture_urlopen: list[Any]) -> None:
    result = alerts_mod.post_failure_alert(
        pipeline_name="example_pg_to_pg_incremental",
        severity=AlertSeverity.P1,
        dag_id="example_pg_to_pg_incremental",
        task_id="example_pg_to_pg_incremental.tg.run",
        run_id="scheduled__2026-05-17T06:00:00",
        log_url="http://airflow.test/log/1",
        exception_summary="RuntimeError: boom",
        alerts=AlertsConfig(severity=AlertSeverity.P1),
    )
    assert result.slack_posted is True
    assert result.deduped is False
    assert len(capture_urlopen) == 1
    req = capture_urlopen[0]
    assert req.full_url == "http://hooks.test/webhook"
    assert req.get_method() == "POST"
    payload = _payload_of(req)
    assert payload["channel"] == "#oncall-data"
    assert payload["text"].startswith("[P1]")
    assert any("RuntimeError: boom" in str(b) for b in payload["blocks"])


def test_post_failure_alert_yaml_channel_override_beats_env(
    _slack_env: None, capture_urlopen: list[Any]
) -> None:
    alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=AlertSeverity.P1,
        dag_id="d",
        task_id="t",
        run_id="r",
        log_url=None,
        exception_summary="e",
        alerts=AlertsConfig(severity=AlertSeverity.P1, slack_channel="#yaml-override"),
    )
    payload = _payload_of(capture_urlopen[0])
    assert payload["channel"] == "#yaml-override"


def test_post_failure_alert_deduped_within_window(
    _slack_env: None, capture_urlopen: list[Any]
) -> None:
    cfg = AlertsConfig(severity=AlertSeverity.P2, dedup_window_minutes=15)
    first = alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=cfg.severity,
        dag_id="d",
        task_id="t",
        run_id="r1",
        log_url=None,
        exception_summary="e",
        alerts=cfg,
    )
    second = alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=cfg.severity,
        dag_id="d",
        task_id="t",
        run_id="r2",
        log_url=None,
        exception_summary="e",
        alerts=cfg,
    )
    assert first.slack_posted is True
    assert first.deduped is False
    assert second.deduped is True
    assert second.slack_posted is False
    assert len(capture_urlopen) == 1


def test_post_failure_alert_dedup_expires(
    _slack_env: None, capture_urlopen: list[Any], _isolate_dedup: dict[str, str]
) -> None:
    cfg = AlertsConfig(severity=AlertSeverity.P2, dedup_window_minutes=5)
    alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=cfg.severity,
        dag_id="d",
        task_id="t",
        run_id="r1",
        log_url=None,
        exception_summary="e",
        alerts=cfg,
    )
    # Backdate the dedup mark past the window.
    stale = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
    for key in list(_isolate_dedup):
        _isolate_dedup[key] = stale
    second = alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=cfg.severity,
        dag_id="d",
        task_id="t",
        run_id="r2",
        log_url=None,
        exception_summary="e",
        alerts=cfg,
    )
    assert second.slack_posted is True
    assert second.deduped is False
    assert len(capture_urlopen) == 2


def test_dedup_disabled_when_window_zero(_slack_env: None, capture_urlopen: list[Any]) -> None:
    cfg = AlertsConfig(severity=AlertSeverity.P2, dedup_window_minutes=0)
    for _ in range(3):
        alerts_mod.post_failure_alert(
            pipeline_name="p",
            severity=cfg.severity,
            dag_id="d",
            task_id="t",
            run_id="r",
            log_url=None,
            exception_summary="e",
            alerts=cfg,
        )
    assert len(capture_urlopen) == 3


def test_slack_unconfigured_no_op(
    monkeypatch: pytest.MonkeyPatch, capture_urlopen: list[Any]
) -> None:
    monkeypatch.delenv("DPT_ALERTS_SLACK_WEBHOOK_URL", raising=False)
    result = alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=AlertSeverity.P2,
        dag_id="d",
        task_id="t",
        run_id="r",
        log_url=None,
        exception_summary="e",
        alerts=AlertsConfig(),
    )
    assert result.slack_posted is False
    assert result.reason == "slack-webhook-unset"
    assert capture_urlopen == []


def test_slack_post_swallows_network_errors(
    _slack_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(req: Any, timeout: float = 0) -> Any:
        raise OSError("boom")

    monkeypatch.setattr(alerts_mod.urllib.request, "urlopen", boom)
    result = alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=AlertSeverity.P2,
        dag_id="d",
        task_id="t",
        run_id="r",
        log_url=None,
        exception_summary="e",
        alerts=AlertsConfig(),
    )
    assert result.slack_posted is False
    assert result.reason is not None and "slack-error" in result.reason


def test_email_skipped_when_recipients_empty(_slack_env: None, capture_urlopen: list[Any]) -> None:
    result = alerts_mod.post_failure_alert(
        pipeline_name="p",
        severity=AlertSeverity.P2,
        dag_id="d",
        task_id="t",
        run_id="r",
        log_url=None,
        exception_summary="e",
        alerts=AlertsConfig(),
    )
    assert result.email_sent is False


def test_email_sent_via_airflow_send_email_smtp(
    _slack_env: None, capture_urlopen: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list[tuple[Any, ...]] = []

    def fake_send(recipients: list[str], subject: str, html: str) -> None:
        sent.append((recipients, subject, html))

    # Replace the lazy-imported helper at its real module path.
    import airflow.utils.email as email_mod  # type: ignore[import-not-found]

    monkeypatch.setattr(email_mod, "send_email_smtp", fake_send)
    result = alerts_mod.post_failure_alert(
        pipeline_name="example",
        severity=AlertSeverity.P1,
        dag_id="d",
        task_id="t",
        run_id="r",
        log_url=None,
        exception_summary="boom",
        alerts=AlertsConfig(severity=AlertSeverity.P1, email_recipients=["data@example.com"]),
    )
    assert result.email_sent is True
    assert sent and sent[0][0] == ["data@example.com"]
    assert "example" in sent[0][1]


def test_schema_change_payload_includes_changes(
    _slack_env: None, capture_urlopen: list[Any]
) -> None:
    updates = [
        {"table": "orders", "column": "discount", "data_type": "double"},
        {"table": "customers", "column": "loyalty_tier", "data_type": "text"},
    ]
    result = alerts_mod.post_schema_change_alert(
        pipeline_name="p",
        dag_id="d",
        run_id="r",
        schema_updates=updates,
        alerts=AlertsConfig(),
    )
    assert result.slack_posted is True
    payload = _payload_of(capture_urlopen[0])
    assert payload["channel"] == "#data-info"
    assert payload["text"].startswith("[info]")
    rendered = json.dumps(payload)
    assert "orders" in rendered and "loyalty_tier" in rendered


def test_schema_change_no_updates_is_noop(_slack_env: None, capture_urlopen: list[Any]) -> None:
    result = alerts_mod.post_schema_change_alert(
        pipeline_name="p", dag_id="d", run_id="r", schema_updates=[], alerts=AlertsConfig()
    )
    assert result.slack_posted is False
    assert result.reason == "no-updates"
    assert capture_urlopen == []


def test_sla_miss_alert_posts(_slack_env: None, capture_urlopen: list[Any]) -> None:
    result = alerts_mod.post_sla_miss_alert(
        pipeline_name="p",
        dag_id="d",
        task_ids=["t1", "t2"],
        alerts=AlertsConfig(severity=AlertSeverity.P1),
    )
    assert result.slack_posted is True
    payload = _payload_of(capture_urlopen[0])
    assert payload["channel"] == "#oncall-data"
    assert "SLA miss" in payload["text"]
