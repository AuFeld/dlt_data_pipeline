from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dlt_data_pipeline.airflow import callbacks as cb_mod
from dlt_data_pipeline.config.models import AlertsConfig, AlertSeverity


@pytest.fixture
def patch_post_failure(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_post(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return MagicMock(slack_posted=True, email_sent=False, deduped=False)

    monkeypatch.setattr(cb_mod, "post_failure_alert", fake_post)
    return calls


@pytest.fixture
def patch_post_sla(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(cb_mod, "post_sla_miss_alert", lambda **kw: calls.append(kw) or MagicMock())
    return calls


@pytest.fixture
def patch_post_schema(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cb_mod, "post_schema_change_alert", lambda **kw: calls.append(kw) or MagicMock()
    )
    return calls


def _fake_ti(**overrides: Any) -> MagicMock:
    ti = MagicMock()
    ti.dag_id = overrides.get("dag_id", "dag1")
    ti.task_id = overrides.get("task_id", "task1")
    ti.run_id = overrides.get("run_id", "run1")
    ti.log_url = overrides.get("log_url", "http://airflow/log/1")
    return ti


def test_make_on_failure_callback_passes_alerts_through(
    patch_post_failure: list[dict[str, Any]],
) -> None:
    alerts = AlertsConfig(severity=AlertSeverity.P1)
    cb = cb_mod.make_on_failure_callback(alerts, "pipe")
    cb({"task_instance": _fake_ti(), "exception": RuntimeError("boom")})
    assert len(patch_post_failure) == 1
    kwargs = patch_post_failure[0]
    assert kwargs["pipeline_name"] == "pipe"
    assert kwargs["severity"] is AlertSeverity.P1
    assert kwargs["dag_id"] == "dag1"
    assert kwargs["task_id"] == "task1"
    assert "RuntimeError" in kwargs["exception_summary"]


def test_on_failure_callback_swallows_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(**kwargs: Any) -> Any:
        raise RuntimeError("alerting broke")

    monkeypatch.setattr(cb_mod, "post_failure_alert", boom)
    cb = cb_mod.make_on_failure_callback(AlertsConfig(), "pipe")
    cb({"task_instance": _fake_ti(), "exception": RuntimeError("orig")})


def test_on_failure_handles_missing_task_instance(
    patch_post_failure: list[dict[str, Any]],
) -> None:
    cb = cb_mod.make_on_failure_callback(AlertsConfig(), "pipe")
    cb({})
    assert len(patch_post_failure) == 1
    assert patch_post_failure[0]["dag_id"] == "pipe"


def test_sla_miss_callback_routes_when_enabled(
    patch_post_sla: list[dict[str, Any]],
) -> None:
    sla = MagicMock(task_id="t1")
    sla2 = MagicMock(task_id="t2")
    dag = MagicMock(dag_id="dag1")
    cb = cb_mod.make_sla_miss_callback(AlertsConfig(on_sla_miss=True), "pipe")
    cb(dag, [], [], [sla, sla2], [])
    assert len(patch_post_sla) == 1
    assert patch_post_sla[0]["task_ids"] == ["t1", "t2"]


def test_sla_miss_callback_skipped_when_disabled(
    patch_post_sla: list[dict[str, Any]],
) -> None:
    cb = cb_mod.make_sla_miss_callback(AlertsConfig(on_sla_miss=False), "pipe")
    cb(MagicMock(dag_id="d"), [], [], [MagicMock(task_id="t")], [])
    assert patch_post_sla == []


def test_schema_change_probe_no_trace_returns_none(
    monkeypatch: pytest.MonkeyPatch, patch_post_schema: list[dict[str, Any]]
) -> None:
    class FakeDlt:
        @staticmethod
        def pipeline(pipeline_name: str) -> MagicMock:
            return MagicMock(last_trace=None)

    monkeypatch.setitem(__import__("sys").modules, "dlt", FakeDlt)
    cb_mod.schema_change_probe(AlertsConfig(), "pipe")
    assert patch_post_schema == []


def test_schema_change_probe_fires_when_updates_present(
    monkeypatch: pytest.MonkeyPatch, patch_post_schema: list[dict[str, Any]]
) -> None:
    step = MagicMock()
    step.step = "normalize"
    step.step_info = MagicMock(table_metrics={"orders": object()}, tables_with_new_columns=None)
    trace = MagicMock(steps=[step])

    class FakeDlt:
        @staticmethod
        def pipeline(pipeline_name: str) -> MagicMock:
            return MagicMock(last_trace=trace)

    monkeypatch.setitem(__import__("sys").modules, "dlt", FakeDlt)
    cb_mod.schema_change_probe(AlertsConfig(), "pipe", task_instance=_fake_ti())
    assert len(patch_post_schema) == 1
    assert patch_post_schema[0]["pipeline_name"] == "pipe"
    assert patch_post_schema[0]["schema_updates"]


def test_schema_change_probe_disabled_short_circuits(
    monkeypatch: pytest.MonkeyPatch, patch_post_schema: list[dict[str, Any]]
) -> None:
    sentinel = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "dlt", sentinel)
    cb_mod.schema_change_probe(AlertsConfig(on_schema_change=False), "pipe")
    sentinel.pipeline.assert_not_called()
    assert patch_post_schema == []


def test_callback_accepts_alerts_dict() -> None:
    # functools.partial must serialize via model_dump(mode="json")
    cb = cb_mod.make_on_failure_callback(
        AlertsConfig(severity=AlertSeverity.P1, slack_channel="#x"), "pipe"
    )
    # The captured alerts_dict must round-trip through model_validate.
    assert cb.keywords["alerts_dict"]["severity"] == "P1"
    assert cb.keywords["alerts_dict"]["slack_channel"] == "#x"
    AlertsConfig.model_validate(cb.keywords["alerts_dict"])
