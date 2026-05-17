"""Unit tests for the standalone scheduler heartbeat DAG (Segment 14)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "dags"
HEARTBEAT_DAG_FILE = DAGS_DIR / "heartbeat_check.py"


def _import_heartbeat_module() -> Any:
    """Import dags/heartbeat_check.py as a module without going through DagBag."""
    if "heartbeat_check_under_test" in sys.modules:
        del sys.modules["heartbeat_check_under_test"]
    spec = importlib.util.spec_from_file_location("heartbeat_check_under_test", HEARTBEAT_DAG_FILE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dagbag_parses_heartbeat_dag() -> None:
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert "heartbeat_check" in bag.dag_ids


def test_heartbeat_dag_shape() -> None:
    # ``bag.dags`` is the in-memory DagBag cache populated by file parsing.
    # ``bag.get_dag`` falls through to a metadata-DB lookup, which fails in
    # CI where Airflow's SQLite DB is unmigrated.
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert "heartbeat_check" in bag.dags
    dag = bag.dags["heartbeat_check"]
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert "health" in dag.tags
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["check_scheduler_heartbeat"]


def test_heartbeat_alert_fires_when_scheduler_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _import_heartbeat_module()
    calls: list[dict[str, Any]] = []

    def fake_post(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return MagicMock()

    monkeypatch.setattr(mod, "post_heartbeat_alert", fake_post)

    stale_ts = datetime(2026, 5, 17, 6, 0, 0, tzinfo=UTC)
    now = stale_ts + timedelta(minutes=10)

    fake_job = SimpleNamespace(latest_heartbeat=stale_ts, job_type="SchedulerJob")
    _install_session_query_stub(monkeypatch, returns=fake_job)
    _install_utcnow_stub(monkeypatch, now=now)

    mod._check_scheduler_heartbeat()

    assert len(calls) == 1
    assert calls[0]["component"] == "scheduler"
    assert calls[0]["last_seen"] == stale_ts
    assert calls[0]["threshold_seconds"] == mod._THRESHOLD_SECONDS


def test_heartbeat_no_alert_when_scheduler_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _import_heartbeat_module()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(mod, "post_heartbeat_alert", lambda **kw: calls.append(kw))

    fresh_ts = datetime(2026, 5, 17, 6, 0, 0, tzinfo=UTC)
    now = fresh_ts + timedelta(seconds=10)

    fake_job = SimpleNamespace(latest_heartbeat=fresh_ts, job_type="SchedulerJob")
    _install_session_query_stub(monkeypatch, returns=fake_job)
    _install_utcnow_stub(monkeypatch, now=now)

    mod._check_scheduler_heartbeat()
    assert calls == []


def test_heartbeat_alert_fires_when_no_job_row(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _import_heartbeat_module()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(mod, "post_heartbeat_alert", lambda **kw: calls.append(kw))

    _install_session_query_stub(monkeypatch, returns=None)
    _install_utcnow_stub(monkeypatch, now=datetime(2026, 5, 17, 6, 0, 0, tzinfo=UTC))

    mod._check_scheduler_heartbeat()
    assert len(calls) == 1
    assert calls[0]["last_seen"] is None


# --------------------------------------------------------------------------- #
# Helpers to stub the lazy airflow imports inside _check_scheduler_heartbeat. #
# --------------------------------------------------------------------------- #


def _install_session_query_stub(monkeypatch: pytest.MonkeyPatch, *, returns: Any) -> None:
    """Stub ``airflow.utils.session.create_session`` to return a fake session
    whose ``query(...).filter(...).order_by(...).first()`` returns ``returns``.
    """
    chain = MagicMock()
    chain.filter.return_value.order_by.return_value.first.return_value = returns

    fake_session = MagicMock()
    fake_session.query.return_value = chain

    cm = MagicMock()
    cm.__enter__ = lambda self: fake_session
    cm.__exit__ = lambda self, *exc: False

    from airflow.utils import session as session_mod

    monkeypatch.setattr(session_mod, "create_session", lambda: cm)


def _install_utcnow_stub(monkeypatch: pytest.MonkeyPatch, *, now: datetime) -> None:
    from airflow.utils import timezone as tz_mod

    monkeypatch.setattr(tz_mod, "utcnow", lambda: now)
