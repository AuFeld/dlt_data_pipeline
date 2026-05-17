"""Standalone health-check DAG: alert when SchedulerJob heartbeat goes stale.

Runs every 5 minutes. Queries ``airflow.jobs.job.Job`` for the most recent
``SchedulerJob`` row and fires a P1 alert via
``observability.alerts.post_heartbeat_alert`` when ``latest_heartbeat`` is
older than ``_THRESHOLD_SECONDS`` (default 60s = 2 * Airflow's default
``scheduler_health_check_threshold``).

State is deliberately not tracked across runs — the Job table's own
``latest_heartbeat`` column is the source of truth for "two consecutive
misses" (one stale-threshold = one miss; doubled = two). Dedup on the
``AlertsConfig.dedup_window_minutes`` axis prevents the 5-minute schedule
from spamming during a sustained outage.

Lives outside the YAML-generated DagBag entry (``data_pipeline_dags.py``)
because it monitors Airflow itself, not a user pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from dlt_data_pipeline.config.models import AlertsConfig, AlertSeverity
from dlt_data_pipeline.observability.alerts import post_heartbeat_alert
from dlt_data_pipeline.observability.log_filter import install_secret_scrub

install_secret_scrub()

log = logging.getLogger(__name__)

# Default Airflow scheduler_health_check_threshold is 30s; alerting at 2x
# encodes "two consecutive heartbeat misses" without a separate counter.
_THRESHOLD_SECONDS = 60

# P1 with 30-min dedup so a multi-hour outage produces at most ~2 alerts/hour
# while the 5-min schedule still catches recovery promptly.
_HEARTBEAT_ALERTS = AlertsConfig(
    severity=AlertSeverity.P1,
    dedup_window_minutes=30,
)

_START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")


def _check_scheduler_heartbeat(**_: Any) -> None:
    # Lazy imports so DagBag parse on a broken Airflow install still surfaces
    # the import error against this single DAG, not every YAML-generated DAG.
    from airflow.jobs.job import Job
    from airflow.utils.session import create_session
    from airflow.utils.timezone import utcnow

    with create_session() as session:
        job = (
            session.query(Job)
            .filter(Job.job_type == "SchedulerJob")
            .order_by(Job.latest_heartbeat.desc())
            .first()
        )

    last_seen = getattr(job, "latest_heartbeat", None) if job is not None else None
    if last_seen is None or (utcnow() - last_seen).total_seconds() > _THRESHOLD_SECONDS:
        post_heartbeat_alert(
            component="scheduler",
            last_seen=last_seen,
            threshold_seconds=_THRESHOLD_SECONDS,
            alerts=_HEARTBEAT_ALERTS,
        )


with DAG(
    dag_id="heartbeat_check",
    description="Alert when the Airflow scheduler heartbeat goes stale.",
    schedule="*/5 * * * *",
    start_date=_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["health"],
) as heartbeat_check:
    PythonOperator(
        task_id="check_scheduler_heartbeat",
        python_callable=_check_scheduler_heartbeat,
    )
