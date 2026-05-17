"""Pipeline failure / schema-change / SLA-miss alerting.

Transports:
    Slack: stdlib ``urllib.request`` (zero new deps). Webhook URL + per-severity
        default channel come from env vars listed below.
    Email: lazy-imported ``airflow.utils.email.send_email_smtp`` so this module
        stays importable from non-Airflow contexts (tests, CLI). Reuses
        whatever SMTP config Airflow already has (``AIRFLOW__SMTP__*``).

Env contract:
    DPT_ALERTS_SLACK_WEBHOOK_URL   Incoming webhook URL (required for Slack)
    DPT_ALERTS_SLACK_CHANNEL_P1    Default channel for severity=P1
    DPT_ALERTS_SLACK_CHANNEL_P2    Default channel for severity=P2
    DPT_ALERTS_SLACK_CHANNEL_INFO  Default channel for severity=info
    DPT_ALERTS_EMAIL_FROM          From address for SMTP

Channel resolution order:
    AlertsConfig.slack_channel (YAML override)
        -> env channel for severity
        -> omitted from payload (webhook default channel applies)

Dedup:
    Airflow Variables keyed on pipeline+severity+kind. Outside an Airflow
    context (no metadata DB reachable), dedup falls back to no-op.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from data_pipeline_template.config.models import AlertsConfig, AlertSeverity

log = logging.getLogger(__name__)

_SLACK_TIMEOUT_SECONDS = 5
_DEDUP_VARIABLE_PREFIX = "dpt_alert_dedup"

_SEVERITY_CHANNEL_ENV = {
    AlertSeverity.P1: "DPT_ALERTS_SLACK_CHANNEL_P1",
    AlertSeverity.P2: "DPT_ALERTS_SLACK_CHANNEL_P2",
    AlertSeverity.info: "DPT_ALERTS_SLACK_CHANNEL_INFO",
}


@dataclass(frozen=True)
class AlertDispatchResult:
    slack_posted: bool
    email_sent: bool
    deduped: bool
    reason: str | None = None


# --------------------------------------------------------------------------- #
# Dedup backend (Airflow Variable). Module-level callables so tests can swap. #
# --------------------------------------------------------------------------- #


def _dedup_get(key: str) -> str | None:
    try:
        from airflow.models import Variable

        value = Variable.get(key, default_var=None)
        return str(value) if value is not None else None
    except Exception:
        return None


def _dedup_set(key: str, value: str) -> None:
    try:
        from airflow.models import Variable

        Variable.set(key, value)
    except Exception:
        return


def _dedup_check_and_mark(
    pipeline_name: str, severity: AlertSeverity, kind: str, window_minutes: int
) -> bool:
    """Return True if a prior alert sits inside the dedup window."""
    if window_minutes <= 0:
        return False
    key = f"{_DEDUP_VARIABLE_PREFIX}:{pipeline_name}:{severity.value}:{kind}"
    now = datetime.now(UTC)
    prior = _dedup_get(key)
    if prior is not None:
        try:
            prior_dt = datetime.fromisoformat(prior)
        except ValueError:
            prior_dt = None
        if prior_dt is not None and (now - prior_dt) < timedelta(minutes=window_minutes):
            return True
    _dedup_set(key, now.isoformat())
    return False


# --------------------------------------------------------------------------- #
# Slack + email transports.                                                   #
# --------------------------------------------------------------------------- #


def _resolve_slack_channel(alerts: AlertsConfig) -> str | None:
    if alerts.slack_channel:
        return alerts.slack_channel
    env_key = _SEVERITY_CHANNEL_ENV.get(alerts.severity)
    if env_key is None:
        return None
    value = os.environ.get(env_key)
    return value or None


def _post_slack(payload: dict[str, Any]) -> tuple[bool, str | None]:
    webhook_url = os.environ.get("DPT_ALERTS_SLACK_WEBHOOK_URL")
    if not webhook_url:
        return False, "slack-webhook-unset"
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=_SLACK_TIMEOUT_SECONDS)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        log.warning("slack post failed: %r", exc)
        return False, f"slack-error:{type(exc).__name__}"
    return True, None


def _send_email(*, subject: str, html_body: str, recipients: list[str]) -> tuple[bool, str | None]:
    if not recipients:
        return False, "email-recipients-empty"
    try:
        from airflow.utils.email import send_email_smtp
    except Exception as exc:
        log.warning("airflow email helper unavailable: %r", exc)
        return False, "email-airflow-unavailable"
    try:
        send_email_smtp(recipients, subject, html_body)
    except Exception as exc:
        log.warning("smtp send failed: %r", exc)
        return False, f"email-error:{type(exc).__name__}"
    return True, None


# --------------------------------------------------------------------------- #
# Payload builders.                                                           #
# --------------------------------------------------------------------------- #


def _failure_payload(
    *,
    pipeline_name: str,
    severity: AlertSeverity,
    dag_id: str,
    task_id: str,
    run_id: str,
    log_url: str | None,
    exception_summary: str,
    channel: str | None,
) -> dict[str, Any]:
    text = f"[{severity.value}] pipeline `{pipeline_name}` failed"
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": text}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:* `{task_id}`"},
                {"type": "mrkdwn", "text": f"*Run:* `{run_id}`"},
                {"type": "mrkdwn", "text": f"*Severity:* {severity.value}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{exception_summary}```"},
        },
    ]
    if log_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View logs"},
                        "url": log_url,
                    }
                ],
            }
        )
    payload: dict[str, Any] = {
        "username": "dpt-alerts",
        "text": text,
        "blocks": blocks,
    }
    if channel:
        payload["channel"] = channel
    return payload


def _schema_change_payload(
    *,
    pipeline_name: str,
    dag_id: str,
    run_id: str,
    schema_updates: list[dict[str, Any]],
    channel: str | None,
) -> dict[str, Any]:
    text = f"[info] schema evolution in `{pipeline_name}`"
    truncated = schema_updates[:20]
    summary = "\n".join(json.dumps(u, sort_keys=True) for u in truncated)
    if len(schema_updates) > 20:
        summary += f"\n… (+{len(schema_updates) - 20} more)"
    payload: dict[str, Any] = {
        "username": "dpt-alerts",
        "text": text,
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": text}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Run:* `{run_id}`"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```{summary}```"}},
        ],
    }
    if channel:
        payload["channel"] = channel
    return payload


def _sla_miss_payload(
    *,
    pipeline_name: str,
    dag_id: str,
    task_ids: list[str],
    severity: AlertSeverity,
    channel: str | None,
) -> dict[str, Any]:
    text = f"[{severity.value}] SLA miss in `{pipeline_name}`"
    payload: dict[str, Any] = {
        "username": "dpt-alerts",
        "text": text,
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": text}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Tasks:* `{', '.join(task_ids) or '-'}`"},
                ],
            },
        ],
    }
    if channel:
        payload["channel"] = channel
    return payload


# --------------------------------------------------------------------------- #
# Public API.                                                                 #
# --------------------------------------------------------------------------- #


def post_failure_alert(
    *,
    pipeline_name: str,
    severity: AlertSeverity,
    dag_id: str,
    task_id: str,
    run_id: str,
    log_url: str | None,
    exception_summary: str,
    alerts: AlertsConfig,
) -> AlertDispatchResult:
    if _dedup_check_and_mark(pipeline_name, severity, "failure", alerts.dedup_window_minutes):
        return AlertDispatchResult(False, False, True, "dedup-window")
    channel = _resolve_slack_channel(alerts)
    payload = _failure_payload(
        pipeline_name=pipeline_name,
        severity=severity,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        log_url=log_url,
        exception_summary=exception_summary,
        channel=channel,
    )
    slack_ok, slack_reason = _post_slack(payload)
    subject = f"[{severity.value}] pipeline {pipeline_name} failed"
    html = (
        f"<p><b>{subject}</b></p>"
        f"<p>DAG: <code>{dag_id}</code><br/>"
        f"Task: <code>{task_id}</code><br/>"
        f"Run: <code>{run_id}</code></p>"
        f"<pre>{exception_summary}</pre>"
    )
    email_ok, email_reason = _send_email(
        subject=subject, html_body=html, recipients=alerts.email_recipients
    )
    reason = slack_reason or email_reason
    return AlertDispatchResult(slack_ok, email_ok, False, reason)


def post_schema_change_alert(
    *,
    pipeline_name: str,
    dag_id: str,
    run_id: str,
    schema_updates: list[dict[str, Any]],
    alerts: AlertsConfig,
) -> AlertDispatchResult:
    if not schema_updates:
        return AlertDispatchResult(False, False, False, "no-updates")
    # Schema events always route on the info channel, ignoring severity overrides.
    info_alerts = AlertsConfig(
        severity=AlertSeverity.info,
        dedup_window_minutes=alerts.dedup_window_minutes,
        slack_channel=alerts.slack_channel,
        email_recipients=alerts.email_recipients,
    )
    if _dedup_check_and_mark(
        pipeline_name, AlertSeverity.info, "schema", alerts.dedup_window_minutes
    ):
        return AlertDispatchResult(False, False, True, "dedup-window")
    channel = _resolve_slack_channel(info_alerts)
    payload = _schema_change_payload(
        pipeline_name=pipeline_name,
        dag_id=dag_id,
        run_id=run_id,
        schema_updates=schema_updates,
        channel=channel,
    )
    slack_ok, slack_reason = _post_slack(payload)
    return AlertDispatchResult(slack_ok, False, False, slack_reason)


def post_sla_miss_alert(
    *,
    pipeline_name: str,
    dag_id: str,
    task_ids: list[str],
    alerts: AlertsConfig,
) -> AlertDispatchResult:
    if _dedup_check_and_mark(pipeline_name, alerts.severity, "sla", alerts.dedup_window_minutes):
        return AlertDispatchResult(False, False, True, "dedup-window")
    channel = _resolve_slack_channel(alerts)
    payload = _sla_miss_payload(
        pipeline_name=pipeline_name,
        dag_id=dag_id,
        task_ids=task_ids,
        severity=alerts.severity,
        channel=channel,
    )
    slack_ok, slack_reason = _post_slack(payload)
    subject = f"[{alerts.severity.value}] SLA miss in {pipeline_name}"
    html = (
        f"<p><b>{subject}</b></p>"
        f"<p>DAG: <code>{dag_id}</code><br/>"
        f"Tasks: <code>{', '.join(task_ids) or '-'}</code></p>"
    )
    email_ok, email_reason = _send_email(
        subject=subject, html_body=html, recipients=alerts.email_recipients
    )
    return AlertDispatchResult(slack_ok, email_ok, False, slack_reason or email_reason)
