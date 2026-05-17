# `observability/` — alerts + log scrubbing

Failure routing and credential scrubbing for the alerting / logging path.
Orchestrator-agnostic (no `import airflow`) — Airflow plumbs in via
[`airflow/callbacks.py`](../airflow/callbacks.py) and
[`airflow/dag_factory.py`](../airflow/dag_factory.py).

## Files

- [`alerts.py`](alerts.py) — Slack webhook + SMTP dispatch with severity
  routing, dedup, and per-event payload builders.
- [`log_filter.py`](log_filter.py) — `SecretScrubFilter`, a
  `logging.Filter` that rewrites obvious credential patterns to `***`.

## Alerts ([`alerts.py`](alerts.py))

Public dispatch functions:

- `post_failure_alert` — wired into `on_failure_callback` from
  `dag_factory`. Routes per `AlertsConfig.severity` (`P1` → P1 Slack
  channel + paging email list; `P2` → low-pri channel).
- `post_schema_change_alert` — info-level. Hooked into dlt's
  schema-evolution event surface so an unexpected column add fires a
  notification without failing the run (Segment 14).
- `post_heartbeat_alert` — used by
  [`dags/heartbeat_check.py`](../../../dags/heartbeat_check.py) when
  the scheduler heartbeat goes stale.
- `post_sla_miss_alert` — wired into the DAG-level `sla_miss_callback`
  built by `make_sla_miss_callback`. Migration path to Airflow 3.x
  `deadline` API documented in [`airflow/README.md`](../airflow/README.md).

YAML knobs (`AlertsConfig` in [`config/models.py`](../config/models.py)):

```yaml
alerts:
  severity: P1                  # P1 | P2
  dedup_window_minutes: 30      # suppress repeats within window
  slack_channel: "#data-alerts" # optional override
  email_recipients: ["oncall@example.com"]
```

Slack webhook URL + SMTP creds resolve from env vars
(`SLACK_WEBHOOK_URL`, `SMTP_HOST` / `SMTP_USER` / `SMTP_PASSWORD`) so
they never appear in YAML.

## Secret scrubbing ([`log_filter.py`](log_filter.py))

`install_secret_scrub()` installs a `logging.Filter` on the root logger
plus `dlt` / `airflow` / `airflow.task` named loggers. Patterns covered:

- `password=…` / `token=…` / `secret=…` / `api_key=…` style kwargs.
- URI `userinfo` components (`scheme://user:pass@host`).
- AWS `aws_access_key_id` / `aws_secret_access_key` style kwargs.

Install sites (no path bypasses the filter):

- [`dags/data_pipeline_dags.py`](../../../dags/data_pipeline_dags.py) —
  DagBag-parse time, covers scheduler + worker pod processes.
- [`src/dlt_data_pipeline/__main__.py:main`](../__main__.py) — CLI entry,
  covers `python -m dlt_data_pipeline …` invocations (including
  KubernetesExecutor per-task pods).
- [`dags/heartbeat_check.py`](../../../dags/heartbeat_check.py) — the
  standalone health DAG installs it at import time so heartbeat alerts
  don't leak DB URIs in failure tracebacks.

Idempotent — repeated calls add the filter once per logger via an
`isinstance` check on `logger.filters`.

## Known issues / troubleshooting

> **Secret leakage** — context for the log-scrubbing work scoped under
> Segment 12: dlt can echo credentials in logs/tracebacks; Airflow
> renders task templates in logs (use `Connection`/`Variable` objects so
> the UI mask filter catches them); CDC needs a dedicated source DB user
> with `REPLICATION` (least privilege), not a superuser.
