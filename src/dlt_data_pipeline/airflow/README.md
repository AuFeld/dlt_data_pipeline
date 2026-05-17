# `dlt_data_pipeline.airflow`

Airflow-specific glue. Only this subpackage and `dags/` may `import airflow`
(enforced by ruff `flake8-tidy-imports`). `pipeline_factory` and everything
under `sources/`, `destinations/`, `config/`, `cli/` stay orchestrator-agnostic
(Design principle #2 in `data_pipeline_plan.md`).

## Modules

- `dag_factory.py` — `PipelineConfig` → `DAG` wrapping `PipelineTasksGroup`.
- `callbacks.py` — `on_failure_callback`, `sla_miss_callback`,
  `schema_change_probe`. All route to `observability.alerts`.
- `quality.py` — row-count reconciliation tasks (Segment 12,
  `quality.row_count_check`).
- `sensors.py` — `PgReplicationSlotLagSensor` (Segment 7, opt-in;
  wire into a separate monitoring DAG).

## SLA migration path (Airflow 2.x → 3.x)

The pipeline YAML exposes one SLA knob:

```yaml
sync:
  sla_minutes: 30
```

`dag_factory` plumbs it into every task generated inside the
`PipelineTasksGroup` as `sla=timedelta(minutes=N)`. Breaches invoke the
DAG-level `sla_miss_callback` built by `make_sla_miss_callback`, which
routes to `observability.alerts.post_sla_miss_alert`.

**v1 (current, Airflow 2.x):**

- Per-task `sla` attribute + `DAG.sla_miss_callback`. Airflow's scheduler
  checks SLAs on a heartbeat cadence and invokes the callback once per
  miss with `(dag, task_list, blocking_task_list, slas, blocking_tis)`.

**v2 (Airflow 3.x):**

- Airflow 3.x deprecates the `sla` task attribute and `sla_miss_callback`
  in favor of the `deadline` API: per-task `deadline=timedelta(...)`
  declaratively bound, with callbacks dispatched through the Deadline
  framework rather than the scheduler heartbeat path.
- The YAML field stays the same — `sync.sla_minutes` is orchestrator-
  agnostic. Only `dag_factory.build_dag` and `callbacks.make_sla_miss_callback`
  change: swap `sla=timedelta(...)` for `deadline=...` on the task
  kwargs and adapt the callback signature. No pipeline YAML edits needed.

Tracking: bump when the project pins Airflow 3.x in `pyproject.toml`. The
plan's Segment 10 KubernetesExecutor work calls out the same `Dataset →
sdk.Dataset` module move; SLA migration lands alongside.

## Scheduler heartbeat DAG (Segment 14)

Lives at [`dags/heartbeat_check.py`](../../../dags/heartbeat_check.py) —
outside the YAML-generated DagBag entry because it monitors Airflow itself,
not a user pipeline.

- Runs every 5 minutes (`schedule="*/5 * * * *"`, `max_active_runs=1`).
- Queries `airflow.jobs.job.Job` for the most recent `SchedulerJob` row;
  fires a P1 alert via `observability.alerts.post_heartbeat_alert` when
  `latest_heartbeat` is older than 60s (= 2× Airflow's default
  `scheduler_health_check_threshold` — encodes "two consecutive misses"
  without a separate counter).
- Dedup window: 30min, so a multi-hour outage produces ~2 alerts/hr while
  the 5-min schedule still catches recovery promptly.
- No cross-run state — the `Job.latest_heartbeat` column is the source of
  truth. Lazy-imports Airflow internals inside the task callable so a
  broken install fails this DAG only, not every YAML-generated DAG.

## Known issues / troubleshooting

> **State persistence across containers:** dlt stores incremental cursors +
> schema in the *destination* dataset (`_dlt_*` tables) — durable, survives
> restarts. Put local `~/.dlt` working dir on a named volume too. Point
> Airflow's metadata DB at the persistent `postgres-airflow` service (not the
> default SQLite). Prevent overlapping runs of the same pipeline (cursor
> corruption) via `max_active_runs=1` on the generated `DAG`, plus a
> per-pipeline `pool` if a single pipeline can fan out into concurrent task
> instances.

> **Monitoring:** Airflow gives run-level observability via task instance
> state + DAG run state. Add `on_failure_callback` and freshness via
> `Dataset`/`DatasetEvent` (catch silently-not-running schedules). Surface
> dlt load metrics into XCom for the UI. (CDC slot-lag monitoring — the
> other half of this Tricky Part — lives in
> [`sources/README.md`](../sources/README.md).)

> **dlt Airflow helper caveats:** `PipelineTasksGroup` runs each dlt resource
> in a separate Airflow task; ensure `pipeline_factory` returns a
> `dlt.Pipeline` whose `source()` exposes resources discoverable by the
> helper. Document this contract in `pipeline_factory.py`.
