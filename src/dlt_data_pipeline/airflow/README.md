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
