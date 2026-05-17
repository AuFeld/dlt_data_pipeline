"""YAML config -> Airflow DAG wrapping a dlt ``PipelineTasksGroup``.

One ``DAG`` per ``PipelineConfig``. One Airflow task per dlt resource
connected-component via ``decompose="serialize"`` (sequential, ordered) — keeps
concurrent writes off shared ``_dlt_*`` state tables.

The pipeline core stays orchestrator-agnostic: this module imports airflow,
``pipeline_factory.build`` does not. Ruff ``TID253`` allows airflow imports
only here, under ``dags/``, and under ``tests/``.

Pod resources from ``cfg.resources`` flow into
``executor_config={"pod_override": V1Pod(...)}`` per task. LocalExecutor
ignores it; KubernetesExecutor (Segment 10) honors it.

Trailing tasks added inside the ``PipelineTasksGroup``:
  * ``schema_change_probe`` — reads ``dlt.pipeline.last_trace`` and emits
    an info-level alert if schema deltas appeared. Opt-out via
    ``alerts.on_schema_change=false``.
  * ``emit_dataset`` — no-op carrier of a ``Dataset("dlt://<name>")``
    outlet so downstream pipelines can subscribe via ``schedule=[Dataset(...)]``.

``Dataset`` lives at ``airflow.datasets`` for Airflow 2.4+; moves to
``airflow.sdk.Dataset`` in 3.x (Segment 10 follow-up).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from dlt.helpers.airflow_helper import PipelineTasksGroup

from dlt_data_pipeline.airflow.callbacks import (
    make_on_failure_callback,
    make_sla_miss_callback,
    schema_change_probe,
)
from dlt_data_pipeline.airflow.quality import build_row_count_tasks
from dlt_data_pipeline.config.models import PipelineConfig, ResourcesConfig
from dlt_data_pipeline.pipeline_factory import build as build_pipeline

# Stable past anchor so Airflow's scheduler computes a deterministic first run.
# catchup=False on the DAG suppresses backfill of intervening intervals.
_START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

_DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def _emit_noop(**_: Any) -> None:
    """No-op PythonOperator callable; carries the Dataset outlet."""
    return None


def _pod_override(res: ResourcesConfig) -> Any:
    """Build a ``V1Pod`` carrying CPU/RAM requests=limits for ``pod_override``.

    Imported lazily so a missing ``kubernetes`` install can't break DAG
    parsing in environments that don't use ``KubernetesExecutor``.
    """
    from kubernetes.client import (  # noqa: PLC0415
        V1Container,
        V1Pod,
        V1PodSpec,
        V1ResourceRequirements,
    )

    qty = {k: v for k, v in (("cpu", res.cpu), ("memory", res.memory)) if v}
    return V1Pod(
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="base",
                    resources=V1ResourceRequirements(requests=qty, limits=qty),
                )
            ]
        )
    )


def build_dag(cfg: PipelineConfig) -> DAG:
    """Translate a ``PipelineConfig`` into a runnable Airflow ``DAG``."""
    task_kwargs: dict[str, Any] = {}
    if cfg.resources.cpu or cfg.resources.memory:
        task_kwargs["executor_config"] = {"pod_override": _pod_override(cfg.resources)}
    if cfg.sync.sla_minutes is not None:
        # Airflow's sla_miss_callback only fires for tasks that carry an
        # ``sla`` attribute, so plumb the YAML value through here. The
        # callback itself is wired below via make_sla_miss_callback.
        task_kwargs["sla"] = timedelta(minutes=cfg.sync.sla_minutes)

    on_failure = make_on_failure_callback(cfg.alerts, cfg.name)
    on_sla_miss = make_sla_miss_callback(cfg.alerts, cfg.name)

    dag = DAG(
        dag_id=cfg.name,
        description=f"{cfg.source.type} -> {cfg.destination.type} ({cfg.sync.mode.value})",
        schedule=cfg.schedule.cron,
        start_date=_START_DATE,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=not cfg.schedule.enabled,
        tags=[cfg.source.type, cfg.destination.type, cfg.sync.mode.value],
        default_args=_DEFAULT_ARGS,
        on_failure_callback=on_failure,  # type: ignore[arg-type]
        sla_miss_callback=on_sla_miss,
    )

    with dag:
        # PipelineTasksGroup MUST be constructed before the dlt pipeline:
        # its __init__ sets ``DLT_DATA_DIR`` env to a temp dir, and dlt
        # rejects pipelines created with a ``pipelines_dir`` that doesn't
        # match. pipeline_factory.build() respects DLT_DATA_DIR when set.
        tg = PipelineTasksGroup(
            pipeline_name=cfg.name,
            use_data_folder=False,
            wipe_local_data=True,
            use_task_logger=True,
        )
        runnable = build_pipeline(cfg)
        with tg:
            tg.add_run(
                runnable.pipeline,
                runnable.source,
                decompose="serialize",
                write_disposition=runnable.write_disposition.value,
                schema_contract=cfg.options.schema_contract.value,
                **task_kwargs,
            )
            leaves = [t for t in tg.children.values() if not getattr(t, "downstream_list", [])]

            trailing: list[Any] = []
            if cfg.alerts.on_schema_change:
                probe = PythonOperator(
                    task_id="schema_change_probe",
                    python_callable=schema_change_probe,
                    op_kwargs={
                        "alerts": cfg.alerts.model_dump(mode="json"),
                        "pipeline_name": cfg.name,
                    },
                    **task_kwargs,
                )
                trailing.append(probe)

            emit_dataset = PythonOperator(
                task_id="emit_dataset",
                python_callable=_emit_noop,
                outlets=[Dataset(f"dlt://{cfg.name}")],
                **task_kwargs,
            )
            trailing.append(emit_dataset)

            # Row-count reconciliation tasks fan out in parallel after
            # emit_dataset (one per replicated table). They're a quality gate,
            # not part of the dataset-event chain — running them after the
            # outlet keeps downstream-dataset scheduling unblocked by check
            # latency.
            row_count_tasks = build_row_count_tasks(cfg, task_kwargs)

            for idx, t in enumerate(trailing):
                upstream = leaves if idx == 0 else [trailing[idx - 1]]
                for up in upstream:
                    up >> t

            for rc in row_count_tasks:
                emit_dataset >> rc

    return dag
