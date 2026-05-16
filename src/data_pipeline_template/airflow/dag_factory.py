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
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from dlt.helpers.airflow_helper import PipelineTasksGroup

from data_pipeline_template.config.models import PipelineConfig, ResourcesConfig
from data_pipeline_template.pipeline_factory import build as build_pipeline

# Stable past anchor so Airflow's scheduler computes a deterministic first run.
# catchup=False on the DAG suppresses backfill of intervening intervals.
_START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

_DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


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
        tg.add_run(
            runnable.pipeline,
            runnable.source,
            decompose="serialize",
            write_disposition=runnable.write_disposition.value,
            schema_contract=cfg.options.schema_contract.value,
            **task_kwargs,
        )

    return dag
