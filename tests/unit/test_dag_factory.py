"""Unit tests for the YAML -> Airflow DAG factory.

Builds DAGs in-process — no scheduler, no Airflow standalone, no DagBag scan.
Validates the contract between ``PipelineConfig`` and the generated ``DAG``:
dag id, schedule, max_active_runs, catchup, pause-on-creation, the inner
``PipelineTasksGroup``, and per-task ``executor_config`` for resources.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_pipeline_template.airflow.dag_factory import build_dag
from data_pipeline_template.config.loader import load_pipelines

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "pipelines_dag_factory"


@pytest.fixture(scope="module")
def configs() -> dict:
    return load_pipelines(FIXTURES)


def _tasks_for(dag) -> list:
    return list(dag.tasks)


def test_build_dag_basic(configs: dict) -> None:
    cfg = configs["basic_rest"]
    dag = build_dag(cfg)

    assert dag.dag_id == "basic_rest"
    # Airflow 2.x exposes the cron via schedule_interval on the DAG.
    assert dag.schedule_interval == cfg.schedule.cron
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert dag.is_paused_upon_creation is False
    assert set(dag.tags) >= {"rest_api", "duckdb", "full_refresh"}

    tasks = _tasks_for(dag)
    assert tasks, "expected at least one task in the generated TaskGroup"
    # PipelineTasksGroup uses cfg.name as the TaskGroup id; per-task task_ids
    # are nested under that prefix.
    assert all(t.task_id.startswith(f"{cfg.name}.") for t in tasks)


def test_build_dag_disabled_paused(configs: dict) -> None:
    cfg = configs["disabled_rest"]
    dag = build_dag(cfg)
    assert dag.is_paused_upon_creation is True


def test_build_dag_no_resources_no_pod_override(configs: dict) -> None:
    cfg = configs["basic_rest"]
    dag = build_dag(cfg)
    for task in _tasks_for(dag):
        # Airflow defaults executor_config to {}. Either an absent key or an
        # empty dict means no override was applied.
        assert "pod_override" not in (task.executor_config or {})


def test_build_dag_resources_pod_override(configs: dict) -> None:
    cfg = configs["delta_with_resources"]
    dag = build_dag(cfg)

    tasks = _tasks_for(dag)
    assert tasks
    for task in tasks:
        cfg_dict = task.executor_config or {}
        assert "pod_override" in cfg_dict, f"task {task.task_id} missing pod_override"
        pod = cfg_dict["pod_override"]
        container = pod.spec.containers[0]
        assert container.name == "base"
        assert container.resources.requests == {"cpu": "500m", "memory": "1Gi"}
        assert container.resources.limits == {"cpu": "500m", "memory": "1Gi"}
