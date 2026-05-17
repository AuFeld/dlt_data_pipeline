"""Row-count check tasks attach to the generated DAG (Segment 12).

Lightweight DAG-build assertion — the heavy live-Postgres path lives in
the integration suite. Forces `cross_cluster` mode so we don't need an
Airflow Connection registered to build the task.
"""

from __future__ import annotations

from typing import Any

import pytest

from dlt_data_pipeline.airflow.dag_factory import build_dag
from dlt_data_pipeline.config.models import (
    DestinationConfig,
    DestinationType,
    OptionsConfig,
    PipelineConfig,
    QualityCheckMode,
    QualityConfig,
    ScheduleConfig,
    SourceSqlDatabase,
    SyncConfig,
    SyncMode,
    WriteDisposition,
)

_SRC_ENV = "SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS"
_DST_ENV = "DESTINATION__PG_WAREHOUSE__CREDENTIALS"


@pytest.fixture
def _creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_SRC_ENV, "postgresql://u:p@source:5432/db")
    monkeypatch.setenv(_DST_ENV, "postgresql://u:p@dest:5432/db")
    yield
    # SQLAlchemy engines aren't created at DAG-parse time, so no teardown needed.


def _cfg(
    row_count: bool, mode: QualityCheckMode = QualityCheckMode.cross_cluster
) -> PipelineConfig:
    return PipelineConfig(
        name="rc_demo",
        source=SourceSqlDatabase(
            type="sql_database",
            connection="pg_source",
            config={"schema": "public", "tables": ["orders", "customers"]},
        ),
        sync=SyncConfig(mode=SyncMode.incremental, cursor_field="updated_at", primary_key="id"),
        destination=DestinationConfig(
            type=DestinationType.postgres, connection="pg_warehouse", dataset="raw"
        ),
        schedule=ScheduleConfig(cron="0 * * * *", enabled=True),
        options=OptionsConfig(write_disposition=WriteDisposition.merge),
        quality=QualityConfig(row_count_check=row_count, check_mode=mode),
    )


def _short_ids(dag: Any) -> set[str]:
    """Strip the TaskGroup prefix so assertions read like the operator definitions."""
    return {t.task_id.rsplit(".", 1)[-1] for t in dag.tasks}


def test_row_count_check_disabled_by_default(_creds: None) -> None:
    dag = build_dag(_cfg(row_count=False))
    assert not any("row_count_check" in t.task_id for t in dag.tasks)


def test_row_count_check_creates_one_task_per_table(_creds: None) -> None:
    dag = build_dag(_cfg(row_count=True))
    short = _short_ids(dag)
    assert "row_count_check_orders" in short
    assert "row_count_check_customers" in short


def _find(dag: Any, short_id: str) -> Any:
    return next(t for t in dag.tasks if t.task_id.endswith(f".{short_id}") or t.task_id == short_id)


def test_row_count_check_runs_downstream_of_emit_dataset(_creds: None) -> None:
    dag = build_dag(_cfg(row_count=True))
    rc = _find(dag, "row_count_check_orders")
    upstream_short = {u.task_id.rsplit(".", 1)[-1] for u in rc.upstream_list}
    assert "emit_dataset" in upstream_short


def test_cross_cluster_uses_python_operator(_creds: None) -> None:
    from airflow.operators.python import PythonOperator

    dag = build_dag(_cfg(row_count=True))
    rc = _find(dag, "row_count_check_orders")
    assert isinstance(rc, PythonOperator)


def test_same_cluster_uses_sql_check_operator(_creds: None) -> None:
    from airflow.providers.common.sql.operators.sql import SQLCheckOperator

    dag = build_dag(_cfg(row_count=True, mode=QualityCheckMode.same_cluster))
    rc = _find(dag, "row_count_check_orders")
    assert isinstance(rc, SQLCheckOperator)
    assert rc.conn_id == "dlt_pg_warehouse"
