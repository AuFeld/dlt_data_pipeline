"""Row-count reconciliation tasks (Segment 12, ``quality.row_count_check``).

Two backends, picked per pipeline by ``quality.check_mode``:

  * ``cross_cluster`` (default) — a ``PythonOperator`` opens two connections
    via ``dlt.secrets`` (source + destination) and compares ``COUNT(*)``
    per table. No Airflow Connection wiring required, which keeps the v1
    install pathless for shops that haven't onboarded their warehouse into
    Airflow's connection store yet.
  * ``same_cluster`` — a ``SQLCheckOperator`` runs one SQL boolean over one
    Airflow Connection that can see both source + destination schemas
    (single Postgres cluster, or Snowflake with grants spanning both
    databases). Cheaper, but requires an Airflow ``Connection`` named
    ``dlt_<destination.connection>``.

The factory `build_row_count_tasks` returns a list of Airflow tasks ready to
be attached downstream of the dlt PipelineTasksGroup in ``dag_factory``.
"""

from __future__ import annotations

from typing import Any

import dlt
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from sqlalchemy import create_engine, text

from dlt_data_pipeline.config.models import (
    PipelineConfig,
    QualityCheckMode,
    SourceConfig,
)


def _tables_for(source: SourceConfig) -> list[str]:
    """Tables under reconciliation per source type.

    Both ``sql_database`` and ``pg_cdc`` carry the list in ``source.config.tables``.
    Other source types are blocked upstream by ``_cross_field``.
    """
    tables = source.config.get("tables")
    if not isinstance(tables, list):
        raise ValueError(
            f"quality.row_count_check: source.config.tables must be a list, got {tables!r}"
        )
    return [str(t) for t in tables]


def _source_schema(source: SourceConfig) -> str:
    return str(source.config.get("schema", "public"))


def _source_connection_string(source: SourceConfig) -> str:
    """Resolve the source-side connection string via dlt.secrets.

    Keys follow the same logical-connection convention as the source
    builders: ``sources.<type>.<connection>.credentials``.
    """
    key = f"sources.{source.type}.{source.connection}.credentials"
    try:
        value = dlt.secrets[key]
    except Exception as exc:
        raise RuntimeError(
            f"quality.row_count_check: missing source credentials at {key!r}: {exc}"
        ) from exc
    return str(value)


def _count(engine: Any, schema: str, table: str) -> int:
    """``SELECT COUNT(*) FROM <schema>.<table>`` via a SQLAlchemy engine.

    Quote identifiers defensively. Both schema and table flow through the
    pydantic schema validator (they live in the YAML, not user input at task
    time), but the cost of quoting is one character per side.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"'))
        row = result.fetchone()
    if row is None:
        raise RuntimeError(f"COUNT(*) returned no rows for {schema}.{table}")
    return int(row[0])


def cross_cluster_row_count_check(
    source_conn_str: str,
    source_schema: str,
    dest_conn_str: str,
    dest_dataset: str,
    table: str,
) -> None:
    """PythonOperator callable: assert source row count == destination row count.

    Raises ``ValueError`` on mismatch, which fails the Airflow task. Connection
    strings come in pre-resolved by the wrapping operator's ``op_kwargs`` so
    this function can be unit-tested without touching dlt.secrets.
    """
    source_engine = create_engine(source_conn_str)
    dest_engine = create_engine(dest_conn_str)
    try:
        src = _count(source_engine, source_schema, table)
        dst = _count(dest_engine, dest_dataset, table)
    finally:
        source_engine.dispose()
        dest_engine.dispose()
    if src != dst:
        raise ValueError(f"row count mismatch for {table}: source={src} destination={dst}")


def _build_cross_cluster_task(
    cfg: PipelineConfig, table: str, task_kwargs: dict[str, Any]
) -> PythonOperator:
    source_conn_str = _source_connection_string(cfg.source)
    # dlt's postgres destination resolves its credentials lazily; we need them
    # here at DAG-parse time to hand a connection string to SQLAlchemy. Same
    # key convention as the destination factory (no type segment per
    # Segment 10.5).
    dest_key = f"destination.{cfg.destination.connection}.credentials"
    try:
        dest_value = dlt.secrets[dest_key]
    except Exception as exc:
        raise RuntimeError(
            f"quality.row_count_check: missing destination credentials at {dest_key!r}: {exc}"
        ) from exc

    return PythonOperator(
        task_id=f"row_count_check_{table}",
        python_callable=cross_cluster_row_count_check,
        op_kwargs={
            "source_conn_str": source_conn_str,
            "source_schema": _source_schema(cfg.source),
            "dest_conn_str": str(dest_value),
            "dest_dataset": cfg.destination.dataset,
            "table": table,
        },
        **task_kwargs,
    )


def _build_same_cluster_task(
    cfg: PipelineConfig, table: str, task_kwargs: dict[str, Any]
) -> SQLCheckOperator:
    # SQLCheckOperator passes/fails on the truthiness of the first column in
    # the first row of the result set. A boolean equality check is the
    # canonical pattern.
    schema = _source_schema(cfg.source)
    dataset = cfg.destination.dataset
    sql = (
        f'SELECT (SELECT COUNT(*) FROM "{schema}"."{table}") '
        f'= (SELECT COUNT(*) FROM "{dataset}"."{table}")'
    )
    return SQLCheckOperator(
        task_id=f"row_count_check_{table}",
        conn_id=f"dlt_{cfg.destination.connection}",
        sql=sql,
        **task_kwargs,
    )


def build_row_count_tasks(cfg: PipelineConfig, task_kwargs: dict[str, Any]) -> list[Any]:
    """Return one row-count task per table, empty list when feature disabled."""
    if not cfg.quality.row_count_check:
        return []
    tables = _tables_for(cfg.source)
    if cfg.quality.check_mode == QualityCheckMode.same_cluster:
        return [_build_same_cluster_task(cfg, t, task_kwargs) for t in tables]
    return [_build_cross_cluster_task(cfg, t, task_kwargs) for t in tables]
