"""Airflow callback factories that route to ``observability.alerts``.

Factories return ``functools.partial`` over module-level dispatch functions
with ``alerts_dict = alerts.model_dump()`` (not a closure over the pydantic
model itself). The DAG serializer prefers JSON-serializable primitives;
reconstructing ``AlertsConfig`` inside the dispatch keeps the closure safe.

All dispatchers swallow exceptions — Airflow callbacks must never raise.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any

from dlt_data_pipeline.config.models import AlertsConfig
from dlt_data_pipeline.observability.alerts import (
    post_failure_alert,
    post_schema_change_alert,
    post_sla_miss_alert,
)

log = logging.getLogger(__name__)


def _on_failure_dispatch(
    context: dict[str, Any], *, alerts_dict: dict[str, Any], pipeline_name: str
) -> None:
    try:
        alerts = AlertsConfig.model_validate(alerts_dict)
        ti = context.get("task_instance")
        dag_id = getattr(ti, "dag_id", pipeline_name) if ti else pipeline_name
        task_id = getattr(ti, "task_id", "?") if ti else "?"
        run_id = getattr(ti, "run_id", "?") if ti else "?"
        log_url = getattr(ti, "log_url", None) if ti else None
        exc = context.get("exception")
        post_failure_alert(
            pipeline_name=pipeline_name,
            severity=alerts.severity,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            log_url=log_url,
            exception_summary=repr(exc)[:500] if exc is not None else "(no exception in context)",
            alerts=alerts,
        )
    except Exception:
        log.exception("on_failure alert dispatch failed for pipeline=%s", pipeline_name)


def _sla_miss_dispatch(
    dag: Any,
    task_list: Any,
    blocking_task_list: Any,
    slas: Any,
    blocking_tis: Any,
    *,
    alerts_dict: dict[str, Any],
    pipeline_name: str,
) -> None:
    try:
        alerts = AlertsConfig.model_validate(alerts_dict)
        if not alerts.on_sla_miss:
            return
        dag_id = getattr(dag, "dag_id", pipeline_name)
        task_ids: list[str] = []
        for sla in slas or []:
            tid = getattr(sla, "task_id", None)
            if tid:
                task_ids.append(tid)
        post_sla_miss_alert(
            pipeline_name=pipeline_name,
            dag_id=dag_id,
            task_ids=task_ids,
            alerts=alerts,
        )
    except Exception:
        log.exception("sla_miss alert dispatch failed for pipeline=%s", pipeline_name)


def make_on_failure_callback(
    alerts: AlertsConfig, pipeline_name: str
) -> Callable[[dict[str, Any]], None]:
    return functools.partial(
        _on_failure_dispatch,
        alerts_dict=alerts.model_dump(mode="json"),
        pipeline_name=pipeline_name,
    )


def make_sla_miss_callback(alerts: AlertsConfig, pipeline_name: str) -> Callable[..., None]:
    return functools.partial(
        _sla_miss_dispatch,
        alerts_dict=alerts.model_dump(mode="json"),
        pipeline_name=pipeline_name,
    )


def _extract_schema_updates(trace: Any) -> list[dict[str, Any]]:
    """Best-effort extraction of schema deltas from a dlt trace.

    Walks two surfaces and dedupes:
      * Legacy: ``step_info.table_metrics`` / ``tables_with_new_columns`` —
        emitted by older dlt point releases.
      * Current (dlt >= 1.x): ``step_info.load_packages[].schema_update`` —
        a ``{table_name: {"columns": {col_name: spec, ...}, ...}}`` dict that
        records the actual delta a load package applied to the schema.

    Returns ``[]`` on anything unexpected — schema-change reporting is
    informational and must never fail the load.
    """
    updates: list[dict[str, Any]] = []
    seen_lp: set[tuple[str, tuple[str, ...]]] = set()
    steps = getattr(trace, "steps", None) or []
    for step in steps:
        info = getattr(step, "step_info", None)
        if info is None:
            continue
        # Legacy paths.
        tables = getattr(info, "table_metrics", None) or {}
        if isinstance(tables, dict):
            for tname, _ in tables.items():
                updates.append({"step": getattr(step, "step", "?"), "table": tname})
        new_cols = getattr(info, "tables_with_new_columns", None) or []
        for tname in new_cols:
            updates.append({"step": "extract", "table": tname, "kind": "new_columns"})
        # Current path: load_packages carry the authoritative schema_update.
        # Dedup by (table, sorted_columns) so the same delta surfacing on
        # extract+normalize+load doesn't triple-report.
        for lp in getattr(info, "load_packages", None) or []:
            schema_update = getattr(lp, "schema_update", None) or {}
            if not isinstance(schema_update, dict):
                continue
            for tname, tdef in schema_update.items():
                cols: list[str] = []
                if isinstance(tdef, dict):
                    cols_dict = tdef.get("columns") or {}
                    if isinstance(cols_dict, dict):
                        cols = sorted(cols_dict.keys())
                key = (tname, tuple(cols))
                if key in seen_lp:
                    continue
                seen_lp.add(key)
                updates.append(
                    {
                        "step": getattr(step, "step", "?"),
                        "table": tname,
                        "new_columns": cols,
                    }
                )
    return updates


def schema_change_probe(
    alerts: AlertsConfig | dict[str, Any], pipeline_name: str, **context: Any
) -> None:
    """PythonOperator callable. Inspect dlt trace; emit info-level alert.

    Always returns ``None`` — schema reporting is best-effort and must not fail
    the upstream load. ``alerts`` accepts a pydantic model OR a plain dict so
    the operator's ``op_kwargs`` can serialize through Airflow.
    """
    try:
        cfg = alerts if isinstance(alerts, AlertsConfig) else AlertsConfig.model_validate(alerts)
        if not cfg.on_schema_change:
            return
        import dlt

        pipeline = dlt.pipeline(pipeline_name=pipeline_name)
        trace = getattr(pipeline, "last_trace", None)
        if trace is None:
            return
        updates = _extract_schema_updates(trace)
        if not updates:
            return
        ti = context.get("task_instance")
        dag_id = getattr(ti, "dag_id", pipeline_name) if ti else pipeline_name
        run_id = getattr(ti, "run_id", "?") if ti else "?"
        post_schema_change_alert(
            pipeline_name=pipeline_name,
            dag_id=dag_id,
            run_id=run_id,
            schema_updates=updates,
            alerts=cfg,
        )
    except Exception:
        log.exception("schema_change_probe failed for pipeline=%s", pipeline_name)
