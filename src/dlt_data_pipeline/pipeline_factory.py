"""Orchestrator-agnostic pipeline factory.

Turns a validated ``PipelineConfig`` into a runnable dlt pipeline + bound
source. Must NOT import ``airflow`` — Design principle #2 (enforced by ruff
``banned-module-level-imports``). The Airflow layer (Segment 4) wraps the
``RunnablePipeline`` returned here in a ``PipelineTasksGroup``; the CLI runs
it directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import dlt
import isodate
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltSource
from dlt.extract.incremental import Incremental

from dlt_data_pipeline.config.loader import load_pipelines
from dlt_data_pipeline.config.models import PipelineConfig, SyncMode, WriteDisposition
from dlt_data_pipeline.destinations.factory import build_destination
from dlt_data_pipeline.sources import registry


@dataclass(frozen=True)
class RunnablePipeline:
    pipeline: dlt.Pipeline
    source: DltSource
    write_disposition: WriteDisposition


def _incremental_lag_seconds(cfg: PipelineConfig) -> float | None:
    """Total cursor-lag in seconds from ``sync.tolerance_seconds`` + ``sync.lookback``.

    Both knobs catch late-arriving rows. They compose additively — a pipeline
    can set ``tolerance_seconds`` for clock-skew margin and ``lookback`` for a
    longer windowed re-scan. Returns ``None`` when neither is set so we don't
    pass ``lag=0`` to dlt (no-op but easier to reason about as absent).
    """
    total = float(cfg.sync.tolerance_seconds)
    if cfg.sync.lookback is not None:
        total += isodate.parse_duration(cfg.sync.lookback).total_seconds()
    return total if total > 0 else None


def _apply_incremental_hints(source: DltSource, cfg: PipelineConfig) -> None:
    cursor = cfg.sync.cursor_field
    primary_key = cfg.sync.primary_key
    if cursor is None:
        return
    lag = _incremental_lag_seconds(cfg)
    incremental = (
        Incremental[Any](cursor_path=cursor, lag=lag)
        if lag is not None
        else Incremental[Any](cursor_path=cursor)
    )
    for resource in source.resources.values():
        hints: dict[str, Any] = {"incremental": incremental}
        if primary_key is not None:
            hints["primary_key"] = primary_key
        resource.apply_hints(**hints)


def _apply_bounded_incremental(
    source: DltSource,
    cursor: str,
    primary_key: str | list[str] | None,
    initial_value: Any,
    end_value: Any,
) -> None:
    """Bound every resource's incremental hint to a closed/open ``[initial, end)`` range.

    Used by ``run_backfill`` to drive chunked historical loads without
    touching dlt's persisted cursor state — bounded ranges short-circuit the
    cursor lookup. dlt's ``sql_database`` honors ``end_value`` in the WHERE
    clause; other source types fall back to client-side filtering.
    """
    incremental = Incremental[Any](
        cursor_path=cursor, initial_value=initial_value, end_value=end_value
    )
    for resource in source.resources.values():
        hints: dict[str, Any] = {"incremental": incremental}
        if primary_key is not None:
            hints["primary_key"] = primary_key
        resource.apply_hints(**hints)


def _apply_primary_key(source: DltSource, primary_key: str | list[str]) -> None:
    for resource in source.resources.values():
        resource.apply_hints(primary_key=primary_key)


def build(cfg: PipelineConfig) -> RunnablePipeline:
    builder = registry.get_builder(cfg.source.type)
    source = builder(cfg.source)

    if cfg.sync.mode == SyncMode.incremental:
        _apply_incremental_hints(source, cfg)
    elif cfg.sync.mode == SyncMode.cdc:
        # Validator guarantees primary_key is set when mode == cdc. The
        # vendor pg_replication source manages LSN-based incremental state
        # internally, so no _apply_incremental_hints call here.
        assert cfg.sync.primary_key is not None
        _apply_primary_key(source, cfg.sync.primary_key)
    elif cfg.sync.primary_key is not None:
        _apply_primary_key(source, cfg.sync.primary_key)

    # cdc emits a mix of inserts + updates + deletes per LSN; append is
    # incoherent. Silent-promote the default to merge but honor an explicit
    # merge / replace from the YAML (audit-table use cases may want replace).
    effective_disposition = cfg.options.write_disposition
    if cfg.sync.mode == SyncMode.cdc and effective_disposition == WriteDisposition.append:
        effective_disposition = WriteDisposition.merge

    destination = build_destination(cfg.destination)
    # Under Airflow, ``PipelineTasksGroup`` sets ``DLT_DATA_DIR`` to a per-task
    # temp dir and rejects pipelines whose ``pipelines_dir`` overrides it. Skip
    # the explicit arg in that case so dlt resolves the dir from the env var.
    kwargs: dict[str, Any] = {
        "pipeline_name": cfg.name,
        "destination": destination,
        "dataset_name": cfg.destination.dataset,
    }
    if "DLT_DATA_DIR" not in os.environ:
        kwargs["pipelines_dir"] = str(Path(".dlt") / "pipelines" / cfg.name)
    pipeline = dlt.pipeline(**kwargs)
    return RunnablePipeline(
        pipeline=pipeline,
        source=source,
        write_disposition=effective_disposition,
    )


def _load_one(name: str, pipelines_root: Path | str) -> PipelineConfig:
    """Load + validate every YAML, return the single config matching ``name``.

    Raises ``KeyError`` with an ``available: [...]`` suggestion list on miss.
    Shared by ``_resolve`` (single-shot runs) and ``run_backfill`` (multi-chunk
    runs that need to rebuild between chunks without re-globbing).
    """
    configs = load_pipelines(pipelines_root)
    try:
        return configs[name]
    except KeyError:
        available = sorted(configs)
        raise KeyError(
            f"pipeline {name!r} not found under {pipelines_root!r}; available: {available}"
        ) from None


def _resolve(name: str, pipelines_root: Path | str) -> RunnablePipeline:
    return build(_load_one(name, pipelines_root))


def run(
    name: str,
    pipelines_root: Path | str = Path("pipelines"),
    limit: int | None = None,
) -> LoadInfo:
    runnable = _resolve(name, pipelines_root)
    if limit is not None:
        runnable.source.add_limit(limit)
    return runnable.pipeline.run(
        runnable.source,
        write_disposition=runnable.write_disposition.value,
    )


def run_backfill(
    name: str,
    start: datetime,
    end: datetime,
    pipelines_root: Path | str = Path("pipelines"),
) -> list[LoadInfo]:
    """Chunked, resumable historical load over ``[start, end)``.

    Requires ``sync.mode == incremental`` and ``sync.backfill`` on the YAML.
    Rebuilds the pipeline per chunk so dlt's state file isolates chunks; if
    the run dies mid-backfill the operator can re-invoke with a new ``start``
    matching the last completed chunk boundary.
    """
    if start >= end:
        raise ValueError(f"run_backfill: start {start!r} must precede end {end!r}")
    cfg = _load_one(name, pipelines_root)
    if cfg.sync.mode != SyncMode.incremental:
        raise ValueError(
            f"run_backfill: pipeline {name!r} sync.mode is {cfg.sync.mode.value!r}; "
            "backfill requires 'incremental'"
        )
    if cfg.sync.backfill is None:
        raise ValueError(
            f"run_backfill: pipeline {name!r} has no sync.backfill block; "
            "add sync.backfill.chunk_size to the YAML"
        )
    chunk_delta = isodate.parse_duration(cfg.sync.backfill.chunk_size)
    cursor = cfg.sync.backfill.partition_field or cfg.sync.cursor_field
    assert cursor is not None  # validator guarantees cursor_field when mode=incremental

    infos: list[LoadInfo] = []
    current = start
    while current < end:
        chunk_end = min(current + chunk_delta, end)
        runnable = build(cfg)
        _apply_bounded_incremental(
            runnable.source, cursor, cfg.sync.primary_key, current, chunk_end
        )
        infos.append(
            runnable.pipeline.run(
                runnable.source,
                write_disposition=runnable.write_disposition.value,
            )
        )
        current = chunk_end
    return infos


def run_dry(
    name: str,
    pipelines_root: Path | str = Path("pipelines"),
    limit: int | None = None,
) -> dict[str, str]:
    """Extract + normalize only — skip destination load.

    Validates source connectivity + schema inference without writing to the
    destination. Snowflake / Databricks destinations still construct via
    ``build()``, so dry-run won't help with stubbed destinations until
    Segment 8 fills them in.
    """
    runnable = _resolve(name, pipelines_root)
    if limit is not None:
        runnable.source.add_limit(limit)
    extract_info = runnable.pipeline.extract(runnable.source)
    normalize_info = runnable.pipeline.normalize()
    return {
        "name": name,
        "extract": str(extract_info),
        "normalize": str(normalize_info),
    }


if __name__ == "__main__":
    from dlt_data_pipeline.__main__ import main

    raise SystemExit(main())
