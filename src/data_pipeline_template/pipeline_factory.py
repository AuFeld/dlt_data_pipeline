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
from pathlib import Path
from typing import Any

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltSource
from dlt.extract.incremental import Incremental

from data_pipeline_template.config.loader import load_pipelines
from data_pipeline_template.config.models import PipelineConfig, SyncMode, WriteDisposition
from data_pipeline_template.destinations.factory import build_destination
from data_pipeline_template.sources import registry


@dataclass(frozen=True)
class RunnablePipeline:
    pipeline: dlt.Pipeline
    source: DltSource
    write_disposition: WriteDisposition


def _apply_incremental_hints(source: DltSource, cfg: PipelineConfig) -> None:
    cursor = cfg.sync.cursor_field
    primary_key = cfg.sync.primary_key
    if cursor is None:
        return
    incremental = Incremental[Any](cursor_path=cursor)
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


def _resolve(name: str, pipelines_root: Path | str) -> RunnablePipeline:
    configs = load_pipelines(pipelines_root)
    try:
        cfg = configs[name]
    except KeyError:
        available = sorted(configs)
        raise KeyError(
            f"pipeline {name!r} not found under {pipelines_root!r}; available: {available}"
        ) from None
    return build(cfg)


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
    from data_pipeline_template.__main__ import main

    raise SystemExit(main())
