"""Orchestrator-agnostic pipeline factory.

Turns a validated ``PipelineConfig`` into a runnable dlt pipeline + bound
source. Must NOT import ``airflow`` — Design principle #2 (enforced by ruff
``banned-module-level-imports``). The Airflow layer (Segment 4) wraps the
``RunnablePipeline`` returned here in a ``PipelineTasksGroup``; the CLI runs
it directly.
"""

from __future__ import annotations

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
    if cfg.sync.mode == SyncMode.cdc:
        raise NotImplementedError("cdc sync mode lands in Segment 7")

    builder = registry.get_builder(cfg.source.type)
    source = builder(cfg.source)

    if cfg.sync.mode == SyncMode.incremental:
        _apply_incremental_hints(source, cfg)
    elif cfg.sync.primary_key is not None:
        _apply_primary_key(source, cfg.sync.primary_key)

    destination = build_destination(cfg.destination)
    pipelines_state_dir = str(Path(".dlt") / "pipelines" / cfg.name)
    pipeline = dlt.pipeline(
        pipeline_name=cfg.name,
        destination=destination,
        dataset_name=cfg.destination.dataset,
        pipelines_dir=pipelines_state_dir,
    )
    return RunnablePipeline(
        pipeline=pipeline,
        source=source,
        write_disposition=cfg.options.write_disposition,
    )


def run(name: str, pipelines_root: Path | str = Path("pipelines")) -> LoadInfo:
    configs = load_pipelines(pipelines_root)
    try:
        cfg = configs[name]
    except KeyError:
        available = sorted(configs)
        raise KeyError(
            f"pipeline {name!r} not found under {pipelines_root!r}; available: {available}"
        ) from None
    runnable = build(cfg)
    return runnable.pipeline.run(
        runnable.source,
        write_disposition=runnable.write_disposition.value,
    )


if __name__ == "__main__":
    from data_pipeline_template.__main__ import main

    raise SystemExit(main())
