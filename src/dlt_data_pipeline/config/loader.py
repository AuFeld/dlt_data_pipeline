"""Discover, parse, and validate ``pipelines/*.yml``.

Aggregates errors across every file before raising so one bad YAML does not
hide the next. Called by ``dags/data_pipeline_dags.py`` at DagBag scan time
(Segment 4) and by the CLI runner (Segment 3).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .models import PipelineConfig


class ConfigError(Exception):
    """Aggregated YAML/validation errors across ``pipelines/*.yml``."""


def _format_validation_error(path: Path, err: ValidationError) -> list[str]:
    lines: list[str] = []
    for detail in err.errors():
        loc = ".".join(str(p) for p in detail["loc"]) or "<root>"
        lines.append(f"{path}: {loc}: {detail['msg']}")
    return lines


def _format_duplicate(name: str, paths: Iterable[Path]) -> str:
    joined = ", ".join(str(p) for p in sorted(paths))
    return f"duplicate pipeline name '{name}' declared in: {joined}"


def load_pipelines(root: Path | str = Path("pipelines")) -> dict[str, PipelineConfig]:
    """Glob ``<root>/*.yml`` (skip ``_*``) and return validated configs.

    Raises :class:`ConfigError` with an aggregated multi-line report when any
    file fails to parse or validate, or when two files declare the same
    ``name``. An empty or missing directory returns ``{}``.
    """
    root = Path(root)
    if not root.exists():
        return {}

    errors: list[str] = []
    parsed: dict[Path, PipelineConfig] = {}
    name_to_paths: dict[str, list[Path]] = defaultdict(list)

    for path in sorted(root.glob("*.yml")):
        if path.name.startswith("_"):
            continue
        try:
            raw: Any = yaml.safe_load(path.read_text())
        except yaml.YAMLError as exc:
            errors.append(f"{path}: YAML parse error: {exc}")
            continue
        if not isinstance(raw, dict):
            errors.append(f"{path}: top-level YAML must be a mapping, got {type(raw).__name__}")
            continue
        try:
            cfg = PipelineConfig.model_validate(raw)
        except ValidationError as exc:
            errors.extend(_format_validation_error(path, exc))
            continue
        parsed[path] = cfg
        name_to_paths[cfg.name].append(path)

    for name, paths in name_to_paths.items():
        if len(paths) > 1:
            errors.append(_format_duplicate(name, paths))

    if errors:
        raise ConfigError(
            f"Found {len(errors)} problem(s) loading pipelines from {root}:\n  - "
            + "\n  - ".join(errors)
        )

    return {cfg.name: cfg for cfg in parsed.values()}
