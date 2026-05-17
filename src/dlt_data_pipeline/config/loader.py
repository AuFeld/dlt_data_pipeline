"""Discover, parse, and validate ``pipelines/*.yml`` (+ optional env overlays).

Aggregates errors across every file before raising so one bad YAML does not
hide the next. Called by ``dags/data_pipeline_dags.py`` at DagBag scan time
(Segment 4) and by the CLI runner (Segment 3).

Environment overlays (Segment 13): ``pipelines/_env/<env>.yml`` is a single
file per env, keyed by pipeline name. Each value is a ``PipelineOverlay``
re-mapping a narrow set of fields (source/destination connection,
schedule.enabled, resources). Resolved env order: explicit ``env`` arg >
``$DLT_ENV`` > ``"dev"``. Overlays merge at the raw-dict level before
``PipelineConfig.model_validate`` so cross-field validators re-run on the
merged result.
"""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .models import PipelineConfig, PipelineOverlay

DEFAULT_ENV = "dev"
_OVERLAY_DIR = "_env"
_OVERLAY_FIELDS: tuple[str, ...] = ("source", "destination", "schedule", "resources")


class ConfigError(Exception):
    """Aggregated YAML/validation errors across ``pipelines/*.yml``."""


def resolve_env(cli_env: str | None = None) -> str:
    """Resolve the active env name from CLI arg > ``$DLT_ENV`` > ``"dev"``."""
    if cli_env:
        return cli_env
    return os.environ.get("DLT_ENV") or DEFAULT_ENV


def _format_validation_error(path: Path, err: ValidationError) -> list[str]:
    lines: list[str] = []
    for detail in err.errors():
        loc = ".".join(str(p) for p in detail["loc"]) or "<root>"
        lines.append(f"{path}: {loc}: {detail['msg']}")
    return lines


def _format_duplicate(name: str, paths: Iterable[Path]) -> str:
    joined = ", ".join(str(p) for p in sorted(paths))
    return f"duplicate pipeline name '{name}' declared in: {joined}"


def _load_overlay(root: Path, env: str) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Parse + validate ``<root>/_env/<env>.yml``.

    Returns ``(overlay_by_name, errors)``. A missing overlay file is not an
    error — returns ``({}, [])`` so default ``dev`` runs without committing
    an overlay.
    """
    overlay_path = root / _OVERLAY_DIR / f"{env}.yml"
    if not overlay_path.exists():
        return {}, []

    try:
        raw: Any = yaml.safe_load(overlay_path.read_text())
    except yaml.YAMLError as exc:
        return {}, [f"{overlay_path}: YAML parse error: {exc}"]

    if raw is None:
        return {}, []
    if not isinstance(raw, dict):
        return {}, [
            f"{overlay_path}: top-level YAML must be a mapping of "
            f"pipeline-name -> overlay block, got {type(raw).__name__}"
        ]

    errors: list[str] = []
    overlay_by_name: dict[str, dict[str, Any]] = {}
    for name, block in raw.items():
        if not isinstance(block, dict):
            errors.append(
                f"{overlay_path}: {name}: overlay block must be a mapping, "
                f"got {type(block).__name__}"
            )
            continue
        try:
            # Validate shape via PipelineOverlay (extra="forbid" rejects
            # out-of-scope keys). Then re-emit as a dict so the merge step
            # works at the raw level.
            PipelineOverlay.model_validate(block)
        except ValidationError as exc:
            for detail in exc.errors():
                loc = ".".join(str(p) for p in detail["loc"]) or "<root>"
                errors.append(f"{overlay_path}: {name}.{loc}: {detail['msg']}")
            continue
        overlay_by_name[name] = block
    return overlay_by_name, errors


def _apply_overlay(raw_base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Merge an overlay block onto a base pipeline dict.

    Scope is bounded to ``_OVERLAY_FIELDS`` — anything else is ignored at
    the merge level (overlay validation already rejected out-of-scope keys
    via ``PipelineOverlay``). For ``source``/``destination``/``schedule``,
    leaf keys replace one-by-one so the base ``config:``/``dataset:``/``cron:``
    survive. ``resources`` replaces wholesale (cpu/memory ride together).
    """
    merged = deepcopy(raw_base)
    for field in _OVERLAY_FIELDS:
        if field not in overlay or overlay[field] is None:
            continue
        if field == "resources":
            merged[field] = deepcopy(overlay[field])
            continue
        base_block = merged.get(field)
        if not isinstance(base_block, dict):
            merged[field] = deepcopy(overlay[field])
            continue
        for leaf_key, leaf_value in overlay[field].items():
            if leaf_value is None:
                continue
            base_block[leaf_key] = deepcopy(leaf_value)
    return merged


def load_pipelines(
    root: Path | str = Path("pipelines"),
    env: str | None = None,
) -> dict[str, PipelineConfig]:
    """Glob ``<root>/*.yml`` (skip ``_*``) and return validated configs.

    Applies the overlay at ``<root>/_env/<env>.yml`` when present. ``env``
    defaults to ``$DLT_ENV`` then ``"dev"`` via :func:`resolve_env`.

    Raises :class:`ConfigError` with an aggregated multi-line report when any
    file fails to parse or validate, when two files declare the same
    ``name``, or when the overlay references unknown pipeline names. An
    empty or missing directory returns ``{}``.
    """
    root = Path(root)
    if not root.exists():
        return {}

    active_env = resolve_env(env)
    errors: list[str] = []
    raw_by_path: dict[Path, dict[str, Any]] = {}
    name_to_paths: dict[str, list[Path]] = defaultdict(list)
    path_to_name: dict[Path, str] = {}

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
        raw_by_path[path] = raw
        cfg_name = raw.get("name")
        if isinstance(cfg_name, str):
            name_to_paths[cfg_name].append(path)
            path_to_name[path] = cfg_name

    overlay_by_name, overlay_errors = _load_overlay(root, active_env)
    errors.extend(overlay_errors)

    known_names = set(path_to_name.values())
    for overlay_name in sorted(overlay_by_name):
        if overlay_name not in known_names:
            errors.append(
                f"{root / _OVERLAY_DIR / f'{active_env}.yml'}: overlay references "
                f"unknown pipeline name {overlay_name!r}; available: {sorted(known_names)}"
            )

    parsed: dict[Path, PipelineConfig] = {}
    for path, raw in raw_by_path.items():
        name = path_to_name.get(path)
        merged = raw
        if name is not None and name in overlay_by_name:
            merged = _apply_overlay(raw, overlay_by_name[name])
        try:
            cfg = PipelineConfig.model_validate(merged)
        except ValidationError as exc:
            errors.extend(_format_validation_error(path, exc))
            continue
        parsed[path] = cfg

    for name, paths in name_to_paths.items():
        if len(paths) > 1:
            errors.append(_format_duplicate(name, paths))

    if errors:
        raise ConfigError(
            f"Found {len(errors)} problem(s) loading pipelines from {root} "
            f"(env={active_env!r}):\n  - " + "\n  - ".join(errors)
        )

    return {cfg.name: cfg for cfg in parsed.values()}
