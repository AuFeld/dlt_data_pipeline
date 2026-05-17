"""`pipelines validate` / `pipelines doctor` / `pipelines promote` subcommands.

Pure data helpers (``validate_pipelines`` / ``doctor_pipelines`` /
``promote_pipelines``) return structured reports consumed by both the CLI
wrappers and the MCP server.
"""

from __future__ import annotations

import argparse
import os
import sys
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

import dlt

from dlt_data_pipeline.config.loader import (
    ConfigError,
    load_pipelines,
    resolve_env,
)
from dlt_data_pipeline.config.models import DestinationType, PipelineConfig
from dlt_data_pipeline.destinations import _metadata as dest_metadata
from dlt_data_pipeline.sources import registry


class CredentialStatus(StrEnum):
    OK_ENV = "env"
    OK_SECRETS_TOML = "secrets-toml"
    OK_AIRFLOW_BACKEND = "airflow-backend"
    OK_NO_CREDS_REQUIRED = "no-creds-required"
    MISSING = "MISSING"


def validate_pipelines(
    pipelines_root: str | Path = "pipelines",
    name: str | None = None,
    env: str | None = None,
) -> dict[str, object]:
    """Parse + validate one or all YAMLs. Pure helper (no printing, no sys.exit).

    Returns a structured report:
        {
          "status": "ok" | "error",
          "pipelines_root": str,
          "env": str,
          "errors": [str, ...],           # aggregated loader errors
          "pipelines": ["name", ...],     # validated names (empty when status=error)
        }
    """
    active_env = resolve_env(env)
    try:
        configs = load_pipelines(pipelines_root, env=active_env)
    except ConfigError as exc:
        return {
            "status": "error",
            "pipelines_root": str(pipelines_root),
            "env": active_env,
            "errors": [str(exc)],
            "pipelines": [],
        }

    if name is not None:
        if name not in configs:
            return {
                "status": "error",
                "pipelines_root": str(pipelines_root),
                "env": active_env,
                "errors": [
                    f"pipeline {name!r} not found under {str(pipelines_root)!r}; "
                    f"available: {sorted(configs)}"
                ],
                "pipelines": [],
            }
        return {
            "status": "ok",
            "pipelines_root": str(pipelines_root),
            "env": active_env,
            "errors": [],
            "pipelines": [name],
        }

    return {
        "status": "ok",
        "pipelines_root": str(pipelines_root),
        "env": active_env,
        "errors": [],
        "pipelines": sorted(configs),
    }


def _dlt_secret_key(env_var: str) -> str:
    # e.g. SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS
    #   -> sources.sql_database.pg_source.credentials
    return ".".join(part.lower() for part in env_var.split("__"))


def _probe_credential(env_var: str | None) -> CredentialStatus:
    if env_var is None:
        return CredentialStatus.OK_NO_CREDS_REQUIRED
    if os.environ.get(env_var):
        return CredentialStatus.OK_ENV
    secret_key = _dlt_secret_key(env_var)
    try:
        value = dlt.secrets[secret_key]
    except Exception:
        value = None
    if value:
        return CredentialStatus.OK_SECRETS_TOML
    # Airflow Secrets Backend (Segment 13, doc + signal only). The CLI does
    # NOT call the backend — that would require an Airflow runtime
    # dependency in the doctor path. Presence of the env var alone signals
    # "doctor cannot verify; trust at runtime."
    if os.environ.get("AIRFLOW__SECRETS__BACKEND"):
        return CredentialStatus.OK_AIRFLOW_BACKEND
    return CredentialStatus.MISSING


def _doctor_slot(
    kind: str,
    type_label: str,
    connection: str,
    env_var: str | None,
) -> dict[str, str | None]:
    status = _probe_credential(env_var)
    return {
        "slot": kind,
        "type": type_label,
        "connection": connection,
        "env_var": env_var,
        "status": status.value,
    }


_LOCAL_FS_SCHEMES: frozenset[str] = frozenset({"", "file"})


def _filesystem_needs_creds(cfg: PipelineConfig) -> bool:
    """Filesystem source only needs credentials for remote buckets.

    Local ``file://`` URLs and absolute paths (empty scheme) are read off
    the local FS by ``fsspec`` without any credential resolution. Treating
    those as MISSING in ``doctor`` would produce noisy false positives.
    """
    if cfg.source.type != "filesystem":
        return True
    from urllib.parse import urlparse

    bucket_url = cfg.source.config.get("bucket_url", "")
    scheme = urlparse(str(bucket_url)).scheme.lower()
    return scheme not in _LOCAL_FS_SCHEMES


def _doctor_one(name: str, cfg: PipelineConfig) -> dict[str, object]:
    slots: list[dict[str, str | None]] = []

    try:
        src_meta = registry.get_metadata(cfg.source.type)
        src_env = src_meta.resolve_env_var(cfg.source.connection)
    except registry.MissingSourceMetadataError:
        src_env = None
    if not _filesystem_needs_creds(cfg):
        src_env = None
    slots.append(_doctor_slot("source", cfg.source.type, cfg.source.connection, src_env))

    dest_type_enum: DestinationType = cfg.destination.type
    dest_meta = dest_metadata.get_metadata(dest_type_enum)
    dest_env = dest_meta.resolve_env_var(cfg.destination.connection)
    slots.append(
        _doctor_slot(
            "destination", cfg.destination.type.value, cfg.destination.connection, dest_env
        )
    )

    any_missing = any(s["status"] == CredentialStatus.MISSING.value for s in slots)
    return {
        "name": name,
        "status": "MISSING" if any_missing else "OK",
        "slots": slots,
    }


def doctor_pipelines(
    pipelines_root: str | Path = "pipelines",
    env: str | None = None,
) -> dict[str, object]:
    """Probe expected env vars + .dlt secrets. Pure helper.

    Returns:
        {
          "status": "ok" | "missing" | "error",
          "pipelines_root": str,
          "env": str,
          "errors": [str, ...],                # loader errors when status=error
          "report": [
            {"name": str, "status": "OK"|"MISSING", "slots": [{...}, {...}]},
            ...
          ],
        }
    """
    active_env = resolve_env(env)
    try:
        configs = load_pipelines(pipelines_root, env=active_env)
    except ConfigError as exc:
        return {
            "status": "error",
            "pipelines_root": str(pipelines_root),
            "env": active_env,
            "errors": [str(exc)],
            "report": [],
        }

    report = [_doctor_one(name, configs[name]) for name in sorted(configs)]
    any_missing = any(p["status"] == "MISSING" for p in report)
    return {
        "status": "missing" if any_missing else "ok",
        "pipelines_root": str(pipelines_root),
        "env": active_env,
        "errors": [],
        "report": report,
    }


_OVERLAY_FIELD_PATHS: tuple[tuple[str, ...], ...] = (
    ("source", "connection"),
    ("destination", "type"),
    ("destination", "connection"),
    ("destination", "dataset"),
    ("schedule", "enabled"),
    ("resources", "cpu"),
    ("resources", "memory"),
)


def _extract_path(cfg: PipelineConfig, path: tuple[str, ...]) -> Any:
    value: Any = cfg
    for part in path:
        value = getattr(value, part, None)
        if value is None:
            return None
    # Enum values render as their .value (e.g. "duckdb") for cleaner diffs.
    if isinstance(value, Enum):
        return value.value
    return value


def promote_pipelines(
    name: str,
    from_env: str,
    to_env: str,
    pipelines_root: str | Path = "pipelines",
) -> dict[str, object]:
    """Diff merged config across two envs for one pipeline. Pure helper.

    Returns:
        {
          "status": "ok" | "error" | "not-found",
          "name": str,
          "from_env": str, "to_env": str,
          "changes": [{"field": str, "from": Any, "to": Any}, ...],
          "errors": [str, ...],
        }
    """
    try:
        from_configs = load_pipelines(pipelines_root, env=from_env)
        to_configs = load_pipelines(pipelines_root, env=to_env)
    except ConfigError as exc:
        return {
            "status": "error",
            "name": name,
            "from_env": from_env,
            "to_env": to_env,
            "changes": [],
            "errors": [str(exc)],
        }

    if name not in from_configs or name not in to_configs:
        available = sorted(set(from_configs) | set(to_configs))
        return {
            "status": "not-found",
            "name": name,
            "from_env": from_env,
            "to_env": to_env,
            "changes": [],
            "errors": [
                f"pipeline {name!r} not found under {str(pipelines_root)!r}; available: {available}"
            ],
        }

    from_cfg = from_configs[name]
    to_cfg = to_configs[name]
    changes: list[dict[str, Any]] = []
    for path in _OVERLAY_FIELD_PATHS:
        from_value = _extract_path(from_cfg, path)
        to_value = _extract_path(to_cfg, path)
        if from_value != to_value:
            changes.append(
                {
                    "field": ".".join(path),
                    "from": from_value,
                    "to": to_value,
                }
            )

    return {
        "status": "ok",
        "name": name,
        "from_env": from_env,
        "to_env": to_env,
        "changes": changes,
        "errors": [],
    }


def cmd_validate(args: argparse.Namespace) -> int:
    report = validate_pipelines(args.pipelines_root, args.name, env=args.env)
    if report["status"] == "error":
        for err in report["errors"]:  # type: ignore[attr-defined]
            print(err, file=sys.stderr)
        return 1

    pipelines = report["pipelines"]
    if not pipelines:
        print(f"No pipelines found under {args.pipelines_root}.")
        return 0
    for name in pipelines:  # type: ignore[attr-defined]
        print(f"OK: {name}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    report = doctor_pipelines(args.pipelines_root, env=args.env)
    if report["status"] == "error":
        for err in report["errors"]:  # type: ignore[attr-defined]
            print(err, file=sys.stderr)
        return 1

    entries = report["report"]
    if not entries:
        print(f"No pipelines found under {args.pipelines_root}.")
        return 0

    for entry in entries:  # type: ignore[attr-defined]
        print(f"{entry['name']}: {entry['status']}")
        for slot in entry["slots"]:
            label = slot["env_var"] or "(no credentials required)"
            print(
                f"  {slot['slot']} {slot['type']}/{slot['connection']}: {slot['status']} [{label}]"
            )

    return 1 if report["status"] == "missing" else 0


def cmd_promote(args: argparse.Namespace) -> int:
    report = promote_pipelines(args.name, args.from_env, args.to_env, args.pipelines_root)
    if report["status"] == "error":
        for err in report["errors"]:  # type: ignore[attr-defined]
            print(err, file=sys.stderr)
        return 1
    if report["status"] == "not-found":
        for err in report["errors"]:  # type: ignore[attr-defined]
            print(err, file=sys.stderr)
        return 1

    changes = report["changes"]  # type: ignore[index]
    print(f"pipeline: {report['name']}")
    print(f"  {report['from_env']} -> {report['to_env']}")
    if not changes:
        print("    (no differences in overlay-eligible fields)")
        return 0
    width = max(len(c["field"]) for c in changes)
    for change in changes:
        from_val = "(unset)" if change["from"] is None else change["from"]
        to_val = "(unset)" if change["to"] is None else change["to"]
        print(f"    {change['field']:<{width}}: {from_val} -> {to_val}")
    plural = "s" if len(changes) != 1 else ""
    print(f"  {len(changes)} field{plural} differ.")
    return 0
