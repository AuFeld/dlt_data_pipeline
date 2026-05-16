"""`pipelines validate` / `pipelines doctor` subcommands.

Pure data helpers (``validate_pipelines`` / ``doctor_pipelines``) return
structured reports consumed by both the CLI wrappers and the MCP server.
"""

from __future__ import annotations

import argparse
import os
import sys
from enum import StrEnum
from pathlib import Path

import dlt

from data_pipeline_template.config.loader import ConfigError, load_pipelines
from data_pipeline_template.config.models import DestinationType, PipelineConfig
from data_pipeline_template.destinations import _metadata as dest_metadata
from data_pipeline_template.sources import registry


class CredentialStatus(StrEnum):
    OK_ENV = "env"
    OK_SECRETS_TOML = "secrets-toml"
    OK_NO_CREDS_REQUIRED = "no-creds-required"
    MISSING = "MISSING"


def validate_pipelines(
    pipelines_root: str | Path = "pipelines",
    name: str | None = None,
) -> dict[str, object]:
    """Parse + validate one or all YAMLs. Pure helper (no printing, no sys.exit).

    Returns a structured report:
        {
          "status": "ok" | "error",
          "pipelines_root": str,
          "errors": [str, ...],           # aggregated loader errors
          "pipelines": ["name", ...],     # validated names (empty when status=error)
        }
    """
    try:
        configs = load_pipelines(pipelines_root)
    except ConfigError as exc:
        return {
            "status": "error",
            "pipelines_root": str(pipelines_root),
            "errors": [str(exc)],
            "pipelines": [],
        }

    if name is not None:
        if name not in configs:
            return {
                "status": "error",
                "pipelines_root": str(pipelines_root),
                "errors": [
                    f"pipeline {name!r} not found under {str(pipelines_root)!r}; "
                    f"available: {sorted(configs)}"
                ],
                "pipelines": [],
            }
        return {
            "status": "ok",
            "pipelines_root": str(pipelines_root),
            "errors": [],
            "pipelines": [name],
        }

    return {
        "status": "ok",
        "pipelines_root": str(pipelines_root),
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


def _doctor_one(name: str, cfg: PipelineConfig) -> dict[str, object]:
    slots: list[dict[str, str | None]] = []

    try:
        src_meta = registry.get_metadata(cfg.source.type)
        src_env = src_meta.resolve_env_var(cfg.source.connection)
    except registry.MissingSourceMetadataError:
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


def doctor_pipelines(pipelines_root: str | Path = "pipelines") -> dict[str, object]:
    """Probe expected env vars + .dlt secrets. Pure helper.

    Returns:
        {
          "status": "ok" | "missing" | "error",
          "pipelines_root": str,
          "errors": [str, ...],                # loader errors when status=error
          "report": [
            {"name": str, "status": "OK"|"MISSING", "slots": [{...}, {...}]},
            ...
          ],
        }
    """
    try:
        configs = load_pipelines(pipelines_root)
    except ConfigError as exc:
        return {
            "status": "error",
            "pipelines_root": str(pipelines_root),
            "errors": [str(exc)],
            "report": [],
        }

    report = [_doctor_one(name, configs[name]) for name in sorted(configs)]
    any_missing = any(p["status"] == "MISSING" for p in report)
    return {
        "status": "missing" if any_missing else "ok",
        "pipelines_root": str(pipelines_root),
        "errors": [],
        "report": report,
    }


def cmd_validate(args: argparse.Namespace) -> int:
    report = validate_pipelines(args.pipelines_root, args.name)
    if report["status"] == "error":
        for err in report["errors"]:  # type: ignore[union-attr]
            print(err, file=sys.stderr)
        return 1

    pipelines = report["pipelines"]
    if not pipelines:
        print(f"No pipelines found under {args.pipelines_root}.")
        return 0
    for name in pipelines:  # type: ignore[union-attr]
        print(f"OK: {name}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    report = doctor_pipelines(args.pipelines_root)
    if report["status"] == "error":
        for err in report["errors"]:  # type: ignore[union-attr]
            print(err, file=sys.stderr)
        return 1

    entries = report["report"]
    if not entries:
        print(f"No pipelines found under {args.pipelines_root}.")
        return 0

    for entry in entries:  # type: ignore[union-attr]
        print(f"{entry['name']}: {entry['status']}")
        for slot in entry["slots"]:
            label = slot["env_var"] or "(no credentials required)"
            print(
                f"  {slot['slot']} {slot['type']}/{slot['connection']}: {slot['status']} [{label}]"
            )

    return 1 if report["status"] == "missing" else 0
