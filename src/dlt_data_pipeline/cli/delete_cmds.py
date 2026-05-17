"""`pipelines delete <name>` CLI subcommand (Segment 12).

Tears down a pipeline's external state in idempotent steps so a partial
failure doesn't strand orphan slots / publications / datasets:

  1. (pg_cdc only) drop the replication slot + publication on the source.
  2. unless ``--keep-data``: ``DROP SCHEMA IF EXISTS <dataset> CASCADE`` on
     the destination via dlt's sql_client.
  3. ``pipeline.drop()`` to wipe local ``.dlt/pipelines/<name>/`` state.
  4. ``Path.unlink()`` the YAML.

Refuses to run without ``--yes`` (CI-safe; no interactive prompt). Each step
captures its own error and the full report prints at the end so the operator
sees every failure, not just the first one.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from dlt_data_pipeline import pipeline_factory
from dlt_data_pipeline.config.loader import ConfigError, load_pipelines
from dlt_data_pipeline.config.models import PipelineConfig


def _drop_cdc_slot_and_publication(cfg: PipelineConfig) -> list[str]:
    """Best-effort drop of the slot + publication for a pg_cdc pipeline.

    Returns an error list (empty on success). The vendor helpers swallow
    ``UndefinedObject`` so re-runs are no-ops.
    """
    errors: list[str] = []
    # Lazy import: vendor helpers pull in psycopg2.extras at module import,
    # which is fine in prod but undesirable when delete is invoked against a
    # non-cdc pipeline that wouldn't otherwise need it.
    from dlt_data_pipeline.sources.pg_cdc import _resolve_credentials
    from dlt_data_pipeline.sources.pg_cdc._vendor.helpers import (
        drop_publication,
        drop_replication_slot,
        replication_connection,
    )

    slot_name = str(cfg.source.config["slot_name"])
    pub_name = str(cfg.source.config["publication_name"])
    try:
        creds = _resolve_credentials(cfg.source.connection)
    except ValueError as exc:
        errors.append(f"pg_cdc credentials: {exc}")
        return errors

    try:
        conn = replication_connection(slot_name=slot_name, credentials=creds)
    except Exception as exc:  # psycopg2 errors aren't a single base class
        errors.append(f"replication_connection: {exc}")
        return errors
    try:
        with conn.cursor() as cur:
            try:
                drop_replication_slot(slot_name, cur)
            except Exception as exc:
                errors.append(f"drop_replication_slot({slot_name!r}): {exc}")
            try:
                drop_publication(pub_name, cur)
            except Exception as exc:
                errors.append(f"drop_publication({pub_name!r}): {exc}")
    finally:
        conn.close()
    return errors


def _drop_destination_dataset(cfg: PipelineConfig) -> list[str]:
    """``DROP SCHEMA IF EXISTS <dataset> CASCADE`` via dlt's sql_client.

    Same SQL works for postgres / snowflake / duckdb. Databricks is rejected
    upstream by ``build_destination`` (Segment 8 deferral). The dataset name
    flows through the pydantic schema validator so SQL injection isn't a
    concern, but quote it defensively anyway.
    """
    errors: list[str] = []
    try:
        runnable = pipeline_factory.build(cfg)
    except Exception as exc:
        errors.append(f"build pipeline: {exc}")
        return errors
    try:
        with runnable.pipeline.sql_client() as client:
            client.execute_sql(f'DROP SCHEMA IF EXISTS "{cfg.destination.dataset}" CASCADE')
    except Exception as exc:
        errors.append(f"DROP SCHEMA: {exc}")
    return errors


def _drop_local_state(cfg: PipelineConfig) -> list[str]:
    errors: list[str] = []
    try:
        runnable = pipeline_factory.build(cfg)
        runnable.pipeline.drop()
    except Exception as exc:
        errors.append(f"pipeline.drop(): {exc}")
    return errors


def _find_yaml_path(name: str, pipelines_root: Path) -> Path | None:
    for path in sorted(pipelines_root.glob("*.yml")):
        if path.name.startswith("_"):
            continue
        # cheap match on filename; final source of truth is the YAML name field
        if path.stem == name:
            return path
    # fall back: parse + match on the name field (handles file/name mismatch)
    try:
        configs = load_pipelines(pipelines_root)
    except ConfigError:
        return None
    if name not in configs:
        return None
    for path in sorted(pipelines_root.glob("*.yml")):
        if path.name.startswith("_"):
            continue
        try:
            cfg = load_pipelines(path.parent).get(name)
        except ConfigError:
            continue
        if cfg is not None:
            return path
    return None


def delete_pipeline(
    name: str,
    pipelines_root: str | Path = "pipelines",
    *,
    keep_data: bool = False,
) -> dict[str, object]:
    """Tear down a pipeline's external state. Pure helper.

    Returns:
        {
          "status": "ok" | "error" | "not-found",
          "name": str,
          "steps": [{"step": str, "errors": [str, ...]}, ...],
          "yaml_removed": bool,
        }
    """
    root = Path(pipelines_root)
    try:
        configs = load_pipelines(root)
    except ConfigError as exc:
        return {
            "status": "error",
            "name": name,
            "steps": [{"step": "load_pipelines", "errors": [str(exc)]}],
            "yaml_removed": False,
        }
    if name not in configs:
        return {
            "status": "not-found",
            "name": name,
            "steps": [],
            "yaml_removed": False,
        }
    cfg = configs[name]

    steps: list[dict[str, Any]] = []
    if cfg.source.type == "pg_cdc":
        steps.append(
            {"step": "drop_cdc_slot_and_publication", "errors": _drop_cdc_slot_and_publication(cfg)}
        )
    if not keep_data:
        steps.append({"step": "drop_destination_dataset", "errors": _drop_destination_dataset(cfg)})
    steps.append({"step": "drop_local_state", "errors": _drop_local_state(cfg)})

    yaml_path = _find_yaml_path(name, root)
    yaml_removed = False
    if yaml_path is not None and yaml_path.exists():
        try:
            yaml_path.unlink()
            yaml_removed = True
        except OSError as exc:
            steps.append({"step": "unlink_yaml", "errors": [f"{yaml_path}: {exc}"]})

    any_errors = any(step["errors"] for step in steps)
    return {
        "status": "error" if any_errors else "ok",
        "name": name,
        "steps": steps,
        "yaml_removed": yaml_removed,
    }


def _print_dry_run(name: str, pipelines_root: Path, keep_data: bool) -> None:
    print(f"Would delete pipeline {name!r} from {pipelines_root}:")
    print("  - drop CDC slot + publication (if source.type == pg_cdc)")
    if keep_data:
        print("  - (skipped) destination dataset drop  [--keep-data]")
    else:
        print("  - DROP SCHEMA IF EXISTS <dataset> CASCADE on destination")
    print("  - pipeline.drop() — wipe local .dlt state")
    print(f"  - unlink pipelines/{name}.yml")
    print("Re-run with --yes to execute.")


def cmd_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        _print_dry_run(args.name, args.pipelines_root, args.keep_data)
        return 0

    report = delete_pipeline(args.name, args.pipelines_root, keep_data=args.keep_data)
    if report["status"] == "not-found":
        print(f"pipeline {args.name!r} not found under {args.pipelines_root}", file=sys.stderr)
        return 1

    for step in report["steps"]:  # type: ignore[attr-defined]
        label = step["step"]
        errs = step["errors"]
        if errs:
            print(f"{label}: {len(errs)} error(s)", file=sys.stderr)
            for err in errs:
                print(f"  - {err}", file=sys.stderr)
        else:
            print(f"{label}: ok")

    if report["yaml_removed"]:
        print(
            f"DAG {args.name!r} is no longer generated; "
            "refresh Airflow UI to clear stale references."
        )
    return 1 if report["status"] == "error" else 0
