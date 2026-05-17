"""CLI entry point: ``python -m dlt_data_pipeline <command> ...``.

Subcommands:
  run                         — execute one pipeline by name (Segment 3+).
  run-backfill                — chunked historical load for an incremental pipeline (Segment 12).
  config schema               — dump pipelines/_schema.json from pydantic models.
  sources list                — list registered source types (entry-point group).
  sources describe <type>     — show env-var template + allowed config keys.
  pipelines validate [name]   — parse + validate one or all pipelines/*.yml.
  pipelines doctor            — probe expected env vars / .dlt secrets per pipeline.
  pipelines delete <name>     — tear down CDC slot, dataset, state, YAML (Segment 12).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

from dlt_data_pipeline import pipeline_factory
from dlt_data_pipeline.cli import (
    backfill_cmds,
    config_cmds,
    delete_cmds,
    pipelines_cmds,
    sources_cmds,
)
from dlt_data_pipeline.observability.log_filter import install_secret_scrub


def _cmd_run(args: argparse.Namespace) -> int:
    if args.no_load:
        result = pipeline_factory.run_dry(
            args.name, pipelines_root=args.pipelines_root, limit=args.limit
        )
        print(result)
        return 0
    load_info = pipeline_factory.run(
        args.name, pipelines_root=args.pipelines_root, limit=args.limit
    )
    print(load_info)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m dlt_data_pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run one pipeline by name.")
    run_p.add_argument("name", help="Pipeline name (the `name:` field of a pipelines/*.yml).")
    run_p.add_argument(
        "--pipelines-root",
        type=Path,
        default=Path("pipelines"),
        help="Directory containing pipelines/*.yml (default: ./pipelines).",
    )
    run_p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap yields per resource via DltSource.add_limit (dry-run aid).",
    )
    run_p.add_argument(
        "--no-load",
        action="store_true",
        help="Skip destination write: extract + normalize only.",
    )
    run_p.set_defaults(_handler=_cmd_run)

    backfill_p = sub.add_parser(
        "run-backfill",
        help="Chunked historical load for an incremental pipeline.",
    )
    backfill_p.add_argument("name", help="Pipeline name (the `name:` field of a pipelines/*.yml).")
    backfill_p.add_argument(
        "--start", required=True, help="ISO-8601 start timestamp (timezone-aware)."
    )
    backfill_p.add_argument(
        "--end", required=True, help="ISO-8601 end timestamp (timezone-aware), exclusive."
    )
    backfill_p.add_argument(
        "--pipelines-root",
        type=Path,
        default=Path("pipelines"),
        help="Directory containing pipelines/*.yml (default: ./pipelines).",
    )
    backfill_p.set_defaults(_handler=backfill_cmds.cmd_run_backfill)

    config_p = sub.add_parser("config", help="Config-layer utilities.")
    config_sub = config_p.add_subparsers(dest="subcommand", required=True)
    schema_p = config_sub.add_parser("schema", help="Dump PipelineConfig JSON Schema.")
    schema_p.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the on-disk schema differs from the regenerated one.",
    )
    schema_p.add_argument(
        "--out",
        type=Path,
        default=None,
        help=f"Output path (default: {config_cmds.DEFAULT_SCHEMA_PATH}).",
    )
    schema_p.set_defaults(_handler=config_cmds.cmd_schema)

    sources_p = sub.add_parser("sources", help="Source-type introspection.")
    sources_sub = sources_p.add_subparsers(dest="subcommand", required=True)
    list_p = sources_sub.add_parser("list", help="List registered source types.")
    list_p.set_defaults(_handler=sources_cmds.cmd_list)
    describe_p = sources_sub.add_parser("describe", help="Describe one source type.")
    describe_p.add_argument("type", help="Source type name (e.g. sql_database).")
    describe_p.set_defaults(_handler=sources_cmds.cmd_describe)

    pipelines_p = sub.add_parser("pipelines", help="Pipeline introspection.")
    pipelines_sub = pipelines_p.add_subparsers(dest="subcommand", required=True)
    validate_p = pipelines_sub.add_parser("validate", help="Validate one or all pipelines/*.yml.")
    validate_p.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Optional pipeline name; defaults to validating every YAML.",
    )
    validate_p.add_argument(
        "--pipelines-root",
        type=Path,
        default=Path("pipelines"),
        help="Directory containing pipelines/*.yml (default: ./pipelines).",
    )
    validate_p.set_defaults(_handler=pipelines_cmds.cmd_validate)
    doctor_p = pipelines_sub.add_parser(
        "doctor", help="Probe expected env vars / .dlt secrets per pipeline."
    )
    doctor_p.add_argument(
        "--pipelines-root",
        type=Path,
        default=Path("pipelines"),
        help="Directory containing pipelines/*.yml (default: ./pipelines).",
    )
    doctor_p.set_defaults(_handler=pipelines_cmds.cmd_doctor)

    delete_p = pipelines_sub.add_parser(
        "delete",
        help="Tear down a pipeline (CDC slot, dataset, local state, YAML).",
    )
    delete_p.add_argument("name", help="Pipeline name to delete.")
    delete_p.add_argument(
        "--yes",
        action="store_true",
        help="Required to actually delete. Without it, prints what would happen.",
    )
    delete_p.add_argument(
        "--keep-data",
        action="store_true",
        help="Skip the destination dataset drop (preserves loaded data).",
    )
    delete_p.add_argument(
        "--pipelines-root",
        type=Path,
        default=Path("pipelines"),
        help="Directory containing pipelines/*.yml (default: ./pipelines).",
    )
    delete_p.set_defaults(_handler=delete_cmds.cmd_delete)

    return parser


def main(argv: list[str] | None = None) -> int:
    install_secret_scrub()
    args = _build_parser().parse_args(argv)
    handler: Callable[[argparse.Namespace], int] = args._handler
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
