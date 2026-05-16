"""CLI entry point: ``python -m data_pipeline_template <command> ...``.

Subcommands:
  run                         — execute one pipeline by name (Segment 3+).
  config schema               — dump pipelines/_schema.json from pydantic models.
  sources list                — list registered source types (entry-point group).
  sources describe <type>     — show env-var template + allowed config keys.
  pipelines validate [name]   — parse + validate one or all pipelines/*.yml.
  pipelines doctor            — probe expected env vars / .dlt secrets per pipeline.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

from data_pipeline_template import pipeline_factory
from data_pipeline_template.cli import config_cmds, pipelines_cmds, sources_cmds


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
    parser = argparse.ArgumentParser(prog="python -m data_pipeline_template")
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

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    handler: Callable[[argparse.Namespace], int] = args._handler
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
