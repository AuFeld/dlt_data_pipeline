"""CLI entry point: ``python -m data_pipeline_template run <name>``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from data_pipeline_template import pipeline_factory


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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "run":
        load_info = pipeline_factory.run(args.name, pipelines_root=args.pipelines_root)
        print(load_info)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
