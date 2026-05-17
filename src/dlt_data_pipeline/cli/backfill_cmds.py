"""`run-backfill` CLI subcommand (Segment 12).

Pure helper ``run_backfill_pipeline`` returns a structured report; the CLI
wrapper ``cmd_run_backfill`` prints it and translates to an exit code.
Mirrors the pure-helper + print-wrapper split established by
``pipelines_cmds.py``.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from dlt_data_pipeline import pipeline_factory
from dlt_data_pipeline.config.loader import ConfigError


def _parse_iso_datetime(value: str) -> datetime:
    """Accept any ISO-8601 timestamp; reject naive datetimes (cursor compares
    naive vs aware would silently drop rows).
    """
    # ``fromisoformat`` accepts trailing 'Z' from Python 3.11.
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"{value!r}: timestamp must be timezone-aware (e.g. 2025-01-01T00:00:00Z)")
    return dt


def run_backfill_pipeline(
    name: str,
    start: datetime,
    end: datetime,
    pipelines_root: str | Path = "pipelines",
) -> dict[str, object]:
    """Drive ``pipeline_factory.run_backfill`` and return a structured report.

    Returns:
        {
          "status": "ok" | "error",
          "name": str,
          "start": str, "end": str,
          "chunks": int,
          "errors": [str, ...],
        }
    """
    try:
        infos = pipeline_factory.run_backfill(name, start, end, pipelines_root=pipelines_root)
    except (ValueError, KeyError, ConfigError) as exc:
        return {
            "status": "error",
            "name": name,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "chunks": 0,
            "errors": [str(exc)],
        }
    return {
        "status": "ok",
        "name": name,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "chunks": len(infos),
        "errors": [],
    }


def cmd_run_backfill(args: argparse.Namespace) -> int:
    try:
        start = _parse_iso_datetime(args.start)
        end = _parse_iso_datetime(args.end)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    report = run_backfill_pipeline(args.name, start, end, args.pipelines_root)
    if report["status"] == "error":
        for err in report["errors"]:  # type: ignore[attr-defined]
            print(err, file=sys.stderr)
        return 1

    print(
        f"backfilled {report['name']} from {report['start']} to {report['end']} "
        f"in {report['chunks']} chunk(s)"
    )
    return 0
