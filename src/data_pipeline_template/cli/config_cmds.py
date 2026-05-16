"""`config schema` subcommand: dump pydantic JSON Schema for PipelineConfig."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from data_pipeline_template.config.models import PipelineConfig

DEFAULT_SCHEMA_PATH = Path("pipelines") / "_schema.json"


def _render_schema() -> str:
    schema = PipelineConfig.model_json_schema()
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def cmd_schema(args: argparse.Namespace) -> int:
    out: Path = args.out or DEFAULT_SCHEMA_PATH
    rendered = _render_schema()
    if args.check:
        if not out.exists():
            print(
                f"{out} does not exist. Run `python -m data_pipeline_template config schema`.",
                file=sys.stderr,
            )
            return 1
        on_disk = out.read_text()
        if on_disk != rendered:
            print(
                f"{out} is stale. Regenerate with "
                f"`python -m data_pipeline_template config schema`.",
                file=sys.stderr,
            )
            return 1
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered)
    print(f"Wrote {out}")
    return 0
