"""Scaffold a new pipelines/<name>.yml.

Pulled forward from Segment 9 in minimal form so the `/add-pipeline` slash
skill (Segment 6.6) has something to call. Reads source + destination
metadata to populate the skeleton; emits TODO markers on the fields a human
or agent still has to fill (connection name, required config keys, sync
mode, cron). Does not validate connectivity — chain
`python -m data_pipeline_template pipelines validate <name>` after.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from data_pipeline_template.config.models import DestinationType
from data_pipeline_template.sources import registry

TEMPLATE = """\
# yaml-language-server: $schema=./_schema.json
name: {name}
source:
  type: {source_type}
  connection: {source_connection}  # TODO: rename to your logical connection
  config:
    # TODO: required keys: {required}
    # TODO: allowed keys: {allowed}
sync:
  mode: full_refresh  # TODO: full_refresh | incremental | cdc
destination:
  type: {dest_type}
  connection: {dest_connection}  # TODO: rename to your logical connection
  dataset: raw_{name}
schedule:
  cron: "0 6 * * *"  # TODO: pick a cron schedule
  enabled: true
options:
  write_disposition: append  # TODO: append | replace | merge
  schema_contract: evolve    # TODO: evolve | freeze | discard_row
"""


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="scripts/new_pipeline.py")
    p.add_argument("name", help="Pipeline name (lowercase identifier).")
    p.add_argument(
        "--source",
        required=True,
        choices=registry.registered_types(),
        help="Source type (entry-point-registered).",
    )
    p.add_argument(
        "--dest",
        required=True,
        choices=[d.value for d in DestinationType],
        help="Destination type.",
    )
    p.add_argument(
        "--pipelines-root",
        type=Path,
        default=Path("pipelines"),
        help="Output directory (default: ./pipelines).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    src_meta = registry.get_metadata(args.source)
    out = args.pipelines_root / f"{args.name}.yml"
    if out.exists():
        print(f"{out} already exists.", file=sys.stderr)
        return 1
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        TEMPLATE.format(
            name=args.name,
            source_type=args.source,
            source_connection=f"{args.source}_conn",
            required=", ".join(src_meta.required_config_keys) or "(none)",
            allowed=", ".join(src_meta.allowed_config_keys) or "(free-form)",
            dest_type=args.dest,
            dest_connection=f"{args.dest}_conn",
        )
    )
    print(
        f"Wrote {out}.\n"
        f"Next: fill TODOs, then "
        f"`python -m data_pipeline_template pipelines validate {args.name}`."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
