"""Scaffold a new pipelines/<name>.yml.

Source + destination metadata drive the skeleton; required source-config keys
become typed YAML placeholders (so ``pipelines validate`` passes on first run);
TODO markers flag what a human still has to fill (connection name, cron,
real key values, write disposition). Calls ``pipelines validate`` after write
and unlinks the file on failure so a broken YAML never sits on disk.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from data_pipeline_template.cli.pipelines_cmds import validate_pipelines
from data_pipeline_template.destinations import registry as dest_registry
from data_pipeline_template.sources import registry as src_registry

_HEADER = "# yaml-language-server: $schema=./_schema.json\n"


def _placeholder_for(key: str) -> str:
    """Return a YAML placeholder value for a required source-config key.

    Plural keys (ending in 's') -> empty list; otherwise empty string. The
    pydantic schema accepts both because ``source.config`` is typed as
    ``dict[str, Any]``; the dlt source builder will (correctly) reject these
    at run time until the human fills them in.
    """
    return "[]" if key.endswith("s") else '""'


def _render_required_keys(keys: tuple[str, ...]) -> str:
    if not keys:
        return "    # (no required keys for this source)\n"
    lines: list[str] = []
    for key in keys:
        lines.append(f"    {key}: {_placeholder_for(key)}   # TODO: required — fill in")
    return "\n".join(lines) + "\n"


def _build_yaml(
    *,
    name: str,
    source_type: str,
    dest_type: str,
) -> str:
    src_meta = src_registry.get_metadata(source_type)
    dest_meta = dest_registry.describe(dest_type)
    src_conn = f"{source_type}_conn"
    dest_conn = f"{dest_type}_conn"
    src_env = src_meta.resolve_env_var(src_conn) or "(no credentials required)"
    dest_env = dest_meta.resolve_env_var(dest_conn) or "(no credentials required)"
    allowed = ", ".join(src_meta.allowed_config_keys) or "(free-form)"
    dest_notes_first = (dest_meta.notes.splitlines() or [""])[0]

    body = (
        _HEADER
        + f"name: {name}\n"
        + "source:\n"
        + f"  type: {source_type}\n"
        + f"  connection: {src_conn}  # TODO: rename to your logical connection\n"
        + f"  # credentials env var: {src_env}\n"
        + f"  # allowed config keys: {allowed}\n"
        + "  config:\n"
        + _render_required_keys(src_meta.required_config_keys)
        + "sync:\n"
        + "  mode: full_refresh  # TODO: full_refresh | incremental | cdc\n"
        + "destination:\n"
        + f"  type: {dest_type}\n"
        + f"  connection: {dest_conn}  # TODO: rename to your logical connection\n"
        + f"  dataset: raw_{name}\n"
        + f"  # credentials env var: {dest_env}\n"
    )
    if dest_notes_first:
        body += f"  # {dest_notes_first}\n"
    body += (
        "schedule:\n"
        '  cron: "0 6 * * *"  # TODO: pick a cron schedule\n'
        "  enabled: true\n"
        "options:\n"
        "  write_disposition: append  # TODO: append | replace | merge\n"
        "  schema_contract: evolve    # TODO: evolve | freeze | discard_row\n"
        "alerts:\n"
        "  severity: P2                  # TODO: P1 | P2\n"
        "  dedup_window_minutes: 15\n"
        "  on_schema_change: true\n"
        "  on_sla_miss: true\n"
        '  # slack_channel: "#data-alerts"\n'
        "  # email_recipients: []\n"
    )
    return body


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="scripts/new_pipeline.py")
    p.add_argument("name", help="Pipeline name (lowercase identifier).")
    p.add_argument(
        "--source",
        required=True,
        choices=src_registry.registered_types(),
        help="Source type (entry-point-registered).",
    )
    p.add_argument(
        "--dest",
        required=True,
        choices=dest_registry.list_types(),
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
    out = args.pipelines_root / f"{args.name}.yml"
    if out.exists():
        print(f"{out} already exists.", file=sys.stderr)
        return 1
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_build_yaml(name=args.name, source_type=args.source, dest_type=args.dest))

    report = validate_pipelines(args.pipelines_root, args.name)
    if report["status"] != "ok":
        for err in report["errors"]:  # type: ignore[union-attr]
            print(err, file=sys.stderr)
        out.unlink(missing_ok=True)
        print(f"Removed {out}: validation failed.", file=sys.stderr)
        return 2
    print(
        f"Wrote {out}.\n"
        f"Next: fill TODOs, then run "
        f"`python -m data_pipeline_template pipelines validate {args.name}`."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
