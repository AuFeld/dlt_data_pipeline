"""`sources list` / `sources describe` subcommands.

Pure data helpers (``list_sources`` / ``describe_source``) are reused by the
MCP server. CLI wrappers (``cmd_list`` / ``cmd_describe``) own the printing
and exit-code translation.
"""

from __future__ import annotations

import argparse
import sys

from dlt_data_pipeline.sources import registry


def list_sources() -> list[str]:
    """Return sorted list of registered source type names."""
    return registry.registered_types()


def describe_source(source_type: str) -> dict[str, object]:
    """Return metadata for one source type.

    Raises ``registry.UnknownSourceTypeError`` / ``MissingSourceMetadataError``
    on bad input — callers translate to their own exit semantics.
    """
    meta = registry.get_metadata(source_type)
    return {
        "type": source_type,
        "description": meta.description,
        "env_var_template": meta.env_var_template,
        "required_config_keys": list(meta.required_config_keys),
        "allowed_config_keys": list(meta.allowed_config_keys),
        "notes": meta.notes,
    }


def cmd_list(args: argparse.Namespace) -> int:
    del args
    for name in list_sources():
        print(name)
    return 0


def cmd_describe(args: argparse.Namespace) -> int:
    try:
        info = describe_source(args.type)
    except (registry.UnknownSourceTypeError, registry.MissingSourceMetadataError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    env = info["env_var_template"] or "(none — source needs no credentials)"
    required = ", ".join(info["required_config_keys"]) or "(none)"  # type: ignore[arg-type]
    allowed = ", ".join(info["allowed_config_keys"]) or "(free-form)"  # type: ignore[arg-type]

    print(f"type: {info['type']}")
    print(f"description: {info['description']}")
    print(f"env_var_template: {env}")
    print(f"required_config_keys: {required}")
    print(f"allowed_config_keys: {allowed}")
    if info["notes"]:
        print(f"notes: {info['notes']}")
    return 0
