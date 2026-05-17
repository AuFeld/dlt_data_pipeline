"""Destination-type introspection helpers.

Destinations are enum-dispatched (not entry-point pluggable), so this module
is a thin facade over ``destinations._metadata.METADATA`` that mirrors the
``sources.registry`` API. The scaffolder (Segment 9) and MCP server use this
to drive ``destinations describe`` flows symmetrically with sources.
"""

from __future__ import annotations

from data_pipeline_template.config.models import DestinationType
from data_pipeline_template.destinations._metadata import (
    METADATA,
    DestinationTypeMetadata,
)


def list_types() -> list[str]:
    """Return all known destination type names in declaration order."""
    return [dt.value for dt in DestinationType]


def describe(type_name: str) -> DestinationTypeMetadata:
    """Return metadata for ``type_name``; raise ``KeyError`` on unknown."""
    try:
        dt = DestinationType(type_name)
    except ValueError as exc:
        raise KeyError(f"unknown destination type {type_name!r}; known: {list_types()}") from exc
    return METADATA[dt]
