"""Entry-point-driven source builder + metadata registry.

Discovers builders via the ``data_pipeline_template.sources`` entry-point group
and matching introspection metadata via
``data_pipeline_template.sources.metadata``, both declared in
``pyproject.toml``. No hardcoded ``{name -> builder}`` mapping — adding a new
source type is a ``pyproject.toml`` edit (or installing an out-of-tree package
that registers the same groups), not a registry edit.
"""

from __future__ import annotations

from importlib.metadata import entry_points
from threading import Lock

from ._metadata import SourceTypeMetadata
from ._protocol import Builder

ENTRY_POINT_GROUP = "data_pipeline_template.sources"
ENTRY_POINT_METADATA_GROUP = "data_pipeline_template.sources.metadata"

_cache: dict[str, Builder] = {}
_cache_lock = Lock()
_loaded = False

_metadata_cache: dict[str, SourceTypeMetadata] = {}
_metadata_lock = Lock()
_metadata_loaded = False


class UnknownSourceTypeError(KeyError):
    pass


class MissingSourceMetadataError(KeyError):
    """Builder registered without a matching metadata entry point."""


def _load_all() -> None:
    global _loaded
    with _cache_lock:
        if _loaded:
            return
        for ep in entry_points(group=ENTRY_POINT_GROUP):
            _cache[ep.name] = ep.load()
        _loaded = True


def _load_all_metadata() -> None:
    global _metadata_loaded
    with _metadata_lock:
        if _metadata_loaded:
            return
        for ep in entry_points(group=ENTRY_POINT_METADATA_GROUP):
            obj = ep.load()
            if not isinstance(obj, SourceTypeMetadata):
                raise TypeError(
                    f"entry point {ENTRY_POINT_METADATA_GROUP}:{ep.name} "
                    f"resolved to {type(obj).__name__}, expected SourceTypeMetadata"
                )
            _metadata_cache[ep.name] = obj
        _metadata_loaded = True


def get_builder(source_type: str) -> Builder:
    _load_all()
    try:
        return _cache[source_type]
    except KeyError:
        registered = sorted(_cache)
        raise UnknownSourceTypeError(
            f"unknown source type {source_type!r}; registered: {registered}"
        ) from None


def registered_types() -> list[str]:
    _load_all()
    return sorted(_cache)


def get_metadata(source_type: str) -> SourceTypeMetadata:
    _load_all()
    if source_type not in _cache:
        registered = sorted(_cache)
        raise UnknownSourceTypeError(
            f"unknown source type {source_type!r}; registered: {registered}"
        )
    _load_all_metadata()
    try:
        return _metadata_cache[source_type]
    except KeyError:
        raise MissingSourceMetadataError(
            f"source type {source_type!r} has no metadata entry. "
            f'Add an entry under [project.entry-points."{ENTRY_POINT_METADATA_GROUP}"] '
            f"in pyproject.toml pointing at the source's `metadata` constant."
        ) from None


def all_metadata() -> dict[str, SourceTypeMetadata]:
    _load_all()
    _load_all_metadata()
    return {name: _metadata_cache[name] for name in sorted(_cache) if name in _metadata_cache}
