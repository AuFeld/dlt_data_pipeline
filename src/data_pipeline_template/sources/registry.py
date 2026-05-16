"""Entry-point-driven source builder registry.

Discovers builders via the ``data_pipeline_template.sources`` entry-point group
declared in ``pyproject.toml``. No hardcoded ``{name -> builder}`` mapping —
adding a new source type is a ``pyproject.toml`` edit (or installing an
out-of-tree package that registers the same group), not a registry edit.
"""

from __future__ import annotations

from importlib.metadata import entry_points
from threading import Lock

from ._protocol import Builder

ENTRY_POINT_GROUP = "data_pipeline_template.sources"

_cache: dict[str, Builder] = {}
_cache_lock = Lock()
_loaded = False


class UnknownSourceTypeError(KeyError):
    pass


def _load_all() -> None:
    global _loaded
    with _cache_lock:
        if _loaded:
            return
        for ep in entry_points(group=ENTRY_POINT_GROUP):
            _cache[ep.name] = ep.load()
        _loaded = True


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
