"""Registry tests — proves entry-point discovery + cache + error handling."""

from __future__ import annotations

import pytest

from dlt_data_pipeline.sources import registry


def test_registry_discovers_rest_api_via_entry_points() -> None:
    # No direct import of dlt_data_pipeline.sources.rest_api here — discovery
    # must be entry-point-driven per Design principle #3.
    builder = registry.get_builder("rest_api")
    assert callable(builder)


def test_registry_lists_all_four_builtins() -> None:
    assert set(registry.registered_types()) >= {"rest_api", "sql_database", "filesystem", "pg_cdc"}


def test_unknown_source_type_raises() -> None:
    with pytest.raises(registry.UnknownSourceTypeError) as exc:
        registry.get_builder("definitely_not_a_real_source")
    msg = str(exc.value)
    assert "definitely_not_a_real_source" in msg
    assert "rest_api" in msg


def test_builder_cache_returns_same_object() -> None:
    a = registry.get_builder("rest_api")
    b = registry.get_builder("rest_api")
    assert a is b
