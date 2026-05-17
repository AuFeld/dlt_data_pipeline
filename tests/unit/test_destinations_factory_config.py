"""Destination config-validation tests: import-extra guards + registry surface."""

from __future__ import annotations

import sys
from types import ModuleType

import dlt.destinations as dlt_destinations
import pytest

from data_pipeline_template.config.models import DestinationConfig, DestinationType
from data_pipeline_template.destinations import registry
from data_pipeline_template.destinations.factory import build_destination


def test_snowflake_missing_extra_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If dlt[snowflake] isn't installed, build_destination must raise a
    RuntimeError naming the extra, not a bare ImportError. Simulates the
    missing extra by replacing dlt.destinations with a stub module that
    has no 'snowflake' attribute, so ``from dlt.destinations import
    snowflake`` raises ImportError at build time."""

    stub = ModuleType("dlt.destinations")
    for attr in ("duckdb", "postgres"):
        if hasattr(dlt_destinations, attr):
            setattr(stub, attr, getattr(dlt_destinations, attr))
    monkeypatch.setitem(sys.modules, "dlt.destinations", stub)

    cfg = DestinationConfig(type=DestinationType.snowflake, connection="sf_test", dataset="raw")
    with pytest.raises(RuntimeError, match="snowflake.*extra"):
        build_destination(cfg)


def test_registry_list_types_covers_all_enum_members() -> None:
    listed = registry.list_types()
    assert set(listed) == {dt.value for dt in DestinationType}


def test_registry_describe_snowflake() -> None:
    meta = registry.describe("snowflake")
    assert meta.env_var_template == "DESTINATION__SNOWFLAKE__<CONNECTION>__CREDENTIALS"
    assert "snowflake" in meta.notes.lower()
    assert "extra" in meta.notes.lower()


def test_registry_describe_databricks_documents_deferral() -> None:
    meta = registry.describe("databricks")
    assert meta.env_var_template is None
    notes_lower = meta.notes.lower()
    desc_lower = meta.description.lower()
    assert "not in active use" in desc_lower or "not implemented" in notes_lower


def test_registry_describe_unknown_raises_keyerror() -> None:
    with pytest.raises(KeyError, match="unknown destination type"):
        registry.describe("clickhouse")


def test_registry_describe_resolve_env_var_snowflake() -> None:
    meta = registry.describe("snowflake")
    resolved = meta.resolve_env_var("my_warehouse")
    assert resolved == "DESTINATION__SNOWFLAKE__MY_WAREHOUSE__CREDENTIALS"


def test_registry_describe_resolve_env_var_databricks_none() -> None:
    meta = registry.describe("databricks")
    assert meta.resolve_env_var("any_conn") is None
