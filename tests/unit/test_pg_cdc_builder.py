"""Unit tests for the pg_cdc source builder.

Hermetic — every test either exercises validation paths that fail before any
DB call, or monkeypatches the vendored ``init_replication`` /
``replication_resource`` so no live Postgres is required. The integration
test under ``tests/integration/test_pg_cdc.py`` covers the real-DB flow,
gated on ``RUN_LIVE_PG_CDC=1``.
"""

from __future__ import annotations

from typing import Any

import dlt
import pytest
from dlt.extract import DltSource

from dlt_data_pipeline.config.models import SourcePgCdc, SourceRestApi
from dlt_data_pipeline.sources import pg_cdc

CREDS_ENV = "SOURCES__PG_CDC__PG_SOURCE_TEST__CREDENTIALS"


def _stub_vendor(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    """Replace init_replication + replication_resource with capture stubs.

    Returns a dict of call-record lists so tests can assert on the arguments
    the builder forwarded to the vendor.
    """
    calls: dict[str, list[Any]] = {"init": [], "resource": []}

    def _fake_init(**kwargs: Any) -> None:
        calls["init"].append(kwargs)
        return None

    def _fake_resource(**kwargs: Any) -> Any:
        calls["resource"].append(kwargs)

        @dlt.resource(name=kwargs["slot_name"])
        def _streaming() -> Any:
            yield from []

        return _streaming

    monkeypatch.setattr(pg_cdc, "init_replication", _fake_init)
    monkeypatch.setattr(pg_cdc, "replication_resource", _fake_resource)
    return calls


def _cfg(**overrides: Any) -> SourcePgCdc:
    base: dict[str, Any] = {
        "slot_name": "test_slot",
        "publication_name": "test_pub",
        "tables": ["orders"],
    }
    base.update(overrides.pop("config_extra", {}))
    return SourcePgCdc(
        type="pg_cdc",
        connection=overrides.pop("connection", "pg_source_test"),
        config=base,
    )


def test_rejects_wrong_config_type() -> None:
    cfg = SourceRestApi(
        type="rest_api",
        connection="x",
        config={"base_url": "https://example.com/", "endpoints": ["e"]},
    )
    with pytest.raises(TypeError, match="pg_cdc builder received wrong config type"):
        pg_cdc.builder(cfg)


@pytest.mark.parametrize("missing", ["slot_name", "publication_name", "tables"])
def test_missing_required_key_raises(missing: str) -> None:
    raw: dict[str, Any] = {
        "slot_name": "s",
        "publication_name": "p",
        "tables": ["t"],
    }
    del raw[missing]
    cfg = SourcePgCdc(type="pg_cdc", connection="pg_source_test", config=raw)
    with pytest.raises(ValueError, match=f"source.config.{missing} is required"):
        pg_cdc.builder(cfg)


def test_empty_tables_raises() -> None:
    cfg = SourcePgCdc(
        type="pg_cdc",
        connection="pg_source_test",
        config={"slot_name": "s", "publication_name": "p", "tables": []},
    )
    with pytest.raises(ValueError, match="source.config.tables is required"):
        pg_cdc.builder(cfg)


def test_unknown_config_key_raises() -> None:
    cfg = SourcePgCdc(
        type="pg_cdc",
        connection="pg_source_test",
        config={
            "slot_name": "s",
            "publication_name": "p",
            "tables": ["t"],
            "bogus": True,
        },
    )
    with pytest.raises(ValueError, match="unknown config keys"):
        pg_cdc.builder(cfg)


def test_missing_credentials_names_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CREDS_ENV, raising=False)
    with pytest.raises(ValueError, match=CREDS_ENV):
        pg_cdc.builder(_cfg())


def test_happy_path_returns_dlt_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(CREDS_ENV, "postgresql://u:p@h:5432/db")
    calls = _stub_vendor(monkeypatch)
    source = pg_cdc.builder(_cfg())

    assert isinstance(source, DltSource)
    assert source.name == "pg_source_test"
    # One init call, one streaming resource call.
    assert len(calls["init"]) == 1
    assert len(calls["resource"]) == 1
    init_kwargs = calls["init"][0]
    assert init_kwargs["slot_name"] == "test_slot"
    assert init_kwargs["pub_name"] == "test_pub"
    assert init_kwargs["schema_name"] == "public"
    assert init_kwargs["table_names"] == ["orders"]
    assert init_kwargs["persist_snapshots"] is True
    assert init_kwargs["reset"] is False
    # Streaming resource is yielded by the @dlt.source generator.
    assert "test_slot" in set(source.resources.keys())


def test_reset_flag_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDS_ENV, "postgresql://u:p@h:5432/db")
    calls = _stub_vendor(monkeypatch)
    pg_cdc.builder(_cfg(config_extra={"reset": True}))
    assert calls["init"][0]["reset"] is True


def test_schema_override_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDS_ENV, "postgresql://u:p@h:5432/db")
    calls = _stub_vendor(monkeypatch)
    pg_cdc.builder(_cfg(config_extra={"schema": "warehouse"}))
    assert calls["init"][0]["schema_name"] == "warehouse"


def test_slot_already_exists_runtimeerror_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Steady-state runs (slot already exists) must not raise."""
    monkeypatch.setenv(CREDS_ENV, "postgresql://u:p@h:5432/db")

    def _init_existing(**_: Any) -> None:
        raise RuntimeError("Cannot create snapshots because slot test_slot is already created.")

    def _fake_resource(**kwargs: Any) -> Any:
        @dlt.resource(name=kwargs["slot_name"])
        def _streaming() -> Any:
            yield from []

        return _streaming

    monkeypatch.setattr(pg_cdc, "init_replication", _init_existing)
    monkeypatch.setattr(pg_cdc, "replication_resource", _fake_resource)

    source = pg_cdc.builder(_cfg())
    assert isinstance(source, DltSource)
    assert "test_slot" in set(source.resources.keys())


def test_unexpected_runtimeerror_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDS_ENV, "postgresql://u:p@h:5432/db")

    def _init_boom(**_: Any) -> None:
        raise RuntimeError("unrelated failure")

    monkeypatch.setattr(pg_cdc, "init_replication", _init_boom)
    with pytest.raises(RuntimeError, match="unrelated failure"):
        pg_cdc.builder(_cfg())
