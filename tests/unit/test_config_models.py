from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from data_pipeline_template.config.models import (
    AlertsConfig,
    AlertSeverity,
    OptionsConfig,
    PipelineConfig,
    SchemaContract,
    SourceFilesystem,
    SourcePgCdc,
    SourceRestApi,
    SourceSqlDatabase,
    WriteDisposition,
)


def _base(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "name": "ok_pipeline",
        "source": {
            "type": "rest_api",
            "connection": "demo",
            "config": {"base_url": "https://x.example/"},
        },
        "sync": {"mode": "full_refresh"},
        "destination": {
            "type": "duckdb",
            "connection": "local",
            "dataset": "raw_x",
        },
        "schedule": {"cron": "0 * * * *"},
    }
    cfg.update(overrides)
    return cfg


@pytest.mark.parametrize(
    "type_value, expected_cls",
    [
        ("rest_api", SourceRestApi),
        ("sql_database", SourceSqlDatabase),
        ("filesystem", SourceFilesystem),
        ("pg_cdc", SourcePgCdc),
    ],
)
def test_discriminator_routes_to_correct_source_subclass(
    type_value: str, expected_cls: type
) -> None:
    raw = _base(source={"type": type_value, "connection": "c", "config": {}})
    # pg_cdc + full_refresh is fine; only cdc mode requires pg_cdc.
    if type_value == "pg_cdc":
        raw["sync"] = {"mode": "full_refresh"}
    cfg = PipelineConfig.model_validate(raw)
    assert isinstance(cfg.source, expected_cls)


def test_unknown_source_type_rejected() -> None:
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(
            _base(source={"type": "unknown_thing", "connection": "c", "config": {}})
        )


def test_extra_keys_rejected_top_level() -> None:
    raw = _base()
    raw["surprise"] = True
    with pytest.raises(ValidationError, match="surprise"):
        PipelineConfig.model_validate(raw)


def test_extra_keys_rejected_nested() -> None:
    raw = _base()
    raw["destination"]["surprise"] = True
    with pytest.raises(ValidationError, match="surprise"):
        PipelineConfig.model_validate(raw)


@pytest.mark.parametrize("bad_name", ["Upper", "has space", "has-hyphen", "1leading_digit", ""])
def test_name_rejects_non_identifier(bad_name: str) -> None:
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(_base(name=bad_name))


def test_incremental_requires_cursor_field() -> None:
    with pytest.raises(ValidationError, match="cursor_field"):
        PipelineConfig.model_validate(
            _base(
                source={
                    "type": "sql_database",
                    "connection": "pg",
                    "config": {},
                },
                sync={"mode": "incremental"},
            )
        )


def test_full_refresh_forbids_cursor_field() -> None:
    with pytest.raises(ValidationError, match="cursor_field"):
        PipelineConfig.model_validate(
            _base(sync={"mode": "full_refresh", "cursor_field": "updated_at"})
        )


def test_cdc_requires_pg_cdc_source() -> None:
    with pytest.raises(ValidationError, match="pg_cdc"):
        PipelineConfig.model_validate(
            _base(
                source={"type": "sql_database", "connection": "pg", "config": {}},
                sync={"mode": "cdc", "primary_key": "id"},
            )
        )


def test_cdc_requires_primary_key() -> None:
    with pytest.raises(ValidationError, match="primary_key"):
        PipelineConfig.model_validate(
            _base(
                source={"type": "pg_cdc", "connection": "pg", "config": {}},
                sync={"mode": "cdc"},
            )
        )


def test_merge_requires_primary_key() -> None:
    with pytest.raises(ValidationError, match="primary_key"):
        PipelineConfig.model_validate(
            _base(
                source={"type": "sql_database", "connection": "pg", "config": {}},
                sync={"mode": "incremental", "cursor_field": "updated_at"},
                options={"write_disposition": "merge"},
            )
        )


@pytest.mark.parametrize("cron", ["0 */2 * * *", "*/5 * * * *", "0 0 1 1 0"])
def test_cron_accepted(cron: str) -> None:
    PipelineConfig.model_validate(_base(schedule={"cron": cron}))


@pytest.mark.parametrize(
    "cron",
    ["every 5 minutes", "0 0 * *", "0 0 * * * *", "", "0;0 * * * *"],
)
def test_cron_rejected(cron: str) -> None:
    with pytest.raises(ValidationError, match="cron"):
        PipelineConfig.model_validate(_base(schedule={"cron": cron}))


@pytest.mark.parametrize(
    "field_path, bad_value",
    [
        (["sync", "mode"], "snapshot"),
        (["destination", "type"], "bigquery"),
        (["options", "write_disposition"], "upsert"),
        (["options", "schema_contract"], "destroy"),
    ],
)
def test_enum_bounds(field_path: list[str], bad_value: str) -> None:
    raw = _base(options={"write_disposition": "append"})
    cursor: Any = raw
    for key in field_path[:-1]:
        cursor = cursor[key]
    cursor[field_path[-1]] = bad_value
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(raw)


def test_defaults_when_options_omitted() -> None:
    raw = _base()
    raw.pop("options", None)
    cfg = PipelineConfig.model_validate(raw)
    assert cfg.options == OptionsConfig(
        write_disposition=WriteDisposition.append,
        schema_contract=SchemaContract.evolve,
    )
    assert cfg.schedule.enabled is True


def test_alerts_defaults() -> None:
    cfg = PipelineConfig.model_validate(_base())
    assert cfg.alerts.severity is AlertSeverity.P2
    assert cfg.alerts.dedup_window_minutes == 15
    assert cfg.alerts.slack_channel is None
    assert cfg.alerts.email_recipients == []
    assert cfg.alerts.on_schema_change is True
    assert cfg.alerts.on_sla_miss is True


def test_pipeline_without_alerts_block_still_valid() -> None:
    raw = _base()
    raw.pop("alerts", None)
    cfg = PipelineConfig.model_validate(raw)
    assert cfg.alerts == AlertsConfig()


@pytest.mark.parametrize("bad", [-1, 1441, 100000])
def test_alerts_dedup_bounds(bad: int) -> None:
    with pytest.raises(ValidationError, match="dedup_window_minutes"):
        PipelineConfig.model_validate(_base(alerts={"dedup_window_minutes": bad}))


def test_alerts_invalid_severity_rejected() -> None:
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(_base(alerts={"severity": "P0"}))


def test_alerts_accepts_overrides() -> None:
    cfg = PipelineConfig.model_validate(
        _base(
            alerts={
                "severity": "P1",
                "dedup_window_minutes": 60,
                "slack_channel": "#oncall-data",
                "email_recipients": ["data@example.com"],
                "on_schema_change": False,
                "on_sla_miss": False,
            }
        )
    )
    assert cfg.alerts.severity is AlertSeverity.P1
    assert cfg.alerts.dedup_window_minutes == 60
    assert cfg.alerts.slack_channel == "#oncall-data"
    assert cfg.alerts.email_recipients == ["data@example.com"]
    assert cfg.alerts.on_schema_change is False
    assert cfg.alerts.on_sla_miss is False
