"""Pydantic schema coverage for Segment 12 new fields.

Targets cross-field validators in
[`PipelineConfig._cross_field`](src/dlt_data_pipeline/config/models.py:166)
plus ISO-8601 validators on `BackfillConfig.chunk_size` and `SyncConfig.lookback`.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dlt_data_pipeline.config.models import (
    BackfillConfig,
    DestinationConfig,
    DestinationType,
    OptionsConfig,
    PipelineConfig,
    QualityCheckMode,
    QualityConfig,
    ScheduleConfig,
    SourcePgCdc,
    SourceRestApi,
    SourceSqlDatabase,
    SyncConfig,
    SyncMode,
    WriteDisposition,
)


def _sql_cfg(**overrides: object) -> PipelineConfig:
    """Build a minimal valid sql_database -> postgres incremental config."""
    base: dict[str, object] = {
        "name": "p",
        "source": SourceSqlDatabase(
            type="sql_database", connection="pg_source", config={"tables": ["orders"]}
        ),
        "sync": SyncConfig(mode=SyncMode.incremental, cursor_field="updated_at", primary_key="id"),
        "destination": DestinationConfig(
            type=DestinationType.postgres, connection="pg_warehouse", dataset="raw"
        ),
        "schedule": ScheduleConfig(cron="0 * * * *", enabled=True),
        "options": OptionsConfig(write_disposition=WriteDisposition.merge),
    }
    base.update(overrides)
    return PipelineConfig(**base)  # type: ignore[arg-type]


def test_backfill_block_accepted() -> None:
    cfg = _sql_cfg(
        sync=SyncConfig(
            mode=SyncMode.incremental,
            cursor_field="updated_at",
            primary_key="id",
            backfill=BackfillConfig(chunk_size="P7D"),
        ),
    )
    assert cfg.sync.backfill is not None
    assert cfg.sync.backfill.chunk_size == "P7D"


def test_backfill_rejected_when_mode_full_refresh() -> None:
    with pytest.raises(ValidationError, match="sync.backfill requires sync.mode"):
        PipelineConfig(
            name="p",
            source=SourceRestApi(
                type="rest_api",
                connection="api",
                config={"base_url": "https://x/", "endpoints": ["t"]},
            ),
            sync=SyncConfig(
                mode=SyncMode.full_refresh,
                backfill=BackfillConfig(chunk_size="P1D"),
            ),
            destination=DestinationConfig(type=DestinationType.duckdb, connection="d", dataset="r"),
            schedule=ScheduleConfig(cron="0 * * * *", enabled=True),
        )


def test_backfill_rejected_when_mode_cdc() -> None:
    with pytest.raises(ValidationError, match="sync.backfill requires sync.mode"):
        PipelineConfig(
            name="p",
            source=SourcePgCdc(
                type="pg_cdc",
                connection="c",
                config={
                    "slot_name": "s",
                    "publication_name": "pub",
                    "tables": ["t"],
                },
            ),
            sync=SyncConfig(
                mode=SyncMode.cdc,
                primary_key="id",
                backfill=BackfillConfig(chunk_size="P1D"),
            ),
            destination=DestinationConfig(
                type=DestinationType.postgres, connection="d", dataset="r"
            ),
            schedule=ScheduleConfig(cron="0 * * * *", enabled=True),
        )


def test_chunk_size_rejects_non_iso8601() -> None:
    with pytest.raises(ValidationError, match="chunk_size must be an ISO-8601 duration"):
        BackfillConfig(chunk_size="7days")


def test_lookback_rejects_non_iso8601() -> None:
    with pytest.raises(ValidationError, match="lookback must be an ISO-8601 duration"):
        SyncConfig(mode=SyncMode.incremental, cursor_field="u", lookback="1h")


def test_tolerance_requires_cursor_field() -> None:
    with pytest.raises(ValidationError, match="tolerance_seconds requires sync.cursor_field"):
        PipelineConfig(
            name="p",
            source=SourceRestApi(
                type="rest_api",
                connection="api",
                config={"base_url": "https://x/", "endpoints": ["t"]},
            ),
            sync=SyncConfig(mode=SyncMode.full_refresh, tolerance_seconds=30),
            destination=DestinationConfig(type=DestinationType.duckdb, connection="d", dataset="r"),
            schedule=ScheduleConfig(cron="0 * * * *", enabled=True),
        )


def test_lookback_requires_cursor_field() -> None:
    with pytest.raises(ValidationError, match="lookback requires sync.cursor_field"):
        PipelineConfig(
            name="p",
            source=SourceRestApi(
                type="rest_api",
                connection="api",
                config={"base_url": "https://x/", "endpoints": ["t"]},
            ),
            sync=SyncConfig(mode=SyncMode.full_refresh, lookback="PT1H"),
            destination=DestinationConfig(type=DestinationType.duckdb, connection="d", dataset="r"),
            schedule=ScheduleConfig(cron="0 * * * *", enabled=True),
        )


def test_tolerance_seconds_upper_bound() -> None:
    with pytest.raises(ValidationError):
        SyncConfig(mode=SyncMode.incremental, cursor_field="updated_at", tolerance_seconds=86401)


def test_row_count_check_rejected_for_rest_api() -> None:
    with pytest.raises(ValidationError, match="quality.row_count_check requires source.type"):
        PipelineConfig(
            name="p",
            source=SourceRestApi(
                type="rest_api",
                connection="api",
                config={"base_url": "https://x/", "endpoints": ["t"]},
            ),
            sync=SyncConfig(mode=SyncMode.full_refresh),
            destination=DestinationConfig(type=DestinationType.duckdb, connection="d", dataset="r"),
            schedule=ScheduleConfig(cron="0 * * * *", enabled=True),
            quality=QualityConfig(row_count_check=True),
        )


def test_row_count_check_rejected_for_databricks() -> None:
    with pytest.raises(ValidationError, match="not supported for databricks"):
        _sql_cfg(
            destination=DestinationConfig(
                type=DestinationType.databricks, connection="dbx", dataset="raw"
            ),
            quality=QualityConfig(row_count_check=True),
        )


def test_row_count_check_default_check_mode_is_cross_cluster() -> None:
    cfg = _sql_cfg(quality=QualityConfig(row_count_check=True))
    assert cfg.quality.check_mode == QualityCheckMode.cross_cluster


def test_row_count_check_accepts_same_cluster() -> None:
    cfg = _sql_cfg(
        quality=QualityConfig(row_count_check=True, check_mode=QualityCheckMode.same_cluster)
    )
    assert cfg.quality.check_mode == QualityCheckMode.same_cluster
