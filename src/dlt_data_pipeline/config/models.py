"""Pydantic v2 schema for ``pipelines/*.yml``.

This module is the YAML contract. Every downstream segment (factory, DAG
builder, source builders) consumes ``PipelineConfig`` — keep changes
backward-compatible or version the schema.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any, Literal

import isodate
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_CRON_FIELD_RE = re.compile(r"^[0-9*/,\-?]+$")


class SyncMode(StrEnum):
    full_refresh = "full_refresh"
    incremental = "incremental"
    cdc = "cdc"


class WriteDisposition(StrEnum):
    append = "append"
    # str.replace is a method; the StrEnum member shadows it on the class. The
    # YAML contract uses the dlt-canonical name, so suppress the false-positive
    # rather than rename.
    replace = "replace"  # type: ignore[assignment]
    merge = "merge"


class SchemaContract(StrEnum):
    evolve = "evolve"
    freeze = "freeze"
    discard_row = "discard_row"


class DestinationType(StrEnum):
    postgres = "postgres"
    snowflake = "snowflake"
    databricks = "databricks"
    duckdb = "duckdb"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _SourceBase(_StrictModel):
    connection: str
    config: dict[str, Any] = Field(default_factory=dict)


class SourceRestApi(_SourceBase):
    type: Literal["rest_api"]


class SourceSqlDatabase(_SourceBase):
    type: Literal["sql_database"]


class SourceFilesystem(_SourceBase):
    type: Literal["filesystem"]


class SourcePgCdc(_SourceBase):
    type: Literal["pg_cdc"]


SourceConfig = Annotated[
    SourceRestApi | SourceSqlDatabase | SourceFilesystem | SourcePgCdc,
    Field(discriminator="type"),
]


class BackfillConfig(_StrictModel):
    """Chunked, resumable historical load knobs (Segment 12).

    ``chunk_size`` is an ISO-8601 duration string parsed by ``isodate``
    (e.g. ``P1D`` = one day, ``PT6H`` = six hours). ``partition_field``
    defaults to ``sync.cursor_field`` when omitted.
    """

    chunk_size: str
    partition_field: str | None = None

    @field_validator("chunk_size")
    @classmethod
    def _iso8601_duration(cls, v: str) -> str:
        try:
            isodate.parse_duration(v)
        except (isodate.ISO8601Error, ValueError) as exc:
            raise ValueError(f"chunk_size must be an ISO-8601 duration: {exc}") from exc
        return v


class QualityCheckMode(StrEnum):
    same_cluster = "same_cluster"
    cross_cluster = "cross_cluster"


class QualityConfig(_StrictModel):
    """Data-quality knobs (Segment 12, v1 scope).

    ``row_count_check`` runs source-vs-destination reconciliation per replicated
    table. ``check_mode`` selects whether to use one SQL connection that can
    reach both (``same_cluster``) or two PythonOperator-driven probes
    (``cross_cluster``, default — no Airflow Connection wiring required).
    """

    row_count_check: bool = False
    check_mode: QualityCheckMode = QualityCheckMode.cross_cluster


class SyncConfig(_StrictModel):
    mode: SyncMode
    cursor_field: str | None = None
    primary_key: str | list[str] | None = None
    tolerance_seconds: int = Field(default=0, ge=0, le=86400)
    lookback: str | None = None
    backfill: BackfillConfig | None = None
    # Per-task SLA in minutes. When set, each task generated inside the
    # PipelineTasksGroup carries ``sla=timedelta(minutes=N)``; breaching
    # invokes ``sla_miss_callback`` -> ``post_sla_miss_alert`` (Segment 14).
    # Airflow 3.x replaces ``sla`` with the ``deadline`` API; see
    # ``src/dlt_data_pipeline/airflow/README.md``.
    sla_minutes: int | None = Field(default=None, gt=0)

    @field_validator("lookback")
    @classmethod
    def _lookback_iso8601(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            isodate.parse_duration(v)
        except (isodate.ISO8601Error, ValueError) as exc:
            raise ValueError(f"lookback must be an ISO-8601 duration: {exc}") from exc
        return v


class DestinationConfig(_StrictModel):
    type: DestinationType
    connection: str
    dataset: str


class ScheduleConfig(_StrictModel):
    cron: str
    enabled: bool = True

    @field_validator("cron")
    @classmethod
    def _shape(cls, v: str) -> str:
        fields = v.split(" ")
        if len(fields) != 5 or not all(_CRON_FIELD_RE.fullmatch(f) for f in fields):
            raise ValueError(
                "cron must be 5 space-separated fields of [0-9*/,-?] "
                "(semantic validity is checked by Airflow at DAG parse)"
            )
        return v


class OptionsConfig(_StrictModel):
    write_disposition: WriteDisposition = WriteDisposition.append
    schema_contract: SchemaContract = SchemaContract.evolve


class ResourcesConfig(_StrictModel):
    """Optional pod resource requests/limits for KubernetesExecutor.

    LocalExecutor ignores these; KubernetesExecutor (Segment 10) consumes
    them via ``executor_config={"pod_override": V1Pod(...)}`` on each task.
    Used as both k8s requests AND limits (Guaranteed QoS).
    """

    cpu: str | None = None
    memory: str | None = None


class AlertSeverity(StrEnum):
    P1 = "P1"
    P2 = "P2"
    info = "info"


class AlertsConfig(_StrictModel):
    """Per-pipeline alert routing knobs.

    Webhook URL, default channels per severity, and SMTP credentials are
    deployment concerns and live in env vars (see ``observability.alerts``
    module docstring). YAML carries only per-pipeline overrides.
    """

    severity: AlertSeverity = AlertSeverity.P2
    dedup_window_minutes: int = Field(default=15, ge=0, le=1440)
    slack_channel: str | None = None
    email_recipients: list[str] = Field(default_factory=list)
    on_schema_change: bool = True
    on_sla_miss: bool = True


class _SourceOverlay(_StrictModel):
    """Source fields allowed to differ per environment (Segment 13)."""

    connection: str | None = None


class _DestinationOverlay(_StrictModel):
    """Destination fields allowed to differ per environment (Segment 13).

    ``type`` + ``dataset`` are overlay-able so a pipeline can target duckdb
    in dev and snowflake in prod (per the segment done-when). Source
    overlays remain ``connection``-only — swapping source types is a
    different pipeline.
    """

    type: DestinationType | None = None
    connection: str | None = None
    dataset: str | None = None


class _ScheduleOverlay(_StrictModel):
    """Schedule fields allowed to differ per environment (Segment 13)."""

    enabled: bool | None = None


class PipelineOverlay(_StrictModel):
    """Environment-specific override block for a single pipeline (Segment 13).

    Lives inside ``pipelines/_env/<env>.yml`` keyed by pipeline name. Scope is
    deliberately narrow — only the four fields most operators need to re-map
    across envs. ``extra="forbid"`` rejects out-of-scope keys with a clear
    error; expanding the scope requires editing this model.
    """

    source: _SourceOverlay | None = None
    destination: _DestinationOverlay | None = None
    schedule: _ScheduleOverlay | None = None
    resources: ResourcesConfig | None = None


class PipelineConfig(_StrictModel):
    name: str
    source: SourceConfig
    sync: SyncConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    options: OptionsConfig = Field(default_factory=OptionsConfig)
    resources: ResourcesConfig = Field(default_factory=ResourcesConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    quality: QualityConfig = Field(default_factory=QualityConfig)

    @field_validator("name")
    @classmethod
    def _name_identifier(cls, v: str) -> str:
        if not _NAME_RE.fullmatch(v):
            raise ValueError(
                "name must match ^[a-z][a-z0-9_]*$ "
                "(lowercase identifier-safe for use as dataset / DAG id)"
            )
        return v

    @model_validator(mode="after")
    def _cross_field(self) -> PipelineConfig:
        mode = self.sync.mode
        if mode == SyncMode.incremental and self.sync.cursor_field is None:
            raise ValueError("sync.cursor_field is required when sync.mode == 'incremental'")
        if mode == SyncMode.full_refresh and self.sync.cursor_field is not None:
            raise ValueError("sync.cursor_field is not allowed when sync.mode == 'full_refresh'")
        if mode == SyncMode.cdc:
            if self.source.type != "pg_cdc":
                raise ValueError("sync.mode == 'cdc' requires source.type == 'pg_cdc'")
            if self.sync.primary_key is None:
                raise ValueError("sync.primary_key is required when sync.mode == 'cdc'")
        if (
            self.options.write_disposition == WriteDisposition.merge
            and self.sync.primary_key is None
        ):
            raise ValueError(
                "sync.primary_key is required when options.write_disposition == 'merge'"
            )
        if self.sync.backfill is not None and mode != SyncMode.incremental:
            raise ValueError(
                "sync.backfill requires sync.mode == 'incremental' "
                "(full_refresh and cdc don't bound by cursor)"
            )
        if self.sync.tolerance_seconds > 0 and self.sync.cursor_field is None:
            raise ValueError("sync.tolerance_seconds requires sync.cursor_field")
        if self.sync.lookback is not None and self.sync.cursor_field is None:
            raise ValueError("sync.lookback requires sync.cursor_field")
        if self.quality.row_count_check:
            if self.source.type not in ("sql_database", "pg_cdc"):
                raise ValueError(
                    "quality.row_count_check requires source.type in {'sql_database', 'pg_cdc'}"
                )
            if self.destination.type == DestinationType.databricks:
                raise ValueError(
                    "quality.row_count_check is not supported for databricks "
                    "destination (deferred per Segment 8)"
                )
        return self
