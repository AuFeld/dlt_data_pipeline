"""Pydantic v2 schema for ``pipelines/*.yml``.

This module is the YAML contract. Every downstream segment (factory, DAG
builder, source builders) consumes ``PipelineConfig`` — keep changes
backward-compatible or version the schema.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any, Literal

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


class SyncConfig(_StrictModel):
    mode: SyncMode
    cursor_field: str | None = None
    primary_key: str | list[str] | None = None


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


class PipelineConfig(_StrictModel):
    name: str
    source: SourceConfig
    sync: SyncConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    options: OptionsConfig = Field(default_factory=OptionsConfig)
    resources: ResourcesConfig = Field(default_factory=ResourcesConfig)

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
        return self
