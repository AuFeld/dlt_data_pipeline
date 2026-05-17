"""Postgres CDC source builder.

Wraps the vendored dlt ``pg_replication`` source under
[`_vendor/`](./_vendor/) and maps the YAML ``source.config`` block to its
``init_replication`` / ``replication_resource`` calls. Credentials resolve
from ``SOURCES__PG_CDC__<CONNECTION>__CREDENTIALS`` (env) or
``[sources.pg_cdc.<connection>.credentials]`` (``.dlt/secrets.toml``) keyed by
the YAML ``source.connection`` logical name.

Source Postgres requires ``wal_level=logical`` plus the connection user
holding the ``REPLICATION`` attribute. The compose ``postgres-source`` service
sets both via ``docker/postgres-source-init/01_replication.sql``.

``init_replication`` is invoked at builder time so the snapshot resources are
known when the returned ``DltSource`` is enumerated by
``PipelineTasksGroup``. The call is idempotent: re-running with the same
``slot_name`` / ``publication_name`` is a no-op for slot + publication
creation, and the snapshot-already-exists ``RuntimeError`` raised on
subsequent runs is swallowed (snapshots only land on the first run, by
design).
"""

from __future__ import annotations

from typing import Any

import dlt
from dlt.extract import DltResource, DltSource
from dlt.sources.credentials import ConnectionStringCredentials

from data_pipeline_template.config.models import SourceConfig, SourcePgCdc

from ._vendor import init_replication, replication_resource

_ALLOWED_KEYS: frozenset[str] = frozenset(
    {
        "slot_name",
        "publication_name",
        "tables",
        "schema",
        "publish",
        "reset",
        "include_columns",
        "columns",
        "target_batch_size",
        "flush_slot",
    }
)

_REQUIRED_KEYS: tuple[str, ...] = ("slot_name", "publication_name", "tables")


def _resolve_credentials(connection_name: str) -> ConnectionStringCredentials:
    key = f"sources.pg_cdc.{connection_name}.credentials"
    env_var = f"SOURCES__PG_CDC__{connection_name.upper()}__CREDENTIALS"
    try:
        value = dlt.secrets[key]
    except Exception as exc:
        raise ValueError(
            f"pg_cdc source {connection_name!r}: missing credentials. "
            f"Set env var {env_var} or add [{key}] to .dlt/secrets.toml"
        ) from exc
    if value is None:
        raise ValueError(
            f"pg_cdc source {connection_name!r}: missing credentials. "
            f"Set env var {env_var} or add [{key}] to .dlt/secrets.toml"
        )
    return ConnectionStringCredentials(str(value))


def _validate_config(connection: str, cfg: dict[str, Any]) -> None:
    unknown = set(cfg) - _ALLOWED_KEYS
    if unknown:
        raise ValueError(
            f"pg_cdc source {connection!r}: unknown config keys {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_KEYS)}"
        )
    for required in _REQUIRED_KEYS:
        value = cfg.get(required)
        if not value:
            raise ValueError(f"pg_cdc source {connection!r}: source.config.{required} is required")


def builder(config: SourceConfig) -> DltSource:
    if not isinstance(config, SourcePgCdc):
        raise TypeError(f"pg_cdc builder received wrong config type: {type(config).__name__}")

    user_cfg = dict(config.config)
    _validate_config(config.connection, user_cfg)

    slot_name = str(user_cfg["slot_name"])
    pub_name = str(user_cfg["publication_name"])
    table_names = [str(t) for t in user_cfg["tables"]]
    schema_name = str(user_cfg.get("schema", "public"))
    publish = str(user_cfg.get("publish", "insert, update, delete"))
    reset = bool(user_cfg.get("reset", False))
    include_columns = user_cfg.get("include_columns")
    columns = user_cfg.get("columns")
    target_batch_size = int(user_cfg.get("target_batch_size", 1000))
    flush_slot = bool(user_cfg.get("flush_slot", True))

    credentials = _resolve_credentials(config.connection)

    try:
        snapshot_result = init_replication(
            slot_name=slot_name,
            pub_name=pub_name,
            schema_name=schema_name,
            table_names=table_names,
            credentials=credentials,
            publish=publish,
            persist_snapshots=True,
            include_columns=include_columns,
            columns=columns,
            reset=reset,
        )
    except RuntimeError as exc:
        # Vendor raises "Cannot create snapshots because slot ... is already
        # created" on every run after the first. That's the expected steady
        # state — slot + publication already exist; only the streaming
        # resource is needed from here on.
        if "already" not in str(exc).lower():
            raise
        snapshot_result = None

    if snapshot_result is None:
        snapshot_resources: list[DltResource] = []
    elif isinstance(snapshot_result, list):
        snapshot_resources = list(snapshot_result)
    else:
        snapshot_resources = [snapshot_result]

    streaming_resource = replication_resource(
        slot_name=slot_name,
        pub_name=pub_name,
        credentials=credentials,
        include_columns=include_columns,
        columns=columns,
        target_batch_size=target_batch_size,
        flush_slot=flush_slot,
    )

    @dlt.source(name=config.connection)
    def pg_cdc_source() -> Any:
        yield from snapshot_resources
        yield streaming_resource

    return pg_cdc_source()
