from __future__ import annotations

from data_pipeline_template.sources._metadata import SourceTypeMetadata

metadata = SourceTypeMetadata(
    description=(
        "Postgres logical-replication CDC source. Wraps the vendored "
        "dlt pg_replication verified source (sources/pg_cdc/_vendor)."
    ),
    env_var_template="SOURCES__PG_CDC__<CONNECTION>__CREDENTIALS",
    allowed_config_keys=(
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
    ),
    required_config_keys=("slot_name", "publication_name", "tables"),
    notes=(
        "Source Postgres needs wal_level=logical, max_replication_slots, "
        "max_wal_senders, and a connection role with the REPLICATION attribute "
        "(see docker/postgres-source-init/01_replication.sql for the local "
        "stack). 'tables' are bare names; use 'schema' for the schema (default "
        "'public'). Set 'reset: true' for a one-shot run to drop + recreate "
        "the slot + publication after an incompatible DDL change. cdc mode "
        "auto-promotes options.write_disposition: append -> merge."
    ),
)
