from __future__ import annotations

from data_pipeline_template.sources._metadata import SourceTypeMetadata

metadata = SourceTypeMetadata(
    description=(
        "Postgres logical-replication CDC source. Wraps dlt.sources.pg_replication. "
        "Builder lands in Segment 7."
    ),
    env_var_template="SOURCES__PG_CDC__<CONNECTION>__CREDENTIALS",
    allowed_config_keys=("slot_name", "publication_name", "tables"),
    required_config_keys=("slot_name", "publication_name", "tables"),
    notes=(
        "Source Postgres needs wal_level=logical, max_replication_slots, "
        "max_wal_senders. Replication user needs the REPLICATION role (not "
        "superuser)."
    ),
)
