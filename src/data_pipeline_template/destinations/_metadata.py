"""Destination-type metadata for introspection.

Destinations are enum-dispatched (not plugin-driven), so metadata lives in a
plain dict keyed by ``DestinationType`` rather than via entry points.
"""

from __future__ import annotations

from dataclasses import dataclass

from data_pipeline_template.config.models import DestinationType


@dataclass(frozen=True)
class DestinationTypeMetadata:
    description: str
    env_var_template: str | None
    notes: str = ""

    def resolve_env_var(self, connection: str) -> str | None:
        if self.env_var_template is None:
            return None
        return self.env_var_template.replace("<CONNECTION>", connection.upper())


METADATA: dict[DestinationType, DestinationTypeMetadata] = {
    DestinationType.duckdb: DestinationTypeMetadata(
        description="Local DuckDB file destination (anchored under .dlt/<connection>.duckdb).",
        env_var_template=None,
        notes=(
            "No credential env var. Override storage dir via "
            "DATA_PIPELINE_DUCKDB_DIR for tests / non-default layouts."
        ),
    ),
    DestinationType.postgres: DestinationTypeMetadata(
        description="Postgres destination via dlt's native postgres adapter.",
        env_var_template="DESTINATION__POSTGRES__<CONNECTION>__CREDENTIALS",
        notes=(
            "Fallback: [destination.postgres.<connection>.credentials] in "
            ".dlt/secrets.toml. Value is any libpq connection URI."
        ),
    ),
    DestinationType.snowflake: DestinationTypeMetadata(
        description="Snowflake destination. Lands in Segment 8.",
        env_var_template="DESTINATION__SNOWFLAKE__<CONNECTION>__CREDENTIALS",
        notes="Key-pair auth: base64-encode the PEM into the URI, or mount via .dlt/secrets.toml.",
    ),
    DestinationType.databricks: DestinationTypeMetadata(
        description="Databricks destination. Lands in Segment 8.",
        env_var_template="DESTINATION__DATABRICKS__<CONNECTION>__CREDENTIALS",
        notes="Needs host + http_path + token; separate staging-location creds (S3/ADLS).",
    ),
}


def get_metadata(destination_type: DestinationType) -> DestinationTypeMetadata:
    return METADATA[destination_type]
