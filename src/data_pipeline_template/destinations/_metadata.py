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
        env_var_template="DESTINATION__<CONNECTION>__CREDENTIALS",
        notes=(
            "Fallback: [destination.<connection>.credentials] in "
            ".dlt/secrets.toml. Value is any libpq connection URI."
        ),
    ),
    DestinationType.snowflake: DestinationTypeMetadata(
        description="Snowflake destination via dlt's native snowflake adapter.",
        env_var_template="DESTINATION__<CONNECTION>__CREDENTIALS",
        notes=(
            "Requires the optional 'snowflake' extra: uv sync --extra snowflake. "
            "Auth options:\n"
            "  (1) Password — env var value: "
            "snowflake://<user>:<password>@<account>/<database>?warehouse=<wh>&role=<role>\n"
            "  (2) Key-pair — set credentials.private_key (base64-encoded PEM) and "
            "credentials.private_key_passphrase under "
            "[destination.<connection>.credentials] in .dlt/secrets.toml. "
            "Account / user / database / warehouse / role go in the same section.\n"
            "Stage: dlt creates an internal named stage by default — no external "
            "S3/ADLS staging credentials needed for v1."
        ),
    ),
    DestinationType.databricks: DestinationTypeMetadata(
        description="Databricks destination — NOT in active use.",
        env_var_template=None,
        notes=(
            "Not implemented. Current architecture: dlt loads Snowflake; Databricks "
            "consumes from Snowflake via its External Data connection. Any pipeline "
            "with destination.type: databricks will fail at build() with a clear "
            "deferral message. Open an issue if direct dlt -> Databricks loads "
            "become a requirement."
        ),
    ),
}


def get_metadata(destination_type: DestinationType) -> DestinationTypeMetadata:
    return METADATA[destination_type]
