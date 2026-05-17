"""Every registered source builder must have a matching metadata entry."""

from __future__ import annotations

from dlt_data_pipeline.sources import registry
from dlt_data_pipeline.sources._metadata import SourceTypeMetadata


def test_every_builder_has_metadata() -> None:
    builders = set(registry.registered_types())
    metas = registry.all_metadata()
    missing = builders - set(metas)
    assert not missing, (
        f"source types {sorted(missing)} have a builder but no metadata entry. "
        f'Add them under [project.entry-points."{registry.ENTRY_POINT_METADATA_GROUP}"] '
        f"in pyproject.toml."
    )


def test_metadata_entries_are_correct_type() -> None:
    for name, meta in registry.all_metadata().items():
        assert isinstance(meta, SourceTypeMetadata), f"{name}: wrong metadata type"


def test_sql_database_metadata_shape() -> None:
    meta = registry.get_metadata("sql_database")
    assert meta.env_var_template == "SOURCES__SQL_DATABASE__<CONNECTION>__CREDENTIALS"
    assert "tables" in meta.required_config_keys
    assert "schema" in meta.allowed_config_keys


def test_resolve_env_var_substitutes_connection() -> None:
    meta = registry.get_metadata("sql_database")
    assert meta.resolve_env_var("pg_source") == ("SOURCES__SQL_DATABASE__PG_SOURCE__CREDENTIALS")


def test_rest_api_has_no_env_var() -> None:
    meta = registry.get_metadata("rest_api")
    assert meta.env_var_template is None
    assert meta.resolve_env_var("anything") is None
