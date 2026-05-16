from __future__ import annotations

from pathlib import Path

import pytest

from data_pipeline_template.config import ConfigError, load_pipelines

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
REPO_PIPELINES = Path(__file__).resolve().parents[2] / "pipelines"


def test_loads_valid_fixtures() -> None:
    cfgs = load_pipelines(FIXTURES / "pipelines_valid")
    assert set(cfgs) == {"alpha", "beta", "gamma"}
    assert cfgs["beta"].sync.cursor_field == "updated_at"


def test_skips_underscore_prefixed_files() -> None:
    # _ignored.yml in pipelines_valid/ contains malformed YAML; loader must skip
    # without raising.
    load_pipelines(FIXTURES / "pipelines_valid")


def test_missing_directory_returns_empty() -> None:
    assert load_pipelines(FIXTURES / "does_not_exist") == {}


def test_empty_directory_returns_empty() -> None:
    assert load_pipelines(FIXTURES / "pipelines_empty") == {}


def test_invalid_dir_aggregates_yaml_and_schema_errors() -> None:
    with pytest.raises(ConfigError) as exc_info:
        load_pipelines(FIXTURES / "pipelines_invalid")
    message = str(exc_info.value)
    assert "bad_yaml.yml" in message
    assert "missing_cursor.yml" in message
    assert "cursor_field" in message


def test_duplicate_name_across_files() -> None:
    with pytest.raises(ConfigError) as exc_info:
        load_pipelines(FIXTURES / "pipelines_dup_a")
    message = str(exc_info.value)
    assert "duplicate pipeline name 'same_name'" in message
    assert "one.yml" in message
    assert "two.yml" in message


def test_validation_error_includes_file_path() -> None:
    with pytest.raises(ConfigError) as exc_info:
        load_pipelines(FIXTURES / "pipelines_invalid")
    assert str(FIXTURES / "pipelines_invalid" / "missing_cursor.yml") in str(exc_info.value)


def test_real_pipelines_dir_parses() -> None:
    cfgs = load_pipelines(REPO_PIPELINES)
    assert set(cfgs) == {
        "example_rest_to_duckdb",
        "example_pg_to_pg_incremental",
        "example_pg_cdc_to_snowflake",
    }
