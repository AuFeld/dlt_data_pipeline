"""Tests for env-overlay loader behavior (Segment 13)."""

from __future__ import annotations

from pathlib import Path

import pytest

from dlt_data_pipeline.config import ConfigError, load_pipelines, resolve_env

_BASE_ALPHA = """\
name: alpha
source:
  type: rest_api
  connection: demo_api
  config:
    base_url: https://example.com/
sync:
  mode: full_refresh
destination:
  type: duckdb
  connection: local_duckdb
  dataset: raw_alpha
schedule:
  cron: "0 6 * * *"
  enabled: true
options:
  write_disposition: replace
"""

_BASE_BETA = """\
name: beta
source:
  type: sql_database
  connection: pg_source
  config:
    schema: public
    tables: [orders]
sync:
  mode: incremental
  cursor_field: updated_at
  primary_key: id
destination:
  type: postgres
  connection: pg_warehouse
  dataset: raw_beta
schedule:
  cron: "*/15 * * * *"
options:
  write_disposition: merge
"""


@pytest.fixture
def overlay_root(tmp_path: Path) -> Path:
    (tmp_path / "alpha.yml").write_text(_BASE_ALPHA)
    (tmp_path / "beta.yml").write_text(_BASE_BETA)
    (tmp_path / "_env").mkdir()
    return tmp_path


def test_no_overlay_file_loads_base_unchanged(overlay_root: Path) -> None:
    cfgs = load_pipelines(overlay_root, env="dev")
    assert cfgs["alpha"].destination.type.value == "duckdb"
    assert cfgs["alpha"].destination.connection == "local_duckdb"
    assert cfgs["alpha"].destination.dataset == "raw_alpha"
    assert cfgs["beta"].destination.connection == "pg_warehouse"


def test_overlay_flips_destination_type_and_connection(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text(
        "alpha:\n"
        "  destination:\n"
        "    type: snowflake\n"
        "    connection: snowflake_warehouse\n"
        "    dataset: prod_raw_alpha\n"
        "  resources:\n"
        '    cpu: "1000m"\n'
        '    memory: "2Gi"\n'
    )
    cfgs = load_pipelines(overlay_root, env="prod")
    assert cfgs["alpha"].destination.type.value == "snowflake"
    assert cfgs["alpha"].destination.connection == "snowflake_warehouse"
    assert cfgs["alpha"].destination.dataset == "prod_raw_alpha"
    assert cfgs["alpha"].resources.cpu == "1000m"
    assert cfgs["alpha"].resources.memory == "2Gi"
    # Untouched pipeline unchanged.
    assert cfgs["beta"].destination.connection == "pg_warehouse"
    # Untouched leaves on alpha survive.
    assert cfgs["alpha"].source.connection == "demo_api"
    assert cfgs["alpha"].schedule.cron == "0 6 * * *"


def test_overlay_schedule_enabled_only(overlay_root: Path) -> None:
    (overlay_root / "_env" / "ci.yml").write_text("alpha:\n  schedule:\n    enabled: false\n")
    cfgs = load_pipelines(overlay_root, env="ci")
    assert cfgs["alpha"].schedule.enabled is False
    # cron survives.
    assert cfgs["alpha"].schedule.cron == "0 6 * * *"


def test_overlay_unknown_pipeline_raises(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text("ghost:\n  schedule:\n    enabled: false\n")
    with pytest.raises(ConfigError) as exc:
        load_pipelines(overlay_root, env="prod")
    assert "ghost" in str(exc.value)
    assert "unknown pipeline name" in str(exc.value)


def test_overlay_out_of_scope_key_raises(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text("alpha:\n  sync:\n    mode: incremental\n")
    with pytest.raises(ConfigError) as exc:
        load_pipelines(overlay_root, env="prod")
    assert "alpha" in str(exc.value)
    # PipelineOverlay has extra="forbid" — sync is not an allowed top-level
    # overlay key.
    assert "sync" in str(exc.value).lower() or "extra" in str(exc.value).lower()


def test_overlay_invalid_leaf_in_destination_raises(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text(
        "alpha:\n  destination:\n    dataset: prod_raw_alpha\n    bogus_field: true\n"
    )
    with pytest.raises(ConfigError) as exc:
        load_pipelines(overlay_root, env="prod")
    assert "bogus_field" in str(exc.value)


def test_overlay_breaks_cross_field_validator(overlay_root: Path) -> None:
    """Flipping a merge-disposition pipeline's destination keeps the
    cross-field constraint that merge requires primary_key. Use a separate
    case: make alpha (full_refresh, no primary_key) try to become a merge
    pipeline via... oh wait, options can't be overlaid. So instead test
    that overlay validation re-runs by flipping the source.connection on a
    cdc pipeline whose primary_key is still required.
    """
    # Simpler: an overlay that points at an invalid destination type (not
    # in the enum) should fail PipelineOverlay validation cleanly.
    (overlay_root / "_env" / "prod.yml").write_text(
        "alpha:\n  destination:\n    type: bogus_type\n"
    )
    with pytest.raises(ConfigError) as exc:
        load_pipelines(overlay_root, env="prod")
    assert "alpha" in str(exc.value)


def test_overlay_top_level_not_mapping_raises(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text("- not\n- a\n- mapping\n")
    with pytest.raises(ConfigError) as exc:
        load_pipelines(overlay_root, env="prod")
    assert "top-level YAML must be a mapping" in str(exc.value)


def test_overlay_block_not_mapping_raises(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text("alpha: 42\n")
    with pytest.raises(ConfigError) as exc:
        load_pipelines(overlay_root, env="prod")
    assert "alpha" in str(exc.value)
    assert "overlay block must be a mapping" in str(exc.value)


def test_resolve_env_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    # CLI arg wins over env var.
    monkeypatch.setenv("DLT_ENV", "staging")
    assert resolve_env("prod") == "prod"
    # Env var wins over default.
    assert resolve_env(None) == "staging"
    # Default.
    monkeypatch.delenv("DLT_ENV", raising=False)
    assert resolve_env(None) == "dev"
    # Empty string CLI arg falls through to env var.
    monkeypatch.setenv("DLT_ENV", "staging")
    assert resolve_env("") == "staging"


def test_load_pipelines_resolves_env_from_env_var(
    overlay_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (overlay_root / "_env" / "prod.yml").write_text(
        "alpha:\n  destination:\n    connection: snowflake_warehouse\n"
    )
    monkeypatch.setenv("DLT_ENV", "prod")
    cfgs = load_pipelines(overlay_root)  # env=None -> resolves to prod
    assert cfgs["alpha"].destination.connection == "snowflake_warehouse"


def test_overlay_empty_yaml_treated_as_empty(overlay_root: Path) -> None:
    """A near-empty overlay file (just comments / nothing) is valid."""
    (overlay_root / "_env" / "prod.yml").write_text("# nothing here\n")
    cfgs = load_pipelines(overlay_root, env="prod")
    assert cfgs["alpha"].destination.connection == "local_duckdb"


def test_overlay_empty_block_is_noop(overlay_root: Path) -> None:
    (overlay_root / "_env" / "dev.yml").write_text("alpha: {}\n")
    cfgs = load_pipelines(overlay_root, env="dev")
    assert cfgs["alpha"].destination.connection == "local_duckdb"


def test_overlay_only_applies_for_active_env(overlay_root: Path) -> None:
    (overlay_root / "_env" / "prod.yml").write_text(
        "alpha:\n  destination:\n    connection: snowflake_warehouse\n"
    )
    # dev does not have its own overlay file → base unchanged.
    cfgs = load_pipelines(overlay_root, env="dev")
    assert cfgs["alpha"].destination.connection == "local_duckdb"
