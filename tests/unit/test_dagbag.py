"""DagBag integration check.

Loads the real ``dags/data_pipeline_dags.py`` against the real ``pipelines/``
directory. Asserts no import errors and that every buildable pipeline shows
up as a DAG. ``cdc`` pipelines are skipped intentionally — ``pipeline_factory``
raises ``NotImplementedError`` for them until Segment 7.
"""

from __future__ import annotations

from pathlib import Path

from airflow.models import DagBag

from data_pipeline_template.config.loader import load_pipelines

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "dags"
PIPELINES_DIR = REPO_ROOT / "pipelines"


def test_dagbag_clean() -> None:
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors


def test_dagbag_contains_every_buildable_pipeline() -> None:
    configs = load_pipelines(PIPELINES_DIR)
    expected = {name for name, cfg in configs.items() if cfg.sync.mode.value != "cdc"}

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert expected.issubset(set(bag.dag_ids)), {
        "expected_subset": sorted(expected),
        "actual": sorted(bag.dag_ids),
    }
