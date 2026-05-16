import importlib.resources

import data_pipeline_template


def test_package_importable() -> None:
    assert data_pipeline_template.__version__


def test_subpackages_importable() -> None:
    for sub in (
        "data_pipeline_template.sources",
        "data_pipeline_template.sources.rest_api",
        "data_pipeline_template.sources.sql_database",
        "data_pipeline_template.sources.filesystem",
        "data_pipeline_template.sources.pg_cdc",
        "data_pipeline_template.airflow",
    ):
        __import__(sub)


def test_py_typed_present() -> None:
    files = importlib.resources.files("data_pipeline_template")
    assert (files / "py.typed").is_file()
