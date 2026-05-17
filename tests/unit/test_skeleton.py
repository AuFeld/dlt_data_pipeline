import importlib.resources

import dlt_data_pipeline


def test_package_importable() -> None:
    assert dlt_data_pipeline.__version__


def test_subpackages_importable() -> None:
    for sub in (
        "dlt_data_pipeline.sources",
        "dlt_data_pipeline.sources.rest_api",
        "dlt_data_pipeline.sources.sql_database",
        "dlt_data_pipeline.sources.filesystem",
        "dlt_data_pipeline.sources.pg_cdc",
        "dlt_data_pipeline.airflow",
    ):
        __import__(sub)


def test_py_typed_present() -> None:
    files = importlib.resources.files("dlt_data_pipeline")
    assert (files / "py.typed").is_file()
