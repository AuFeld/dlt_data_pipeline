from importlib.metadata import entry_points

EXPECTED_SOURCE_BUILDERS = {"rest_api", "sql_database", "filesystem", "pg_cdc"}


def test_source_entry_points_registered() -> None:
    eps = entry_points(group="dlt_data_pipeline.sources")
    names = {ep.name for ep in eps}
    assert names == EXPECTED_SOURCE_BUILDERS


def test_source_entry_points_load_callable() -> None:
    eps = entry_points(group="dlt_data_pipeline.sources")
    for ep in eps:
        builder = ep.load()
        assert callable(builder), f"{ep.name} builder not callable"
