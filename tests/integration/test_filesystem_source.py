"""End-to-end filesystem -> duckdb via the pipeline factory.

Hermetic: reads CSV / Parquet / JSONL fixtures from
``tests/fixtures/files/`` via ``file://`` URLs. No network, no S3 mocking.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
import yaml

from dlt_data_pipeline import pipeline_factory

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "files"
EXPECTED_ROWS = 5


def _stage_pipeline(
    tmp_path: Path,
    *,
    name: str,
    file_glob: str,
    fmt: str,
    dataset: str,
) -> Path:
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir(exist_ok=True)
    cfg = {
        "name": name,
        "source": {
            "type": "filesystem",
            "connection": "local_fixtures",
            "config": {
                "bucket_url": f"file://{FIXTURES_DIR}/",
                "file_glob": file_glob,
                "format": fmt,
                "table_name": "orders",
            },
        },
        "sync": {"mode": "full_refresh"},
        "destination": {
            "type": "duckdb",
            "connection": "local_duckdb",
            "dataset": dataset,
        },
        "schedule": {"cron": "0 6 * * *", "enabled": False},
        "options": {"write_disposition": "replace"},
    }
    (pipelines_dir / f"{name}.yml").write_text(yaml.safe_dump(cfg))
    return pipelines_dir


def _rowcount(db_path: Path, dataset: str, table: str) -> int:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        (count,) = conn.execute(f"SELECT COUNT(*) FROM {dataset}.{table}").fetchone()
    return int(count)


@pytest.mark.parametrize(
    ("fmt", "file_glob", "dataset"),
    [
        ("csv", "orders.csv", "raw_csv"),
        ("parquet", "orders.parquet", "raw_parquet"),
        ("jsonl", "orders.jsonl", "raw_jsonl"),
    ],
)
def test_filesystem_to_duckdb(
    fmt: str,
    file_glob: str,
    dataset: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipelines_dir = _stage_pipeline(
        tmp_path,
        name=f"smoketest_{fmt}",
        file_glob=file_glob,
        fmt=fmt,
        dataset=dataset,
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_PIPELINE_DUCKDB_DIR", str(tmp_path / ".dlt"))

    load_info = pipeline_factory.run(f"smoketest_{fmt}", pipelines_root=pipelines_dir)
    assert load_info is not None

    db_path = tmp_path / ".dlt" / "local_duckdb.duckdb"
    assert db_path.exists(), f"expected duckdb at {db_path}"
    assert _rowcount(db_path, dataset, "orders") == EXPECTED_ROWS


def test_filesystem_unknown_format_raises(tmp_path: Path) -> None:
    """Glob without a recognized extension and no explicit format -> clear error."""
    bad_fixtures = tmp_path / "files"
    bad_fixtures.mkdir()
    shutil.copy(FIXTURES_DIR / "orders.csv", bad_fixtures / "orders.txt")

    from dlt_data_pipeline.config.models import SourceFilesystem
    from dlt_data_pipeline.sources.filesystem import builder

    cfg = SourceFilesystem(
        type="filesystem",
        connection="weird",
        config={"bucket_url": f"file://{bad_fixtures}/", "file_glob": "orders.txt"},
    )
    with pytest.raises(ValueError, match="cannot infer format"):
        builder(cfg)


def test_filesystem_unknown_config_key_raises() -> None:
    from dlt_data_pipeline.config.models import SourceFilesystem
    from dlt_data_pipeline.sources.filesystem import builder

    cfg = SourceFilesystem(
        type="filesystem",
        connection="weird",
        config={"bucket_url": "file:///tmp/", "file_glob": "*.csv", "bogus": "x"},
    )
    with pytest.raises(ValueError, match="unknown config keys"):
        builder(cfg)


def test_filesystem_remote_bucket_missing_creds_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dlt_data_pipeline.config.models import SourceFilesystem
    from dlt_data_pipeline.sources.filesystem import builder

    monkeypatch.delenv("SOURCES__FILESYSTEM__S3CONN__CREDENTIALS", raising=False)
    cfg = SourceFilesystem(
        type="filesystem",
        connection="s3conn",
        config={
            "bucket_url": "s3://my-bucket/",
            "file_glob": "*.csv",
            "format": "csv",
        },
    )
    with pytest.raises(ValueError, match="missing credentials"):
        builder(cfg)
