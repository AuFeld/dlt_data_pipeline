"""Boundary test: pipeline_factory must import + run with airflow blocked.

Proves Design principle #2 — orchestrator-agnostic core. Spawns a subprocess
that pre-installs ``sys.modules["airflow"] = None`` so any attempted ``import
airflow`` (or submodule) raises ``ImportError``, then exercises the factory
for both ``rest_api`` and ``sql_database`` sources.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

_PROBE_REST = """
import sys

class _Blocked:
    def __getattr__(self, name):
        raise ImportError("airflow is intentionally blocked for this boundary test")

sys.modules["airflow"] = None  # type: ignore[assignment]

from data_pipeline_template import pipeline_factory  # noqa: F401
from data_pipeline_template.config.models import (
    DestinationConfig,
    DestinationType,
    OptionsConfig,
    PipelineConfig,
    ScheduleConfig,
    SourceRestApi,
    SyncConfig,
    SyncMode,
)

cfg = PipelineConfig(
    name="probe_pipeline",
    source=SourceRestApi(
        type="rest_api",
        connection="probe_api",
        config={"base_url": "https://example.com/api/", "endpoints": ["thing"]},
    ),
    sync=SyncConfig(mode=SyncMode.full_refresh),
    destination=DestinationConfig(type=DestinationType.duckdb, connection="probe", dataset="raw"),
    schedule=ScheduleConfig(cron="0 6 * * *", enabled=True),
    options=OptionsConfig(),
)

runnable = pipeline_factory.build(cfg)
assert runnable.pipeline is not None
assert runnable.source is not None
print("ok")
"""

_PROBE_SQL = """
import sys

sys.modules["airflow"] = None  # type: ignore[assignment]

from data_pipeline_template import pipeline_factory  # noqa: F401
from data_pipeline_template.config.models import (
    DestinationConfig,
    DestinationType,
    OptionsConfig,
    PipelineConfig,
    ScheduleConfig,
    SourceSqlDatabase,
    SyncConfig,
    SyncMode,
    WriteDisposition,
)

cfg = PipelineConfig(
    name="probe_sql_pipeline",
    source=SourceSqlDatabase(
        type="sql_database",
        connection="sqlite_probe",
        config={"tables": ["orders"]},
    ),
    sync=SyncConfig(mode=SyncMode.incremental, cursor_field="updated_at", primary_key="id"),
    destination=DestinationConfig(
        type=DestinationType.duckdb, connection="probe", dataset="raw"
    ),
    schedule=ScheduleConfig(cron="0 6 * * *", enabled=True),
    options=OptionsConfig(write_disposition=WriteDisposition.merge),
)

runnable = pipeline_factory.build(cfg)
assert runnable.pipeline is not None
assert runnable.source is not None
assert set(runnable.source.resources.keys()) == {"orders"}
print("ok")
"""


def _run_probe(probe: str, tmp_path: Path, env_extra: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert result.returncode == 0, (
        f"subprocess failed.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert "ok" in result.stdout


def test_pipeline_factory_works_without_airflow_rest(tmp_path: Path) -> None:
    _run_probe(_PROBE_REST, tmp_path)


def test_pipeline_factory_works_without_airflow_sql_database(tmp_path: Path) -> None:
    db_path = tmp_path / "probe_source.db"
    # SQLite file does not need to exist for deferred reflection — Engine is
    # constructed lazily and never connected during build().
    _run_probe(
        _PROBE_SQL,
        tmp_path,
        env_extra={
            "SOURCES__SQL_DATABASE__SQLITE_PROBE__CREDENTIALS": f"sqlite:///{db_path}",
        },
    )


_PROBE_CDC = """
import sys

sys.modules["airflow"] = None  # type: ignore[assignment]

# Stub the vendored pg_replication so the cdc branch builds without a live
# Postgres + REPLICATION-capable role. The boundary test only cares that the
# code path doesn't transitively import airflow — actual DB I/O is covered by
# tests/integration/test_pg_cdc.py.
import dlt

from data_pipeline_template.sources import pg_cdc as _pg_cdc

def _fake_init(**_kw):
    return None

def _fake_resource(**kw):
    @dlt.resource(name=kw['slot_name'])
    def _streaming():
        yield from []
    return _streaming

_pg_cdc.init_replication = _fake_init
_pg_cdc.replication_resource = _fake_resource

from data_pipeline_template import pipeline_factory  # noqa: F401,E402
from data_pipeline_template.config.models import (  # noqa: E402
    DestinationConfig,
    DestinationType,
    OptionsConfig,
    PipelineConfig,
    ScheduleConfig,
    SourcePgCdc,
    SyncConfig,
    SyncMode,
    WriteDisposition,
)

cfg = PipelineConfig(
    name='probe_cdc_pipeline',
    source=SourcePgCdc(
        type='pg_cdc',
        connection='probe_cdc',
        config={
            'slot_name': 'probe_slot',
            'publication_name': 'probe_pub',
            'tables': ['orders'],
        },
    ),
    sync=SyncConfig(mode=SyncMode.cdc, primary_key='id'),
    destination=DestinationConfig(
        type=DestinationType.duckdb, connection='probe_dest', dataset='raw'
    ),
    schedule=ScheduleConfig(cron='*/5 * * * *', enabled=True),
    options=OptionsConfig(write_disposition=WriteDisposition.append),
)

runnable = pipeline_factory.build(cfg)
assert runnable.pipeline is not None
assert runnable.source is not None
# Validator passes append through, but cdc branch promotes to merge.
assert runnable.write_disposition == WriteDisposition.merge
print('ok')
"""


def test_pipeline_factory_works_without_airflow_pg_cdc(tmp_path: Path) -> None:
    _run_probe(
        _PROBE_CDC,
        tmp_path,
        env_extra={
            "SOURCES__PG_CDC__PROBE_CDC__CREDENTIALS": "postgresql://u:p@h:5432/db",
        },
    )
