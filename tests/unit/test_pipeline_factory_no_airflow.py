"""Boundary test: pipeline_factory must import + run with airflow blocked.

Proves Design principle #2 — orchestrator-agnostic core. Spawns a subprocess
that pre-installs ``sys.modules["airflow"] = None`` so any attempted ``import
airflow`` (or submodule) raises ``ImportError``, then exercises the factory.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

_PROBE = """
import sys

# Block airflow + all submodules. Any module-level `import airflow.*` in the
# factory or its transitive deps will now raise ImportError.
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


def test_pipeline_factory_works_without_airflow(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"subprocess failed.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
    assert "ok" in result.stdout
