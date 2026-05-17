"""Airflow DagBag entrypoint.

Discovers every ``pipelines/*.yml``, builds one ``DAG`` per config via
``dag_factory.build_dag``, and assigns each generated ``DAG`` into module
``globals()`` so Airflow's DagBag scans them.

One bad YAML or one un-buildable pipeline must not take out all DAGs — errors
are logged and the offending pipeline is skipped.
"""

from __future__ import annotations

import logging
from pathlib import Path

from dlt_data_pipeline.airflow.dag_factory import build_dag
from dlt_data_pipeline.config.loader import ConfigError, load_pipelines

log = logging.getLogger(__name__)

# pipelines/ lives next to dags/ at the repo root. AIRFLOW_HOME may point
# elsewhere, so resolve relative to this file rather than CWD.
_PIPELINES_DIR = Path(__file__).resolve().parent.parent / "pipelines"

try:
    _configs = load_pipelines(_PIPELINES_DIR)
except ConfigError as e:
    log.error("pipeline config errors:\n%s", e)
    _configs = {}

for _name, _cfg in _configs.items():
    try:
        globals()[_name] = build_dag(_cfg)
    except NotImplementedError as e:
        # Source / sync mode not landed yet (e.g. sql_database -> Segment 5,
        # cdc -> Segment 7). Single-line warning; full traceback would spam
        # the scheduler log on every DagBag re-parse.
        log.warning("skipping pipeline %r: %s", _name, e)
    except Exception:
        log.exception("failed to build DAG for pipeline %r", _name)
