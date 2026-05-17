"""Airflow sensors for CDC pipeline operations.

Lives under the airflow subpackage so ruff's ``flake8-tidy-imports`` boundary
(Design principle #2) keeps the ``airflow`` import contained. The sensor is
not auto-wired into ``dag_factory`` — Segment 7 ships it as an opt-in utility.
Segment 9 will own the threshold/routing decisions when alerting lands.

Opt-in usage example: write a separate monitoring DAG that pokes the slot and
fails (or, when alerting lands, emits a Slack message) when lag exceeds the
configured byte budget.

```python
from data_pipeline_template.airflow.sensors import PgReplicationSlotLagSensor

with DAG("pg_cdc_slot_health", schedule="*/5 * * * *", ...):
    PgReplicationSlotLagSensor(
        task_id="orders_slot_lag",
        postgres_conn_id="pg_source_admin",
        slot_name="dlt_orders_slot",
        max_lag_bytes=64 * 1024 * 1024,
    )
```
"""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator


class PgReplicationSlotLagSensor(BaseSensorOperator):
    """Polls ``pg_replication_slots`` and returns True while lag is under budget.

    Raises ``AirflowFailException`` (terminal failure, no retries) when the
    named slot does not exist on the target server — there is no recovery
    path from "slot missing" without operator intervention, so retrying just
    wastes cycles.

    Args:
        postgres_conn_id: Airflow connection id pointing at the source
            postgres (the database that owns the slot, NOT the destination).
            The connection user needs ``REPLICATION`` or ``pg_monitor``.
        slot_name: Logical replication slot name (must match the
            ``slot_name`` from the pg_cdc pipeline YAML).
        max_lag_bytes: Inclusive upper bound on
            ``pg_current_wal_lsn() - confirmed_flush_lsn``. Defaults to 64
            MiB — high enough to ride out a slow consumer batch, low enough
            to catch a stuck slot before WAL retention fills the disk.
    """

    template_fields = ("slot_name", "postgres_conn_id")

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        slot_name: str,
        max_lag_bytes: int = 64 * 1024 * 1024,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.slot_name = slot_name
        self.max_lag_bytes = max_lag_bytes

    def poke(self, context: Any) -> bool:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        row = hook.get_first(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
            "FROM pg_replication_slots WHERE slot_name = %s",
            parameters=(self.slot_name,),
        )
        if row is None:
            raise AirflowFailException(
                f"replication slot {self.slot_name!r} not found on "
                f"connection {self.postgres_conn_id!r}"
            )
        lag = int(row[0])
        self.log.info(
            "slot %s lag %d bytes (budget %d)",
            self.slot_name,
            lag,
            self.max_lag_bytes,
        )
        return lag <= self.max_lag_bytes
