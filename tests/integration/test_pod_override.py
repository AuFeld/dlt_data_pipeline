"""Smoke test for ``dag_factory._pod_override()`` V1Pod shape (Segment 10).

Exercises the private helper directly across a matrix of ``ResourcesConfig``
inputs. Complements ``tests/unit/test_dag_factory.py`` which asserts the
DAG-builder wires ``executor_config`` onto tasks — this file owns the
per-input V1Pod shape contract that KubernetesExecutor merges by container
name into the pod template at ``airflow_home/pod_templates/base.yaml``.

No cluster required: imports the ``kubernetes`` client (pulled in via
``apache-airflow-providers-cncf-kubernetes``) and asserts python objects.
Runs in the default integration-duckdb CI job (no marker → matches the
``not postgres and not cdc and not snowflake`` selector).
"""

from __future__ import annotations

import pytest
from kubernetes.client import V1Pod

from dlt_data_pipeline.airflow.dag_factory import _pod_override
from dlt_data_pipeline.config.models import ResourcesConfig


@pytest.mark.parametrize(
    ("resources", "expected"),
    [
        (ResourcesConfig(), {}),
        (ResourcesConfig(cpu="500m"), {"cpu": "500m"}),
        (ResourcesConfig(memory="2Gi"), {"memory": "2Gi"}),
        (ResourcesConfig(cpu="2", memory="4Gi"), {"cpu": "2", "memory": "4Gi"}),
        (ResourcesConfig(cpu="250m", memory="512Mi"), {"cpu": "250m", "memory": "512Mi"}),
    ],
    ids=["none", "cpu-only", "memory-only", "both-whole", "both-fractional"],
)
def test_pod_override_shape(resources: ResourcesConfig, expected: dict[str, str]) -> None:
    """Each ResourcesConfig produces a V1Pod with one container named 'base'.

    requests == limits (Guaranteed QoS), and both maps contain only the
    keys the user actually set in the YAML.
    """
    pod = _pod_override(resources)

    assert isinstance(pod, V1Pod)
    assert pod.spec is not None
    assert len(pod.spec.containers) == 1

    container = pod.spec.containers[0]
    # The container name is the merge key with the pod template — must stay
    # "base" or KubernetesExecutor silently ignores the per-task overrides.
    assert container.name == "base"

    assert container.resources is not None
    assert container.resources.requests == expected
    assert container.resources.limits == expected
