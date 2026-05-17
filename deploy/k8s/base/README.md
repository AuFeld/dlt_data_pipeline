# `deploy/k8s/base/` — KubernetesExecutor base manifests (Segment 10)

Raw kubectl manifests for the prod Airflow deployment. The kustomize vs.
helm decision is deferred — overlays under `../overlays/{dev,staging,prod}/`
are stubs today; environment-specific values (image tag, S3 bucket, replica
counts, resource requests) are patched inline before `kubectl apply` or by
the deploy CI job.

## Apply order

```bash
kubectl create namespace airflow
kubectl apply -f deploy/k8s/base/
# CI deploy job additionally:
#   - rebuilds `airflow-pod-template` ConfigMap from
#     `pod-template-base.yaml` so template revs ship without an image rebuild
#   - patches `airflow-config` worker image tag
#   - rolls webserver / scheduler / triggerer
```

## Files

| File | What |
|---|---|
| `airflow-configmap.yaml` | Non-secret env shared by all components + worker pod template. |
| `airflow-secret.yaml` | Placeholder credentials. Override per env — never commit real values. |
| `airflow-pod-template-configmap.yaml` | Worker pod template, mounted onto scheduler. |
| `pod-template-base.yaml` | **Mirror** of `airflow_home/pod_templates/base.yaml`. Keep byte-identical. |
| `postgres-metadata-statefulset.yaml` | Airflow metadata DB. Swap for managed RDS/CloudSQL in prod overlays. |
| `rbac.yaml` | ServiceAccounts + scheduler pod-spawn Role/RoleBinding. |
| `webserver-deployment.yaml` | UI deployment (`replicas: 2`, `maxUnavailable: 0`). |
| `scheduler-deployment.yaml` | Spawns worker pods. Mounts pod-template ConfigMap. |
| `triggerer-deployment.yaml` | Deferrable-operator host. |

## Dual-write rule: pod template

`airflow_home/pod_templates/base.yaml` is the authoritative source.
`deploy/k8s/base/pod-template-base.yaml` is a byte-identical mirror so
`kubectl apply -f deploy/k8s/base/` works without extra cp steps. **Edit
the authoritative file, then `cp` to the mirror before committing.** A
pre-commit hook to enforce this lands in Segment 11.

## Secrets backend v1 vs v2

- **v1 (this directory):** `Secret` env-mounted via `envFrom`. Simple,
  cluster-native, no extra controller. Rotation = `kubectl apply` + pod
  restart.
- **v2 (doc-only):** `AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`
  + `AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "...",
  "variables_prefix": "..."}'` in `airflow-config`. Rotation is centralized
  but adds AWS Secrets Manager as a runtime dependency. Defer until a real
  rotation/audit requirement materializes.

## Destination credentials env-var shape

`DESTINATION__<CONN>__CREDENTIALS` — see
[`destinations/_metadata.py`](../../../src/dlt_data_pipeline/destinations/_metadata.py)
and the [`AGENTS.md`](../../../AGENTS.md) "Logical-connection-name →
env-var convention" section. Segment 10.5 dropped the type segment from
the resolver path, so no `__<TYPE>__` prefix.
