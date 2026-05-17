# `overlays/prod/` — stub

Overlay templating (kustomize vs helm) is deferred from Segment 10. For now,
env-specific values (image tag, S3 bucket, replica counts, resource
requests, namespace) are patched inline in the relevant `../../base/`
manifest before `kubectl apply`, or via `kubectl set image` / `kubectl set
env` in the deploy CI job.

When kustomize lands, this directory will hold `kustomization.yaml` +
patches for the prod environment.
