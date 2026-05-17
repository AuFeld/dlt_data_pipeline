# Single image used by:
#   - docker-compose airflow-{webserver,scheduler,triggerer,init} services
#     (target: dev — default, includes pytest + responses for live-PG tests)
#   - one-shot CLI runs (`docker compose run --rm airflow-scheduler python -m
#     data_pipeline_template run <name>`)
#   - per-task pods under KubernetesExecutor (Segment 10)
#     (target: prod — `uv sync --frozen --no-dev` slims the runtime venv)
# Same artifact for every role; the compose service (or pod spec) supplies its
# own command. CI build-and-push (.github/workflows/ci.yml) pushes the `prod`
# target only.
#
# Dependency install uses uv against the committed uv.lock, which already pins
# apache-airflow==2.11.2 and its full transitive closure. To bump Airflow run
# `uv lock --upgrade-package apache-airflow` — uv enforces Airflow's pins
# during resolution, so no separate constraints file is needed.

# =============================================================================
# base — OS, uv, airflow user, env. Shared between dev + prod targets.
# =============================================================================
FROM python:3.12-slim-bookworm AS base

# OS deps: libpq5 for psycopg2 wheels, tini for proper PID-1 signal handling
# (otherwise SIGTERM doesn't reach the scheduler / webserver cleanly), curl
# for the webserver healthcheck, ca-certificates for outbound TLS.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 \
      tini \
      curl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pin uv to a known-good release.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

# Match Airflow's canonical uid/gid so bind-mounted log dirs work without
# host-side chown gymnastics on Linux (macOS Docker Desktop reverse-maps).
RUN groupadd -g 50000 airflow \
    && useradd -u 50000 -g 50000 -m -d /home/airflow airflow

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/opt/app/src \
    PATH=/opt/app/.venv/bin:$PATH

WORKDIR /opt/app

# Lockfile + pyproject — layer-cached so source bumps don't bust the deps
# install layer.
COPY pyproject.toml uv.lock ./

# =============================================================================
# dev — pytest + responses + live-PG fixtures. Default target for compose.
# =============================================================================
FROM base AS dev

RUN uv sync --frozen --extra dev --no-install-project

COPY src ./src
COPY dags ./dags
COPY pipelines ./pipelines
COPY .dlt/config.toml ./.dlt/config.toml
COPY airflow_home/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
# Bake the pod template so local `airflow standalone` / single-image runs
# resolve AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE without a mount.
# In-cluster scheduler mounts the airflow-pod-template ConfigMap on top so
# pod-spec revs ship without an image rebuild.
COPY airflow_home/pod_templates ${AIRFLOW_HOME}/pod_templates

RUN uv sync --frozen --extra dev

RUN mkdir -p ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/dags \
    && chown -R airflow:airflow ${AIRFLOW_HOME} /opt/app/.dlt

USER airflow

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]

# =============================================================================
# prod — no dev extras. Slimmer runtime venv; pushed to GHCR by CI.
# =============================================================================
FROM base AS prod

RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY dags ./dags
COPY pipelines ./pipelines
COPY .dlt/config.toml ./.dlt/config.toml
COPY airflow_home/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY airflow_home/pod_templates ${AIRFLOW_HOME}/pod_templates

RUN uv sync --frozen --no-dev

RUN mkdir -p ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/dags \
    && chown -R airflow:airflow ${AIRFLOW_HOME} /opt/app/.dlt

USER airflow

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
