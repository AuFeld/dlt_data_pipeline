# Single image used by:
#   - docker-compose airflow-{webserver,scheduler,triggerer,init} services
#   - one-shot CLI runs (`docker compose run --rm airflow-scheduler python -m
#     data_pipeline_template run <name>`)
#   - per-task pods under KubernetesExecutor in Segment 10
# Same artifact for every role; the compose service (or pod spec) supplies its
# own command.
#
# Dependency install uses uv against the committed uv.lock, which already pins
# apache-airflow==2.11.2 and its full transitive closure. To bump Airflow run
# `uv lock --upgrade-package apache-airflow` — uv enforces Airflow's pins
# during resolution, so no separate constraints file is needed.

FROM python:3.12-slim-bookworm

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

# Layer-cache the dependency install: copy lock + pyproject first, sync, then
# copy source. Bumping source code doesn't bust the dep layer.
COPY pyproject.toml uv.lock ./
# Include dev deps (pytest, responses) so the live-PG integration test from
# Segment 5 can run inside the local stack. Segment 10's prod image will
# add --no-dev to slim down.
RUN uv sync --frozen --extra dev --no-install-project

# Application source. Bind mounts in docker-compose override these for live
# dev edits; the baked copies are what ships in the prod K8s image.
COPY src ./src
COPY dags ./dags
COPY pipelines ./pipelines
COPY .dlt/config.toml ./.dlt/config.toml
COPY airflow_home/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Install the project itself into the venv.
RUN uv sync --frozen --extra dev

# AIRFLOW_HOME and the dlt working dir must be writable by the airflow uid.
# /opt/app stays root-owned (read-only at runtime).
RUN mkdir -p ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/dags \
    && chown -R airflow:airflow ${AIRFLOW_HOME} /opt/app/.dlt

USER airflow

ENTRYPOINT ["/usr/bin/tini", "--"]
# No default airflow subcommand — every compose service supplies its own
# command. Bash default keeps `docker run <image>` interactively usable and
# also serves as the K8s-pod default (Segment 10 overrides via pod spec).
CMD ["bash"]
