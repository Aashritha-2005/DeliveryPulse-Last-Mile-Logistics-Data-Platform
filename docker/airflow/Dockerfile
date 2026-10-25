# DeliveryPulse Airflow image: the official Airflow 3.3.2 image (Python 3.12) plus an isolated
# virtual environment with the project's locked runtime dependencies. The DAG's tasks run the
# project's command-line entry points with that environment, so the project's packages never
# mix with Airflow's own. Project code (src/, sql/, dags/) is mounted read-only at run time.
#
# Both images are pinned by digest; see docs/operations.md ("Updating pins").

FROM ghcr.io/astral-sh/uv:0.13.5@sha256:32a7bb39b496babbd997b0a39a4d66c126bdaf5661b302bf6f06abda8deca6ff AS uv

FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

COPY --from=uv /uv /usr/local/bin/uv

# The containers run as the host user's UID with group 0 (AIRFLOW_UID), so every directory they
# write to is owned by group 0 and group-writable. /opt/airflow/auth holds the generated admin
# password (a named volume, never the project directory).
USER root
RUN mkdir -p /opt/deliverypulse /opt/airflow/auth \
    && chown airflow:0 /opt/deliverypulse /opt/airflow/auth \
    && chmod 2775 /opt/deliverypulse /opt/airflow/auth
USER airflow

WORKDIR /opt/deliverypulse
COPY --chown=airflow:0 pyproject.toml uv.lock ./

# Exact versions from uv.lock (--locked fails if the lock is stale); runtime dependencies only;
# the image's own Python 3.12, not a downloaded one; no cache left in the image.
ENV UV_PROJECT_ENVIRONMENT=/opt/deliverypulse/.venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_CACHE=1
RUN uv sync --locked --no-dev --no-install-project --python-preference only-system --python 3.12

WORKDIR /opt/airflow
