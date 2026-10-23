FROM ghcr.io/astral-sh/uv:0.13.0@sha256:cdc6093146eb3ff6a40107b38f008b789e050e77ad87865e381d9917da55a168 AS uv
FROM python:3.12.15-slim-trixie@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704 AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/deliverypulse
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never UV_NO_CACHE=1
RUN uv sync --locked --no-dev --no-install-project --python-preference only-system --python 3.12

FROM python:3.12.15-slim-trixie@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704 AS runtime
RUN useradd --uid 10001 --no-create-home app
COPY --from=build /opt/venv /opt/venv
WORKDIR /opt/deliverypulse
COPY src/ ./src/
COPY sql/ ./sql/
COPY scripts/__init__.py scripts/k8s_pipeline.py ./scripts/
ENV PATH=/opt/venv/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "scripts.k8s_pipeline"]
