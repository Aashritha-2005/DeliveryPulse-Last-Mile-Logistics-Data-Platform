FROM ghcr.io/astral-sh/uv:0.13.1@sha256:4a1f3666816dafadd20ceb71eac9a7d56d7ef30279f7d64048519ab901ada336 AS uv
FROM python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/deliverypulse
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never UV_NO_CACHE=1
RUN uv sync --locked --no-dev --no-install-project --python-preference only-system --python 3.12

FROM python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f AS runtime
RUN useradd --uid 10001 --no-create-home app
COPY --from=build /opt/venv /opt/venv
WORKDIR /opt/deliverypulse
COPY src/ ./src/
COPY sql/ ./sql/
COPY scripts/__init__.py scripts/k8s_pipeline.py ./scripts/
ENV PATH=/opt/venv/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["/opt/venv/bin/python", "-m", "scripts.k8s_pipeline"]
