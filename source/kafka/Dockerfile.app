FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.8 /uv /uvx /bin/

COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-install-project

COPY src /app/src

ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"
