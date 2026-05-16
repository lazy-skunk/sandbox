FROM python:3.13-slim

WORKDIR /workspace

ENV PATH="/root/.local/bin/:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
ENV UV_SYSTEM_PYTHON=1
ENV UV_PROJECT_ENVIRONMENT="/usr/local"

RUN apt-get update && \
    apt-get install -y \
    git \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
RUN uv sync --frozen
