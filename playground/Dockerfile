FROM python:3.13-slim

WORKDIR /playground

ENV PATH="/root/.local/bin/:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
ENV UV_SYSTEM_PYTHON=1
ENV UV_PROJECT_ENVIRONMENT="/usr/local"

RUN apt-get update && \
    apt-get install -y \
    git \
    curl \
    texlive-xetex \
    texlive-fonts-recommended \
    texlive-plain-generic && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml uv.lock ./
RUN uv sync --no-install-project
