FROM python:3.13-slim

WORKDIR /playground

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    texlive-xetex \
    texlive-fonts-recommended \
    texlive-plain-generic \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
