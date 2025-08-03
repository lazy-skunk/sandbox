FROM python:latest

WORKDIR /backend

RUN apt-get update \
   && apt-get install -y \
   git \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"
COPY pyproject.toml poetry.lock poetry.toml ./
RUN poetry install --no-root --with dev

COPY . .
