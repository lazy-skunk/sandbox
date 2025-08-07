FROM node:lts-slim

WORKDIR /frontend

RUN apt-get update \
    && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g npm@latest
