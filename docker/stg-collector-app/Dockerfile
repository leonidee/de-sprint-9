FROM python:3.11.4-bullseye

USER root
WORKDIR /app

ARG YC_REDIS_HOST
ARG YC_REDIS_PORT
ARG YC_REDIS_PASSWORD

ARG YC_KAFKA_BOOTSTRAP_SERVERS
ARG YC_KAFKA_USERNAME
ARG YC_KAFKA_PASSWORD

ARG CERTIFICATE_PATH

ARG YC_POSTGRE_HOST
ARG YC_POSTGRE_PORT
ARG YC_POSTGRE_USERNAME
ARG YC_POSTGRE_PASSWORD
ARG YC_POSTGRE_DB

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download certificate
RUN curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" -o /app/CA.pem
RUN chmod 0600 /app/CA.pem

# Install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry $(which python3) -

ENV PATH="/opt/poetry/bin:$PATH"

COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock

RUN poetry env use $(which python3) \
    && . $(poetry env info --path)/bin/activate \
    && poetry install