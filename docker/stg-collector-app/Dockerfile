FROM python:3.11.4-bullseye

USER root
WORKDIR /app

ARG REDIS_HOST
ARG REDIS_PORT
ARG REDIS_PASSWORD
ARG CERTIFICATE_PATH

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download certificate
# RUN mkdir -p /crt
# RUN wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" --output-document /crt/YandexInternalRootCA.crt
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