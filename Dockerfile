FROM python:3.11-slim

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get -y upgrade

WORKDIR /app

COPY requirements.txt /app

COPY templates/ /app/templates

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -r requirements.txt

COPY /catalog_generator /app/catalog_generator


CMD ["python", "-m", "catalog_generator"]
