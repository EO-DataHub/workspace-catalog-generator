FROM python:3.11-slim

RUN apt-get update && \
    apt-get -y upgrade

WORKDIR /app

COPY requirements.txt /app

COPY templates/ /app/templates

RUN pip install -r requirements.txt

COPY /catalog_generator /app/catalog_generator


CMD ["python", "-m", "catalog_generator"]
