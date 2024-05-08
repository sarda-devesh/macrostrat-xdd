FROM python:3.11

RUN pip install arq httpx "weaviate-client==3.*"

WORKDIR /app
COPY *.py ./
COPY wrapper_classes wrapper_classes