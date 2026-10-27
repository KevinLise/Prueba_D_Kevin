FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app
COPY requirements-minio.txt /app/requirements-minio.txt
RUN pip install --no-cache-dir -r /app/requirements-minio.txt

COPY config /app/config
COPY ingest /app/ingest

CMD ["python", "-m", "ingest.ensure_bucket"]
