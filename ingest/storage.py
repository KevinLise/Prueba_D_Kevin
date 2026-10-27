"""Utilidades de escritura en MinIO mediante la API S3."""

from __future__ import annotations

import logging
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config.settings import Settings

LOGGER = logging.getLogger(__name__)


def build_s3_client(settings: Settings):
    settings.require_storage_credentials()
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code not in {"404", "NoSuchBucket", "NotFound"} and status != 404:
            raise
    client.create_bucket(Bucket=bucket)
    LOGGER.info("Bucket creado: %s", bucket)


def put_geojson(
    client,
    bucket: str,
    key: str,
    body: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    safe_metadata = {str(k): str(v) for k, v in (metadata or {}).items()}
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/geo+json",
        Metadata=safe_metadata,
    )
    LOGGER.info("Objeto escrito: s3://%s/%s (%s bytes)", bucket, key, len(body.encode("utf-8")))
