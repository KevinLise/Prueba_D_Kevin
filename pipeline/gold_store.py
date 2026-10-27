"""Lectura backend de los Parquet Gold almacenados en MinIO."""

from __future__ import annotations

import logging

import pyarrow.dataset as ds
import pyarrow.fs as pafs

from config.settings import Settings

LOGGER = logging.getLogger(__name__)

DATASETS = {
    "daily_activity",
    "magnitude_distribution",
    "top_places",
    "top_regions",
    "summary",
    "events",
    "data_quality",
}


def _is_missing(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in ("path does not exist", "no such file", "not found", "no files")
    )


class GoldStore:
    def __init__(self, settings: Settings) -> None:
        settings.require_storage_credentials()
        self.bucket = settings.s3_bucket
        self.filesystem = pafs.S3FileSystem(
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            endpoint_override=settings.minio_endpoint_host,
            scheme="https" if settings.minio_secure else "http",
            region="us-east-1",
            force_virtual_addressing=False,
        )

    def _path(self, dataset: str) -> str:
        if dataset not in DATASETS:
            raise ValueError(f"Dataset Gold no permitido: {dataset}")
        return f"{self.bucket}/gold/{dataset}"

    def read(self, dataset: str, columns: list[str] | None = None) -> list[dict]:
        path = self._path(dataset)
        try:
            table = ds.dataset(
                path,
                filesystem=self.filesystem,
                format="parquet",
            ).to_table(columns=columns)
        except FileNotFoundError:
            LOGGER.warning("Dataset Gold inexistente: %s", path)
            return []
        except Exception as exc:
            if _is_missing(exc):
                LOGGER.warning("Dataset Gold vacio o inexistente: %s", path)
                return []
            raise
        return table.to_pylist()

    def count(self, dataset: str) -> int:
        path = self._path(dataset)
        try:
            return int(
                ds.dataset(
                    path,
                    filesystem=self.filesystem,
                    format="parquet",
                ).count_rows()
            )
        except FileNotFoundError:
            LOGGER.warning("Dataset Gold inexistente: %s", path)
            return 0
        except Exception as exc:
            if _is_missing(exc):
                LOGGER.warning("Dataset Gold vacio o inexistente: %s", path)
                return 0
            raise

    def health(self) -> bool:
        try:
            info = self.filesystem.get_file_info(self.bucket)
        except Exception:
            LOGGER.exception("No se pudo comprobar Gold en MinIO")
            return False
        return info.type != pafs.FileType.NotFound
