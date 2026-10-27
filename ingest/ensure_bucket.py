"""Espera a MinIO/Silo y crea el bucket Medallion."""

from __future__ import annotations

import logging
import sys
import time

from config.settings import get_settings
from ingest.storage import build_s3_client, ensure_bucket

LOGGER = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    settings.require_storage_credentials()
    deadline = time.time() + 180
    while True:
        try:
            client = build_s3_client(settings)
            ensure_bucket(client, settings.s3_bucket)
            LOGGER.info("Bucket listo: %s", settings.s3_bucket)
            return 0
        except Exception as exc:
            if time.time() >= deadline:
                LOGGER.exception("No se pudo preparar MinIO/Silo: %s", exc)
                return 1
            LOGGER.warning("Esperando MinIO/Silo: %s", exc)
            time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
