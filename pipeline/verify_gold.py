"""Verificacion de los Parquet Gold en MinIO."""

from __future__ import annotations

import argparse
import json
import sys

from config.settings import get_settings
from ingest.storage import build_s3_client

EXPECTED = [
    "daily_activity",
    "magnitude_distribution",
    "top_places",
    "top_regions",
    "summary",
    "events",
    "data_quality",
]
REQUIRED = ("summary", "data_quality")


def count_parquet(client, bucket: str, prefix: str) -> int:
    paginator = client.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".parquet"):
                total += 1
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Verifica archivos Parquet en Gold")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Falla si algun dataset Gold esperado no tiene Parquet",
    )
    args = parser.parse_args()

    settings = get_settings()
    settings.require_storage_credentials()
    client = build_s3_client(settings)
    result = {"datasets": {}, "total_parquet": 0}
    for dataset in EXPECTED:
        count = count_parquet(client, settings.s3_bucket, f"gold/{dataset}/")
        result["datasets"][dataset] = count
        result["total_parquet"] += count
    print(json.dumps(result, ensure_ascii=False))

    missing_required = [name for name in REQUIRED if result["datasets"].get(name, 0) == 0]
    if missing_required:
        return 1
    if args.strict and any(count == 0 for count in result["datasets"].values()):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
