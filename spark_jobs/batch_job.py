"""Spark Batch Medallion: Bronze -> Silver -> Gold en MinIO."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid

from config.settings import get_settings
from common import (
    build_gold_tables,
    build_quality_metrics,
    build_spark,
    deduplicate_events,
    normalize_features,
    path_exists,
    read_bronze_features,
    write_dataset,
)

LOGGER = logging.getLogger(__name__)


def _iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Procesamiento Spark Batch de SismoWatch")
    parser.add_argument("--run-id", required=True, help="UUID de la ejecucion de Airflow")
    args = parser.parse_args()
    try:
        run_id = str(uuid.UUID(args.run_id))
    except ValueError:
        LOGGER.error("run-id invalido: %s", args.run_id)
        return 2

    settings = get_settings()
    spark = None
    try:
        spark = build_spark("SismoWatch Medallion Batch", settings)
        bronze_path = settings.s3a_uri("bronze/source=usgs")
        silver_path = settings.s3a_uri("silver/events")
        gold_base = settings.s3a_uri("gold")

        if not path_exists(spark, bronze_path):
            raise RuntimeError(
                f"No existe Bronze en {bronze_path}. "
                "Ejecuta la carga historica o la ingesta reciente."
            )

        features = read_bronze_features(spark, bronze_path)
        bronze_records = features.count()
        valid = normalize_features(features)
        valid_records = valid.count()
        silver = deduplicate_events(valid).cache()
        silver_records = silver.count()
        duplicate_records = max(0, valid_records - silver_records)

        write_dataset(silver, silver_path, partition_by=["event_date"])

        gold_tables = build_gold_tables(silver)
        for name, dataframe in gold_tables.items():
            write_dataset(dataframe, f"{gold_base}/{name}")

        quality = build_quality_metrics(
            silver=silver,
            run_id=run_id,
            bronze_records=bronze_records,
            valid_records=valid_records,
            duplicate_records=duplicate_records,
        )
        write_dataset(quality, f"{gold_base}/data_quality")

        quality_metrics = {
            key: _iso(value) for key, value in quality.first().asDict().items()
        }
        result = {
            "run_id": run_id,
            "bronze": bronze_path,
            "silver": silver_path,
            "gold": gold_base,
            "bronze_records": bronze_records,
            "valid_records": valid_records,
            "duplicate_records": duplicate_records,
            "silver_records": silver_records,
            "invalid_records": bronze_records - valid_records,
            "quality": quality_metrics,
        }
        LOGGER.info("Batch Medallion completado: %s", json.dumps(result, ensure_ascii=False))
        print(json.dumps(result, ensure_ascii=False))
        silver.unpersist()
        return 0
    except Exception:
        LOGGER.exception("Fallo el procesamiento Spark Batch")
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
