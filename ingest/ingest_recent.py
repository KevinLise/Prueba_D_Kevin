"""Ingesta del feed reciente de USGS hacia Bronze, ejecutada por Airflow."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone

from config.settings import get_settings
from db.runs import mark_run_failed, mark_run_ingested, start_run, upsert_layer_metadata
from ingest.paths import build_bronze_key
from ingest.storage import build_s3_client, ensure_bucket, put_geojson
from ingest.usgs_client import build_session, event_time_range, extract_features, fetch_recent

LOGGER = logging.getLogger(__name__)


def run(
    run_id: uuid.UUID | str | None = None,
    airflow_run_id: str | None = None,
) -> dict:
    settings = get_settings()
    settings.require_storage_credentials()
    identifier = uuid.UUID(str(run_id)) if run_id else uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    start_run(
        run_id=identifier,
        run_type="recent",
        started_at=started_at,
        airflow_run_id=airflow_run_id,
        current_stage="ingest_recent",
    )
    try:
        session = build_session()
        text, payload = fetch_recent(session, settings)
        features = extract_features(payload)
        ingested_at = datetime.now(timezone.utc)
        key = build_bronze_key("recent", identifier, ingested_at)
        client = build_s3_client(settings)
        ensure_bucket(client, settings.s3_bucket)
        put_geojson(
            client,
            settings.s3_bucket,
            key,
            text,
            metadata={
                "source": "usgs",
                "kind": "recent",
                "run_id": str(identifier),
                "records_received": len(features),
                "ingested_at": ingested_at.isoformat(),
            },
        )
        finished_at = datetime.now(timezone.utc)
        event_min, event_max = event_time_range(features)
        mark_run_ingested(
            run_id=str(identifier),
            bronze_key=key,
            records_received=len(features),
            finished_at=finished_at,
        )
        upsert_layer_metadata(
            run_id=str(identifier),
            layer="bronze",
            status="successful",
            records_in=len(features),
            records_out=len(features),
            location=settings.s3_uri(key),
            format="geojson",
            event_time_min=event_min,
            event_time_max=event_max,
            details={"source": "usgs", "kind": "recent"},
            started_at=started_at,
            finished_at=finished_at,
        )
        result = {
            "run_id": str(identifier),
            "airflow_run_id": airflow_run_id,
            "run_type": "recent",
            "bronze_key": key,
            "records_received": len(features),
            "event_time_min": event_min.isoformat() if event_min else None,
            "event_time_max": event_max.isoformat() if event_max else None,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
        }
        print(json.dumps(result, ensure_ascii=False))
        return result
    except Exception as exc:
        LOGGER.exception("Fallo la ingesta reciente")
        try:
            mark_run_failed(str(identifier), str(exc), stage="ingest_recent")
        except Exception:
            LOGGER.exception("No se pudo marcar la ejecucion como fallida")
        raise


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Ingesta del feed reciente de USGS a Bronze")
    parser.parse_args()
    try:
        run()
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
