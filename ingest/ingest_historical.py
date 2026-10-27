"""Carga historica controlada de USGS hacia Bronze."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

from config.settings import get_settings
from db.runs import (
    mark_history_completed,
    mark_run_completed,
    mark_run_failed,
    mark_run_ingested,
    start_run,
    upsert_layer_metadata,
)
from ingest.paths import build_bronze_key
from ingest.storage import build_s3_client, ensure_bucket, put_geojson
from ingest.usgs_client import build_session, event_time_range, extract_features, fetch_historical

LOGGER = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Fecha invalida: {value}. Usa YYYY-MM-DD") from exc


def run(
    start_date: date,
    end_date: date,
    min_magnitude: float | None = None,
    mark_control: bool = True,
    load_name: str = "historical_30d",
    run_id: uuid.UUID | str | None = None,
    airflow_run_id: str | None = None,
) -> dict:
    settings = get_settings()
    settings.require_storage_credentials()
    identifier = uuid.UUID(str(run_id)) if run_id else uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    start_run(
        run_id=identifier,
        run_type="historical",
        started_at=started_at,
        airflow_run_id=airflow_run_id,
        current_stage="historical_ingest",
    )
    try:
        session = build_session()
        text, payload = fetch_historical(
            session,
            settings,
            start_date=start_date,
            end_date=end_date,
            min_magnitude=min_magnitude,
        )
        features = extract_features(payload)
        ingested_at = datetime.now(timezone.utc)
        key = build_bronze_key(
            "historical",
            identifier,
            ingested_at,
            start_date=start_date,
            end_date=end_date,
        )
        client = build_s3_client(settings)
        ensure_bucket(client, settings.s3_bucket)
        put_geojson(
            client,
            settings.s3_bucket,
            key,
            text,
            metadata={
                "source": "usgs",
                "kind": "historical",
                "run_id": str(identifier),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
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
            details={
                "source": "usgs",
                "kind": "historical",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            started_at=started_at,
            finished_at=finished_at,
        )
        if mark_control:
            mark_history_completed(load_name=load_name, run_id=str(identifier))
        mark_run_completed(
            run_id=str(identifier),
            records_bronze=len(features),
            records_processed=len(features),
        )
        result = {
            "run_id": str(identifier),
            "airflow_run_id": airflow_run_id,
            "run_type": "historical",
            "bucket": settings.s3_bucket,
            "bronze_key": key,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "min_magnitude": settings.usgs_min_magnitude if min_magnitude is None else min_magnitude,
            "records_received": len(features),
            "event_time_min": event_min.isoformat() if event_min else None,
            "event_time_max": event_max.isoformat() if event_max else None,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "load_control": load_name if mark_control else None,
        }
        print(json.dumps(result, ensure_ascii=False))
        return result
    except Exception as exc:
        LOGGER.exception("Fallo la ingesta historica")
        try:
            mark_run_failed(str(identifier), str(exc), stage="historical_ingest")
        except Exception:
            LOGGER.exception("No se pudo marcar la ejecucion como fallida")
        raise


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    today = datetime.now(timezone.utc).date()
    parser = argparse.ArgumentParser(description="Carga historica de USGS a Bronze")
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=today - timedelta(days=settings.historical_days),
    )
    parser.add_argument("--end", type=_parse_date, default=today)
    parser.add_argument("--min-magnitude", type=float, default=None)
    parser.add_argument(
        "--no-mark-control",
        action="store_true",
        help="No marca la carga historica como completada en ops.load_control",
    )
    args = parser.parse_args()
    try:
        run(
            args.start,
            args.end,
            args.min_magnitude,
            mark_control=not args.no_mark_control,
        )
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
