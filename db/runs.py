"""Registro operativo de ejecuciones y control de la carga historica."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from psycopg2.extras import Json, RealDictCursor

from db.connection import connection

VALID_RUN_TYPES = {"recent", "historical"}
VALID_STATUSES = {"pending", "running", "ingested", "processed", "completed", "successful", "failed"}


def start_run(
    run_id: uuid.UUID | str,
    run_type: str,
    started_at: datetime,
    source: str = "usgs",
    airflow_run_id: str | None = None,
    current_stage: str = "ingest_recent",
) -> dict[str, Any]:
    """Crea o reanuda una ejecucion sin duplicarla en reintentos."""
    if run_type not in VALID_RUN_TYPES:
        raise ValueError(f"run_type invalido: {run_type}")
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO ops.runs (
                    run_id, run_type, source, airflow_run_id, started_at,
                    status, current_stage, records_received
                ) VALUES (%s, %s, %s, %s, %s, 'running', %s, 0)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'running',
                    current_stage = EXCLUDED.current_stage,
                    airflow_run_id = COALESCE(EXCLUDED.airflow_run_id, ops.runs.airflow_run_id),
                    error_message = NULL,
                    finished_at = NULL,
                    completed_at = NULL
                RETURNING run_id, run_type, source, started_at, status,
                          current_stage, records_received
                """,
                (
                    identifier,
                    run_type,
                    source,
                    airflow_run_id,
                    started_at,
                    current_stage,
                ),
            )
            row = dict(cur.fetchone())
    row["run_id"] = str(row["run_id"])
    return row


def mark_run_ingested(
    run_id: str,
    bronze_key: str,
    records_received: int,
    finished_at: datetime,
) -> None:
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.runs
                SET status = 'running',
                    current_stage = 'bronze',
                    bronze_key = %s,
                    records_received = %s,
                    finished_at = %s,
                    duration_seconds = EXTRACT(EPOCH FROM (%s - started_at))
                WHERE run_id = %s
                """,
                (bronze_key, records_received, finished_at, finished_at, identifier),
            )


def mark_stage(run_id: str, stage: str) -> None:
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.runs
                SET status = 'running',
                    current_stage = %s,
                    error_message = NULL
                WHERE run_id = %s
                """,
                (stage, identifier),
            )


def mark_run_completed(
    run_id: str,
    records_bronze: int | None = None,
    records_silver: int | None = None,
    records_processed: int | None = None,
) -> None:
    identifier = uuid.UUID(str(run_id))
    with connection(statement_timeout_ms=120000) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.runs
                SET status = 'successful',
                    current_stage = 'completed',
                    completed_at = now(),
                    last_data_update = now(),
                    records_bronze = COALESCE(%s, records_bronze),
                    records_silver = COALESCE(%s, records_silver),
                    records_processed = COALESCE(%s, records_processed),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)),
                    error_message = NULL
                WHERE run_id = %s
                """,
                (records_bronze, records_silver, records_processed, identifier),
            )


def mark_run_failed(
    run_id: str,
    error_message: str,
    stage: str | None = None,
) -> None:
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.runs
                SET status = 'failed',
                    current_stage = COALESCE(%s, current_stage),
                    error_message = %s,
                    finished_at = COALESCE(finished_at, now()),
                    duration_seconds = EXTRACT(EPOCH FROM (now() - started_at))
                WHERE run_id = %s
                """,
                (stage, error_message[:4000], identifier),
            )


def upsert_quality_metrics(run_id: str, metrics: dict[str, Any]) -> None:
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.quality_metrics (
                    run_id, bronze_records, valid_records, invalid_records,
                    duplicate_records, silver_records, last_event_time,
                    freshness_minutes, quality_percentage, generated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    bronze_records = EXCLUDED.bronze_records,
                    valid_records = EXCLUDED.valid_records,
                    invalid_records = EXCLUDED.invalid_records,
                    duplicate_records = EXCLUDED.duplicate_records,
                    silver_records = EXCLUDED.silver_records,
                    last_event_time = EXCLUDED.last_event_time,
                    freshness_minutes = EXCLUDED.freshness_minutes,
                    quality_percentage = EXCLUDED.quality_percentage,
                    generated_at = EXCLUDED.generated_at,
                    recorded_at = now()
                """,
                (
                    identifier,
                    metrics["bronze_records"],
                    metrics["valid_records"],
                    metrics["invalid_records"],
                    metrics["duplicate_records"],
                    metrics["silver_records"],
                    metrics.get("last_event_time"),
                    metrics.get("freshness_minutes"),
                    metrics.get("quality_percentage"),
                    metrics.get("generated_at"),
                ),
            )


def upsert_layer_metadata(
    run_id: str,
    layer: str,
    status: str,
    records_in: int | None = None,
    records_out: int | None = None,
    records_duplicates: int | None = None,
    records_invalid: int | None = None,
    location: str | None = None,
    format: str | None = None,
    event_time_min: datetime | None = None,
    event_time_max: datetime | None = None,
    details: dict[str, Any] | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError(f"layer invalido: {layer}")
    if status not in {"pending", "running", "successful", "failed"}:
        raise ValueError(f"status de capa invalido: {status}")
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.layer_metadata (
                    run_id, layer, status, records_in, records_out,
                    records_duplicates, records_invalid, location, format,
                    event_time_min, event_time_max, details, started_at, finished_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, layer) DO UPDATE SET
                    status = EXCLUDED.status,
                    records_in = EXCLUDED.records_in,
                    records_out = EXCLUDED.records_out,
                    records_duplicates = EXCLUDED.records_duplicates,
                    records_invalid = EXCLUDED.records_invalid,
                    location = EXCLUDED.location,
                    format = EXCLUDED.format,
                    event_time_min = EXCLUDED.event_time_min,
                    event_time_max = EXCLUDED.event_time_max,
                    details = EXCLUDED.details,
                    started_at = COALESCE(EXCLUDED.started_at, ops.layer_metadata.started_at),
                    finished_at = EXCLUDED.finished_at,
                    updated_at = now()
                """,
                (
                    identifier,
                    layer,
                    status,
                    records_in,
                    records_out,
                    records_duplicates,
                    records_invalid,
                    location,
                    format,
                    event_time_min,
                    event_time_max,
                    Json(details or {}),
                    started_at,
                    finished_at,
                ),
            )


def history_completed(load_name: str = "historical_30d") -> bool:
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM ops.load_control WHERE load_name = %s",
                (load_name,),
            )
            row = cur.fetchone()
    return bool(row and row[0] == "completed")


def mark_history_completed(load_name: str, run_id: str) -> None:
    identifier = uuid.UUID(str(run_id))
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.load_control (load_name, status, run_id, completed_at, updated_at)
                VALUES (%s, 'completed', %s, now(), now())
                ON CONFLICT (load_name) DO UPDATE
                SET status = 'completed',
                    run_id = EXCLUDED.run_id,
                    completed_at = EXCLUDED.completed_at,
                    updated_at = now()
                """,
                (load_name, identifier),
            )
