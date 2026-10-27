"""Consultas sismicas sobre MinIO Gold y consultas operativas sobre PostgreSQL."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from psycopg2.extras import RealDictCursor

from pipeline.gold_store import GoldStore

DATASET_COLUMNS = {
    "events": [
        "event_id",
        "event_time",
        "event_date",
        "magnitude",
        "magnitude_class",
        "place",
        "region",
        "latitude",
        "longitude",
        "depth_km",
        "tsunami",
        "url",
        "source_system",
        "source_file",
        "ingested_at",
    ],
    "daily_activity": [
        "event_date",
        "event_count",
        "avg_magnitude",
        "max_magnitude",
        "avg_depth_km",
    ],
    "magnitude_distribution": [
        "magnitude_class",
        "magnitude_min",
        "magnitude_max",
        "event_count",
        "avg_magnitude",
    ],
    "top_places": ["place", "event_count", "avg_magnitude", "max_magnitude"],
    "top_regions": ["region", "event_count", "avg_magnitude", "max_magnitude"],
    "summary": [
        "total_events",
        "avg_magnitude",
        "max_magnitude",
        "min_event_time",
        "max_event_time",
        "last_ingested_at",
        "generated_at",
    ],
    "pipeline_runs": [
        "run_id",
        "run_type",
        "status",
        "current_stage",
        "started_at",
        "completed_at",
        "duration_seconds",
        "records_received",
        "records_processed",
        "last_data_update",
        "error_message",
    ],
    "quality_metrics": [
        "run_id",
        "bronze_records",
        "valid_records",
        "invalid_records",
        "duplicate_records",
        "silver_records",
        "quality_percentage",
        "freshness_minutes",
        "recorded_at",
    ],
    "anomalies": [
        "anomaly_id",
        "window_start",
        "window_end",
        "scope",
        "anomaly_type",
        "severity",
        "z_score",
        "observed_count",
        "max_magnitude",
        "explanation",
    ],
}


def _clamp(value: int, default: int, maximum: int) -> int:
    if value is None:
        return default
    return max(1, min(int(value), maximum))


def _rows(conn, sql: str, params: tuple | list | None = None) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return [dict(row) for row in cur.fetchall()]


def _sort_rows(rows: list[dict], field: str, order: str = "desc") -> list[dict]:
    present = [row for row in rows if row.get(field) is not None]
    missing = [row for row in rows if row.get(field) is None]
    present.sort(key=lambda row: row[field], reverse=(order == "desc"))
    return present + missing


def _columns(dataset: str, rows: list[dict]) -> list[str]:
    if rows:
        return list(rows[0].keys())
    return DATASET_COLUMNS.get(dataset, [])


def _parse_event_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
    return None


def _filter_events(
    rows: list[dict],
    min_magnitude: float | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    region: str | None = None,
    place: str | None = None,
    days: int | None = None,
    hours: int | None = None,
) -> list[dict]:
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days) if days else None
    cutoff_dt = None
    if hours:
        parsed_times = [_parse_event_dt(r.get("event_time")) for r in rows]
        valid_times = [dt for dt in parsed_times if dt is not None]
        ref_time = max(valid_times) if valid_times else datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff_dt = ref_time - timedelta(hours=int(hours))
    region_text = region.lower() if region else None
    place_text = place.lower() if place else None
    result = []
    for row in rows:
        magnitude = row.get("magnitude")
        event_date = row.get("event_date")
        if min_magnitude is not None and (magnitude is None or magnitude < min_magnitude):
            continue
        if start_date is not None and (event_date is None or event_date < start_date):
            continue
        if end_date is not None and (event_date is None or event_date > end_date):
            continue
        if cutoff is not None and (event_date is None or event_date < cutoff):
            continue
        if cutoff_dt is not None:
            ev_dt = _parse_event_dt(row.get("event_time"))
            if ev_dt is None or ev_dt < cutoff_dt:
                continue
        if region_text and region_text not in str(row.get("region") or "").lower():
            continue
        if place_text and place_text not in str(row.get("place") or "").lower():
            continue
        result.append(row)
    return result


def fetch_summary(store: GoldStore) -> list[dict]:
    return store.read("summary")[:1]


def fetch_events(
    store: GoldStore,
    limit: int = 1000,
    min_magnitude: float | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    region: str | None = None,
    place: str | None = None,
) -> list[dict]:
    rows = _filter_events(
        store.read("events"),
        min_magnitude=min_magnitude,
        start_date=start_date,
        end_date=end_date,
        region=region,
        place=place,
    )
    return _sort_rows(rows, "event_time", "desc")[: _clamp(limit, 1000, 5000)]


def fetch_daily_activity(store: GoldStore, days: int = 30) -> list[dict]:
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=_clamp(days, 30, 3650))
    rows = [
        row
        for row in store.read("daily_activity")
        if row.get("event_date") is not None and row["event_date"] >= cutoff
    ]
    return _sort_rows(rows, "event_date", "asc")


def fetch_magnitude_distribution(store: GoldStore) -> list[dict]:
    return _sort_rows(store.read("magnitude_distribution"), "event_count", "desc")


def fetch_top_places(store: GoldStore, limit: int = 20) -> list[dict]:
    rows = _sort_rows(store.read("top_places"), "event_count", "desc")
    return rows[: _clamp(limit, 20, 500)]


def fetch_top_regions(store: GoldStore, limit: int = 20) -> list[dict]:
    rows = _sort_rows(store.read("top_regions"), "event_count", "desc")
    return rows[: _clamp(limit, 20, 500)]


def fetch_quality(conn, limit: int = 20) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT run_id::text AS run_id, bronze_records, valid_records,
               invalid_records, duplicate_records, silver_records,
               last_event_time, freshness_minutes, quality_percentage,
               generated_at, recorded_at
        FROM ops.quality_metrics
        ORDER BY recorded_at DESC
        LIMIT %s
        """,
        (_clamp(limit, 20, 500),),
    )


def fetch_runs(conn, limit: int = 20) -> list[dict]:
    return _rows(
        conn,
        """
        SELECT run_id::text AS run_id, run_type, source, airflow_run_id,
               started_at, finished_at, completed_at,
               CASE
                   WHEN status IN ('completed', 'successful') THEN 'successful'
                   WHEN status IN ('running', 'ingested', 'processed') THEN 'running'
                   ELSE status
               END AS status,
               current_stage, bronze_key, records_received, records_processed,
               records_bronze, records_silver, duration_seconds,
               last_data_update, error_message
        FROM ops.runs
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (_clamp(limit, 20, 500),),
    )


def fetch_metadata_summary(conn) -> dict:
    runs = _rows(
        conn,
        """
        SELECT run_id::text AS run_id, run_type, source, airflow_run_id,
               started_at, finished_at, completed_at,
               CASE
                   WHEN status IN ('completed', 'successful') THEN 'successful'
                   WHEN status IN ('running', 'ingested', 'processed') THEN 'running'
                   ELSE status
               END AS status,
               current_stage, bronze_key, records_received, records_processed,
               records_bronze, records_silver, duration_seconds,
               last_data_update, error_message
        FROM ops.runs
        ORDER BY started_at DESC
        LIMIT 1
        """,
    )
    latest_run = runs[0] if runs else None
    quality = []
    layers = []
    if latest_run:
        quality = _rows(
            conn,
            """
            SELECT run_id::text AS run_id, bronze_records, valid_records,
                   invalid_records, duplicate_records, silver_records,
                   last_event_time, freshness_minutes, quality_percentage,
                   generated_at, recorded_at
            FROM ops.quality_metrics
            WHERE run_id = %s
            """,
            (latest_run["run_id"],),
        )
        if not quality:
            quality = fetch_quality(conn, limit=1)
        layers = fetch_metadata_layers(conn, run_id=latest_run["run_id"])
        if len(layers) < 3 and quality:
            fallback_layers = fetch_metadata_layers(conn, run_id=quality[0]["run_id"])
            existing_names = {item.get("layer") for item in layers}
            for item in fallback_layers:
                if item.get("layer") not in existing_names:
                    layers.append(item)
    return {
        "run": latest_run,
        "quality": quality[0] if quality else None,
        "layers": layers,
    }


def fetch_metadata_layers(
    conn,
    limit: int = 50,
    run_id: str | None = None,
) -> list[dict]:
    if run_id:
        return _rows(
            conn,
            """
            SELECT layer_metadata_id, run_id::text AS run_id, layer, status,
                   records_in, records_out, records_duplicates, records_invalid,
                   location, format, event_time_min, event_time_max, details,
                   started_at, finished_at, updated_at
            FROM ops.layer_metadata
            WHERE run_id = %s
            ORDER BY CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END
            """,
            (run_id,),
        )
    return _rows(
        conn,
        """
        SELECT layer_metadata_id, run_id::text AS run_id, layer, status,
               records_in, records_out, records_duplicates, records_invalid,
               location, format, event_time_min, event_time_max, details,
               started_at, finished_at, updated_at
        FROM ops.layer_metadata
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (_clamp(limit, 50, 500),),
    )


def execute_plan(store: GoldStore, plan: dict, conn=None) -> tuple[list[str], list[dict]]:
    """Ejecuta un plan validado de solo lectura sobre Gold o PostgreSQL ops."""
    dataset = plan["dataset"]
    filters = plan.get("filters", {})
    sort = plan.get("sort") or {}
    limit = _clamp(plan.get("limit", 200), 200, 1000)

    if dataset == "events":
        rows = _filter_events(
            store.read("events"),
            min_magnitude=filters.get("min_magnitude"),
            region=filters.get("region"),
            place=filters.get("place"),
            days=filters.get("days"),
            hours=filters.get("hours"),
        )
        total_matched = len(rows)
        field = sort.get("field", "event_time")
        order = sort.get("order", "desc")
        rows = _sort_rows(rows, field, order)
        rows = rows[:limit]
        if rows:
            rows = [{**rows[0], "_total_matched": total_matched}] + rows[1:]
    elif dataset == "daily_activity":
        days = filters.get("days", 30)
        rows = fetch_daily_activity(store, days=days)
        if sort:
            rows = _sort_rows(rows, sort["field"], sort["order"])
        rows = rows[:limit]
    elif dataset in {"magnitude_distribution", "top_places", "top_regions"}:
        rows = store.read(dataset)
        field = sort.get("field", "event_count")
        order = sort.get("order", "desc")
        rows = _sort_rows(rows, field, order)[:limit]
    elif dataset == "summary":
        rows = fetch_summary(store)[:1]
    elif dataset in {"pipeline_runs", "quality_metrics", "anomalies"}:
        from agent.anomalies import detect_and_store_anomalies, fetch_anomalies
        from db.connection import connection

        def _query_ops(active_conn) -> list[dict]:
            if dataset == "pipeline_runs":
                data = fetch_runs(active_conn, limit=limit)
            elif dataset == "quality_metrics":
                data = fetch_quality(active_conn, limit=limit)
            else:
                data = fetch_anomalies(active_conn, limit=limit)
                if not data:
                    data = detect_and_store_anomalies(store, persist=False)[:limit]
            if sort and data:
                data = _sort_rows(data, sort["field"], sort["order"])
            return data[:limit]

        if conn is not None:
            rows = _query_ops(conn)
        else:
            with connection(readonly=True) as ro_conn:
                rows = _query_ops(ro_conn)
    else:
        rows = []

    cols = [c for c in _columns(dataset, rows) if c != "_total_matched"]
    return cols, rows
