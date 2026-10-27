"""Registra en PostgreSQL operativo las metricas de la ultima ejecucion Spark."""

from __future__ import annotations

from datetime import datetime

from config.settings import get_settings
from db.runs import mark_run_completed, upsert_layer_metadata, upsert_quality_metrics
from pipeline.gold_store import GoldStore
from spark_jobs.pure import quality_percentage


TRANSFORMATIONS = [
    "explode_features",
    "normalize_types",
    "validate_required_fields",
    "validate_coordinates",
    "validate_magnitude_depth",
    "deduplicate_by_event_id",
    "write_parquet_snappy",
    "partition_by_event_date",
]

GOLD_DATASETS = [
    "daily_activity",
    "magnitude_distribution",
    "top_places",
    "top_regions",
    "summary",
    "events",
]


def _latest_quality(rows: list[dict]) -> dict:
    if not rows:
        raise RuntimeError("No existe gold/data_quality en MinIO")

    def key(row):
        value = row.get("generated_at")
        return value if isinstance(value, datetime) else datetime.min

    return max(rows, key=key)


def record_metrics(run_id: str) -> dict:
    settings = get_settings()
    store = GoldStore(settings)
    row = _latest_quality(store.read("data_quality"))
    if str(row.get("run_id")) != str(run_id):
        raise RuntimeError(
            f"gold/data_quality pertenece a run_id={row.get('run_id')} "
            f"y no a run_id={run_id}"
        )

    summary_rows = store.read("summary")
    summary = summary_rows[0] if summary_rows else {}
    event_min = summary.get("min_event_time")
    event_max = summary.get("max_event_time")

    quality_pct = row.get("quality_percentage")
    if quality_pct is None:
        quality_pct = quality_percentage(
            int(row["bronze_records"]),
            int(row["valid_records"]),
            int(row["duplicate_records"]),
            int(row["silver_records"]),
        )

    metrics = {
        "bronze_records": int(row["bronze_records"]),
        "valid_records": int(row["valid_records"]),
        "invalid_records": int(row["invalid_records"]),
        "duplicate_records": int(row["duplicate_records"]),
        "silver_records": int(row["silver_records"]),
        "last_event_time": row.get("last_event_time"),
        "freshness_minutes": row.get("freshness_minutes"),
        "quality_percentage": float(quality_pct),
        "generated_at": row.get("generated_at"),
    }
    upsert_quality_metrics(run_id, metrics)

    upsert_layer_metadata(
        run_id=run_id,
        layer="silver",
        status="successful",
        records_in=metrics["bronze_records"],
        records_out=metrics["silver_records"],
        records_duplicates=metrics["duplicate_records"],
        records_invalid=metrics["invalid_records"],
        location=settings.s3_uri("silver/events"),
        format="parquet",
        event_time_min=event_min,
        event_time_max=event_max,
        details={"transformations": TRANSFORMATIONS, "partition": "event_date"},
        finished_at=datetime.now(),
    )

    gold_counts = {dataset: store.count(dataset) for dataset in GOLD_DATASETS}
    upsert_layer_metadata(
        run_id=run_id,
        layer="gold",
        status="successful",
        records_in=metrics["silver_records"],
        records_out=gold_counts["events"],
        location=settings.s3_uri("gold"),
        format="parquet",
        event_time_min=event_min,
        event_time_max=event_max,
        details={
            "datasets": gold_counts,
            "generated": GOLD_DATASETS + ["data_quality"],
        },
        finished_at=datetime.now(),
    )

    mark_run_completed(
        run_id,
        records_bronze=metrics["bronze_records"],
        records_silver=metrics["silver_records"],
        records_processed=metrics["silver_records"],
    )

    try:
        from agent.anomalies import detect_and_store_anomalies
        from agent.reports import generate_daily_report

        detect_and_store_anomalies(store, settings=settings, persist=True)
        generate_daily_report(
            store,
            settings=settings,
            run_id=str(run_id),
            quality_metrics=metrics,
            persist=True,
        )
    except Exception:
        # Un fallo del modelo o del subsistema de IA no debe detener el pipeline.
        pass

    serializable = {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in metrics.items()
    }
    serializable["gold_counts"] = gold_counts
    return {"run_id": str(run_id), **serializable}
