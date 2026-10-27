"""Validador de planes de consulta de solo lectura sobre Gold."""

from __future__ import annotations

from typing import Any


ALLOWED_DATASETS = {
    "events",
    "daily_activity",
    "magnitude_distribution",
    "top_places",
    "top_regions",
    "summary",
    "pipeline_runs",
    "quality_metrics",
    "anomalies",
}
ALLOWED_FILTERS_BY_DATASET = {
    "events": {"min_magnitude", "region", "place", "days", "hours"},
    "daily_activity": {"days"},
    "magnitude_distribution": set(),
    "top_places": set(),
    "top_regions": set(),
    "summary": set(),
    "pipeline_runs": set(),
    "quality_metrics": set(),
    "anomalies": set(),
}
ALLOWED_ORDERS = {"asc", "desc"}
ALLOWED_SORT_FIELDS = {
    "events": {"event_time", "magnitude", "depth_km"},
    "daily_activity": {"event_date", "event_count", "avg_magnitude", "max_magnitude"},
    "magnitude_distribution": {"event_count", "avg_magnitude", "magnitude_class"},
    "top_places": {"event_count", "avg_magnitude", "max_magnitude"},
    "top_regions": {"event_count", "avg_magnitude", "max_magnitude"},
    "summary": set(),
    "pipeline_runs": {"started_at", "duration_seconds", "records_received", "records_processed"},
    "quality_metrics": {"recorded_at", "quality_percentage", "invalid_records", "duplicate_records"},
    "anomalies": {"detected_at", "z_score", "max_magnitude"},
}
PLAN_KEYS = {"dataset", "filters", "sort", "limit", "explicacion", "explanation"}


class UnsafePlanError(ValueError):
    """El plan de consulta no cumple las reglas."""


def _validate_filters(filters: Any, dataset: str) -> dict:
    if filters is None:
        return {}
    if not isinstance(filters, dict):
        raise UnsafePlanError("filters debe ser un objeto")
    unknown = set(filters) - ALLOWED_FILTERS_BY_DATASET[dataset]
    if unknown:
        raise UnsafePlanError("Filtros no permitidos: " + ", ".join(sorted(unknown)))

    result: dict[str, Any] = {}
    if "min_magnitude" in filters:
        try:
            magnitude = float(filters["min_magnitude"])
        except (TypeError, ValueError) as exc:
            raise UnsafePlanError("min_magnitude debe ser numerico") from exc
        if not -2.0 <= magnitude <= 10.0:
            raise UnsafePlanError("min_magnitude fuera de rango")
        result["min_magnitude"] = magnitude
    if "days" in filters:
        try:
            days = int(filters["days"])
        except (TypeError, ValueError) as exc:
            raise UnsafePlanError("days debe ser entero") from exc
        if not 1 <= days <= 3650:
            raise UnsafePlanError("days fuera de rango")
        result["days"] = days
    if "hours" in filters:
        try:
            hours = int(filters["hours"])
        except (TypeError, ValueError) as exc:
            raise UnsafePlanError("hours debe ser entero") from exc
        if not 1 <= hours <= 720:
            raise UnsafePlanError("hours fuera de rango")
        result["hours"] = hours
    for key in ("region", "place"):
        if key in filters:
            value = filters[key]
            if not isinstance(value, str) or not value.strip() or len(value) > 200:
                raise UnsafePlanError(f"{key} debe ser texto de hasta 200 caracteres")
            result[key] = value.strip()
    return result


def validate_plan(plan: Any, max_limit: int = 500) -> dict:
    if not isinstance(plan, dict):
        raise UnsafePlanError("El plan debe ser un objeto JSON")
    unknown = set(plan) - PLAN_KEYS
    if unknown:
        raise UnsafePlanError("Claves no permitidas: " + ", ".join(sorted(unknown)))

    dataset = plan.get("dataset")
    if dataset not in ALLOWED_DATASETS:
        raise UnsafePlanError(f"Dataset no permitido: {dataset}")

    filters = _validate_filters(plan.get("filters"), dataset)
    limit = plan.get("limit", 200)
    try:
        limit = int(limit)
    except (TypeError, ValueError) as exc:
        raise UnsafePlanError("limit debe ser entero") from exc
    if not 1 <= limit <= max_limit:
        raise UnsafePlanError(f"limit debe estar entre 1 y {max_limit}")

    sort = plan.get("sort")
    normalized_sort = None
    if sort is not None:
        if not isinstance(sort, dict):
            raise UnsafePlanError("sort debe ser un objeto")
        if set(sort) - {"field", "order"}:
            raise UnsafePlanError("sort solo admite field y order")
        field = sort.get("field")
        order = str(sort.get("order", "desc")).lower()
        if field not in ALLOWED_SORT_FIELDS[dataset]:
            raise UnsafePlanError(f"Campo de orden no permitido: {field}")
        if order not in ALLOWED_ORDERS:
            raise UnsafePlanError("order debe ser asc o desc")
        normalized_sort = {"field": field, "order": order}

    return {
        "dataset": dataset,
        "filters": filters,
        "sort": normalized_sort,
        "limit": limit,
    }
