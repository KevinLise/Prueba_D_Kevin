"""Funciones puras compartidas por Spark y por las pruebas.

No importan pyspark para poder probarse sin dependencias de Spark.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable


MIN_LATITUDE = -90.0
MAX_LATITUDE = 90.0
MIN_LONGITUDE = -180.0
MAX_LONGITUDE = 180.0
MIN_MAGNITUDE = -1.5
MAX_MAGNITUDE = 10.0
MIN_DEPTH_KM = -20.0
MAX_DEPTH_KM = 800.0
MIN_EVENT_DATE = date(1900, 1, 1)


def magnitude_class(magnitude: Any) -> str | None:
    """Clasifica la magnitud en una etiqueta legible."""
    if magnitude is None:
        return None
    try:
        value = float(magnitude)
    except (TypeError, ValueError):
        return None
    if value != value:  # NaN
        return None
    if value < 2.5:
        return "micro"
    if value < 4.5:
        return "menor"
    if value < 6.0:
        return "moderado"
    if value < 7.0:
        return "fuerte"
    return "mayor"


def parse_epoch_millis(value: Any) -> datetime | None:
    """Convierte milisegundos epoch de USGS a datetime UTC."""
    if value is None or value == "":
        return None
    try:
        millis = float(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0 or millis != millis:
        return None
    try:
        return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def region_from_place(place: Any) -> str | None:
    """Deriva una region textual desde el campo libre place de USGS.

    No inventa un pais: solo extrae el texto disponible despues de una coma
    o despues de la palabra "of".
    """
    if place is None:
        return None
    text = str(place).strip()
    if not text:
        return None
    if "," in text:
        candidate = text.rsplit(",", 1)[1].strip()
        if candidate:
            return candidate
    match = re.search(r"\bof\s+(.+)$", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def feature_to_record(
    feature: dict[str, Any],
    source_system: str = "usgs",
    ingested_at: datetime | None = None,
) -> dict[str, Any]:
    """Normaliza una feature GeoJSON de USGS a un registro plano."""
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coordinates = geometry.get("coordinates") or []
    longitude = _as_float(coordinates[0]) if len(coordinates) > 0 else None
    latitude = _as_float(coordinates[1]) if len(coordinates) > 1 else None
    depth_km = _as_float(coordinates[2]) if len(coordinates) > 2 else None
    event_time = parse_epoch_millis(properties.get("time"))
    updated_time = parse_epoch_millis(properties.get("updated"))
    now = ingested_at or datetime.now(timezone.utc)
    magnitude = _as_float(properties.get("mag"))
    return {
        "id": str(feature.get("id")) if feature.get("id") is not None else None,
        "magnitude": magnitude,
        "magnitude_class": magnitude_class(magnitude),
        "place": properties.get("place"),
        "region": region_from_place(properties.get("place")),
        "event_time": event_time,
        "event_date": event_time.date() if event_time else None,
        "updated_ms": _as_int(properties.get("updated")),
        "updated_time": updated_time,
        "tsunami": _as_int(properties.get("tsunami")),
        "significance": _as_int(properties.get("sig")),
        "status": properties.get("status"),
        "mag_type": properties.get("magType"),
        "event_type": properties.get("type"),
        "url": properties.get("url"),
        "longitude": longitude,
        "latitude": latitude,
        "depth_km": depth_km,
        "source_system": source_system,
        "ingested_at": now,
    }


def is_valid_record(record: dict[str, Any], today: date | None = None) -> bool:
    """Aplica las mismas reglas de calidad que el job de Spark."""
    current_day = today or datetime.now(timezone.utc).date()
    event_date = record.get("event_date")
    magnitude = record.get("magnitude")
    latitude = record.get("latitude")
    longitude = record.get("longitude")
    depth_km = record.get("depth_km")

    if not record.get("id") or not str(record.get("id")).strip():
        return False
    if record.get("event_type") != "earthquake":
        return False
    if magnitude is None or not (MIN_MAGNITUDE <= float(magnitude) <= MAX_MAGNITUDE):
        return False
    if not isinstance(event_date, date):
        return False
    if event_date < MIN_EVENT_DATE or event_date > current_day + timedelta(days=1):
        return False
    if latitude is None or not (MIN_LATITUDE <= float(latitude) <= MAX_LATITUDE):
        return False
    if longitude is None or not (MIN_LONGITUDE <= float(longitude) <= MAX_LONGITUDE):
        return False
    if depth_km is None or not (MIN_DEPTH_KM <= float(depth_km) <= MAX_DEPTH_KM):
        return False
    return True


def quality_percentage(
    received: int,
    valid: int,
    duplicates: int,
    silver: int,
) -> float:
    """Porcentaje de registros utilizables sobre los recibidos.

    Formula: (validos - duplicados) / recibidos * 100.
    Si recibidos es cero, devuelve 0.0 para evitar divisiones entre cero.
    """
    if received <= 0:
        return 0.0
    usable = max(0, int(valid) - int(duplicates))
    value = (usable / int(received)) * 100.0
    return round(max(0.0, min(100.0, value)), 2)


def deduplicate_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplica por id conservando la version con updated_ms mas reciente."""
    best: dict[str, dict[str, Any]] = {}
    for record in records:
        event_id = record.get("id")
        if not event_id:
            continue
        current = best.get(event_id)
        if current is None:
            best[event_id] = record
            continue
        new_updated = record.get("updated_ms") or 0
        old_updated = current.get("updated_ms") or 0
        new_event = record.get("event_time")
        old_event = current.get("event_time")
        if new_updated > old_updated:
            best[event_id] = record
        elif new_updated == old_updated and new_event and old_event and new_event > old_event:
            best[event_id] = record
    return list(best.values())
