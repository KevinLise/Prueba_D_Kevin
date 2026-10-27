"""Convenciones de rutas para la capa Bronze."""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID


VALID_KINDS = {"recent", "historical"}


def build_bronze_key(
    kind: str,
    run_id: UUID | str,
    ingested_at: datetime,
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    if kind not in VALID_KINDS:
        raise ValueError("kind debe ser 'recent' o 'historical'")
    if kind == "historical" and (start_date is None or end_date is None):
        raise ValueError("historical requiere start_date y end_date")

    timestamp = ingested_at.strftime("%Y%m%dT%H%M%SZ")
    identifier = str(run_id)
    if kind == "historical":
        suffix = f"{start_date.isoformat()}_{end_date.isoformat()}_{timestamp}_{identifier}"
    else:
        suffix = f"{timestamp}_{identifier}"
    return (
        f"bronze/source=usgs/kind={kind}/"
        f"ingest_date={ingested_at.date().isoformat()}/{kind}_{suffix}.geojson"
    )
