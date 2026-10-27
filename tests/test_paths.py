from datetime import date, datetime, timezone
from uuid import UUID

from ingest.paths import build_bronze_key


RUN_ID = UUID("12345678-1234-5678-1234-567812345678")


def test_recent_key_contains_source_kind_and_date():
    key = build_bronze_key(
        "recent",
        RUN_ID,
        datetime(2026, 10, 27, 12, 30, tzinfo=timezone.utc),
    )
    assert key == (
        "bronze/source=usgs/kind=recent/ingest_date=2026-10-27/"
        "recent_20261027T123000Z_12345678-1234-5678-1234-567812345678.geojson"
    )


def test_historical_key_contains_range():
    key = build_bronze_key(
        "historical",
        RUN_ID,
        datetime(2026, 10, 27, 12, 30, tzinfo=timezone.utc),
        start_date=date(2026, 9, 27),
        end_date=date(2026, 10, 27),
    )
    assert "kind=historical" in key
    assert "2026-09-27_2026-10-27" in key
    assert key.endswith(".geojson")
