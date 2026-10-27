from datetime import date, datetime, timezone

from spark_jobs.pure import (
    deduplicate_records,
    feature_to_record,
    is_valid_record,
    magnitude_class,
    parse_epoch_millis,
    quality_percentage,
    region_from_place,
)


FEATURE = {
    "id": "us1000",
    "properties": {
        "mag": 5.2,
        "place": "10 km S of Example",
        "time": 1700000000000,
        "updated": 1700000100000,
        "tsunami": 0,
        "sig": 400,
        "status": "reviewed",
        "magType": "mw",
        "type": "earthquake",
        "url": "https://example.local/event",
    },
    "geometry": {"coordinates": [-70.1, 10.2, 15.5]},
}


def test_magnitude_class_thresholds():
    assert magnitude_class(-0.5) == "micro"
    assert magnitude_class(2.5) == "menor"
    assert magnitude_class(4.5) == "moderado"
    assert magnitude_class(6.0) == "fuerte"
    assert magnitude_class(7.0) == "mayor"
    assert magnitude_class(None) is None


def test_parse_epoch_millis():
    parsed = parse_epoch_millis(1700000000000)
    assert parsed is not None
    assert parsed.tzinfo == timezone.utc
    assert parse_epoch_millis(None) is None
    assert parse_epoch_millis(-1) is None


def test_region_from_place():
    assert region_from_place("10 km S of Example") == "Example"
    assert region_from_place("Example, California") == "California"
    assert region_from_place("Off the coast of Japan") == "Japan"
    assert region_from_place(None) is None


def test_feature_to_record_normalizes_fields():
    record = feature_to_record(FEATURE, ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert record["id"] == "us1000"
    assert record["magnitude"] == 5.2
    assert record["magnitude_class"] == "moderado"
    assert record["place"] == "10 km S of Example"
    assert record["region"] == "Example"
    assert record["latitude"] == 10.2
    assert record["longitude"] == -70.1
    assert record["depth_km"] == 15.5
    assert record["event_date"] == date(2023, 11, 14)
    assert record["source_system"] == "usgs"


def test_valid_record():
    record = feature_to_record(FEATURE)
    assert is_valid_record(record, today=date(2026, 10, 27))


def test_invalid_records_are_rejected():
    base = feature_to_record(FEATURE)
    assert not is_valid_record({**base, "magnitude": 11.0}, today=date(2026, 10, 27))
    assert not is_valid_record({**base, "latitude": 100.0}, today=date(2026, 10, 27))
    assert not is_valid_record({**base, "event_type": "quarry blast"}, today=date(2026, 10, 27))
    assert not is_valid_record({**base, "id": ""}, today=date(2026, 10, 27))


def test_deduplicate_records_keeps_latest_updated():
    old = feature_to_record({**FEATURE, "properties": {**FEATURE["properties"], "updated": 100}})
    new = feature_to_record({**FEATURE, "properties": {**FEATURE["properties"], "updated": 200}})
    other = feature_to_record(
        {**FEATURE, "id": "us2000", "properties": {**FEATURE["properties"], "updated": 50}}
    )
    result = deduplicate_records([old, new, other])
    assert len(result) == 2
    by_id = {record["id"]: record for record in result}
    assert by_id["us1000"]["updated_ms"] == 200


def test_quality_percentage():
    assert quality_percentage(100, 90, 5, 85) == 85.0
    assert quality_percentage(0, 0, 0, 0) == 0.0
    assert quality_percentage(10, 10, 0, 10) == 100.0
    assert quality_percentage(10, 5, 6, 0) == 0.0
