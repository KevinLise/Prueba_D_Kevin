import json
from datetime import date

import pytest

from config.settings import get_settings
from ingest.usgs_client import (
    UsgsError,
    extract_features,
    fetch_historical,
    fetch_recent,
    validate_feature_collection,
)


SAMPLE = {
    "type": "FeatureCollection",
    "metadata": {"count": 1},
    "features": [
        {
            "type": "Feature",
            "id": "us1000",
            "properties": {
                "mag": 4.2,
                "place": "Example",
                "time": 1700000000000,
                "updated": 1700000100000,
                "type": "earthquake",
            },
            "geometry": {"type": "Point", "coordinates": [-70.0, 10.0, 12.0]},
        }
    ],
}


class FakeResponse:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.response


def test_extract_features():
    features = extract_features(SAMPLE)
    assert len(features) == 1
    assert features[0]["id"] == "us1000"


def test_validate_feature_collection_rejects_invalid_payload():
    with pytest.raises(UsgsError):
        validate_feature_collection({"type": "FeatureCollection"})
    with pytest.raises(UsgsError):
        validate_feature_collection([])


def test_fetch_recent():
    session = FakeSession(FakeResponse(json.dumps(SAMPLE)))
    settings = get_settings()
    text, payload = fetch_recent(session, settings)
    assert "FeatureCollection" in text
    assert len(payload["features"]) == 1
    assert session.calls[0]["url"] == settings.usgs_recent_url


def test_fetch_historical_builds_expected_parameters():
    session = FakeSession(FakeResponse(json.dumps(SAMPLE)))
    settings = get_settings()
    _, payload = fetch_historical(
        session,
        settings,
        start_date=date(2026, 10, 20),
        end_date=date(2026, 10, 27),
        min_magnitude=2.5,
    )
    params = session.calls[0]["params"]
    assert params["starttime"] == "2026-10-20T00:00:00"
    assert params["endtime"] == "2026-10-27T23:59:59"
    assert params["minmagnitude"] == 2.5
    assert params["format"] == "geojson"
    assert len(payload["features"]) == 1
