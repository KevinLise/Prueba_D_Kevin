import pytest

pytest.importorskip("pyspark")

from datetime import datetime

from pyspark.sql import Row, SparkSession

from common import deduplicate_events


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("sismowatch-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_deduplicate_events_keeps_latest(spark):
    rows = [
        Row(id="a", updated_time=datetime(2026, 1, 1, 10, 0), event_time=datetime(2026, 1, 1), magnitude=4.0),
        Row(id="a", updated_time=datetime(2026, 1, 1, 11, 0), event_time=datetime(2026, 1, 1), magnitude=5.0),
        Row(id="b", updated_time=datetime(2026, 1, 1, 9, 0), event_time=datetime(2026, 1, 1), magnitude=3.0),
    ]
    df = spark.createDataFrame(rows)
    result = {row["id"]: row for row in deduplicate_events(df).collect()}
    assert len(result) == 2
    assert result["a"]["magnitude"] == 5.0
    assert result["b"]["magnitude"] == 3.0
