import pytest

from agent.plan_guard import UnsafePlanError, validate_plan


def test_accepts_valid_plan():
    plan = validate_plan(
        {
            "dataset": "events",
            "filters": {"min_magnitude": 4.0, "region": "Example", "days": 30},
            "sort": {"field": "magnitude", "order": "desc"},
            "limit": 20,
            "explicacion": "prueba",
        }
    )
    assert plan["dataset"] == "events"
    assert plan["filters"]["min_magnitude"] == 4.0
    assert plan["sort"] == {"field": "magnitude", "order": "desc"}


@pytest.mark.parametrize(
    "plan",
    [
        {"dataset": "raw_events", "limit": 10},
        {"dataset": "curated", "limit": 10},
        {"dataset": "events", "filters": {"sql": "select 1"}, "limit": 10},
        {"dataset": "events", "filters": {"min_magnitude": 99}, "limit": 10},
        {"dataset": "events", "sort": {"field": "url", "order": "desc"}, "limit": 10},
        {"dataset": "events", "sort": {"field": "magnitude", "order": "sideways"}, "limit": 10},
        {"dataset": "events", "limit": 10000},
        {"dataset": "summary", "sort": {"field": "total_events", "order": "desc"}, "limit": 1},
        {"dataset": "events", "limit": 10, "sql": "DROP TABLE x"},
    ],
)
def test_rejects_invalid_plans(plan):
    with pytest.raises(UnsafePlanError):
        validate_plan(plan)


def test_rejects_non_object_plan():
    with pytest.raises(UnsafePlanError):
        validate_plan("SELECT * FROM events")
