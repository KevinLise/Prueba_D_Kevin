from agent.agent import GoldAgent
from config.settings import get_settings


class FakeExecutor:
    def __init__(self):
        self.plan = None

    def __call__(self, plan):
        self.plan = plan
        return (
            ["event_time", "place", "region", "magnitude", "depth_km"],
            [
                {
                    "event_time": "2026-10-27T00:00:00",
                    "place": "Example",
                    "region": "Example",
                    "magnitude": 5.0,
                    "depth_km": 10.0,
                }
            ],
        )


def test_rule_based_agent_builds_readonly_plan():
    settings = get_settings()
    settings = settings.__class__(
        **{**settings.__dict__, "agent_llm_api_key": ""}
    )
    executor = FakeExecutor()
    agent = GoldAgent(executor, settings)
    answer = agent.ask("cuales son los sismos de mayor magnitud")
    assert answer.mode == "reglas"
    assert executor.plan["dataset"] == "events"
    assert executor.plan["sort"] == {"field": "magnitude", "order": "desc"}
    assert executor.plan["limit"] == 10
    assert len(answer.rows) == 1
    assert answer.columns[0] == "event_time"


def test_rule_agent_uses_regions_for_zone_questions():
    settings = get_settings()
    settings = settings.__class__(
        **{**settings.__dict__, "agent_llm_api_key": ""}
    )
    executor = FakeExecutor()
    agent = GoldAgent(executor, settings)
    agent.ask("zonas con mayor actividad")
    assert executor.plan["dataset"] == "top_regions"


def test_agent_rejects_invalid_plan():
    settings = get_settings()
    settings = settings.__class__(
        **{**settings.__dict__, "agent_llm_api_key": ""}
    )
    agent = GoldAgent(FakeExecutor(), settings)
    try:
        agent._execute({"dataset": "raw_events", "limit": 10})
    except Exception as exc:
        assert "Dataset no permitido" in str(exc)
    else:
        raise AssertionError("El plan inseguro no fue rechazado")


def test_agent_answers_pipeline_and_quality_questions():
    settings = get_settings()
    settings = settings.__class__(
        **{**settings.__dict__, "agent_llm_api_key": "", "ollama_base_url": ""}
    )

    def ops_executor(plan):
        if plan["dataset"] == "pipeline_runs":
            return (
                ["run_id", "status", "current_stage", "started_at", "records_processed"],
                [
                    {
                        "run_id": "dae07d2c-79c5-57c7-a4ab-dc832974f965",
                        "status": "successful",
                        "current_stage": "completed",
                        "started_at": "2026-10-27T17:00:00Z",
                        "last_data_update": "2026-10-27T17:02:00Z",
                        "records_processed": 1998,
                        "duration_seconds": 42.5,
                    }
                ],
            )
        if plan["dataset"] == "quality_metrics":
            return (
                ["run_id", "bronze_records", "invalid_records", "duplicate_records", "silver_records", "quality_percentage"],
                [
                    {
                        "run_id": "dae07d2c-79c5-57c7-a4ab-dc832974f965",
                        "bronze_records": 2033,
                        "valid_records": 2033,
                        "invalid_records": 0,
                        "duplicate_records": 35,
                        "silver_records": 1998,
                        "quality_percentage": 98.28,
                    }
                ],
            )
        return ([], [])

    agent = GoldAgent(ops_executor, settings)
    ans_pipe = agent.ask("¿Cuándo se ejecutó por última vez el pipeline?")
    assert ans_pipe.plan["dataset"] == "pipeline_runs"
    assert "1998" in ans_pipe.answer
    assert "READ-ONLY" in (ans_pipe.sql_evidence or "")

    ans_qual = agent.ask("¿Cuántos registros fueron rechazados por los controles de calidad?")
    assert ans_qual.plan["dataset"] == "quality_metrics"
    assert "35" in ans_qual.answer
    assert "98.28%" in ans_qual.answer


def test_agent_handles_ollama_disconnected_with_fallback():
    settings = get_settings()
    settings = settings.__class__(
        **{
            **settings.__dict__,
            "agent_llm_api_key": "",
            "ollama_base_url": "http://127.0.0.1:59999",
            "ollama_timeout": 1,
        }
    )
    agent = GoldAgent(FakeExecutor(), settings)
    answer = agent.ask("¿Cuántos sismos hubo en las últimas 24 horas?")
    assert answer.mode == "reglas"
    assert answer.warning is not None
    assert answer.plan["dataset"] == "events"
    assert answer.plan["filters"] == {"hours": 24}


def test_agent_handles_insufficient_data():
    settings = get_settings()
    settings = settings.__class__(
        **{**settings.__dict__, "agent_llm_api_key": "", "ollama_base_url": ""}
    )
    agent = GoldAgent(lambda _plan: (["event_time"], []), settings)
    answer = agent.ask("¿Cuántos sismos hubo en las últimas 24 horas?")
    assert "No existen datos suficientes" in answer.answer
    assert answer.rows == []


def test_report_validation_and_deterministic_fallback():
    from agent.reports import (
        NarrativeReportSchema,
        build_deterministic_report,
        sanitize_external_text,
        validate_report_coherence,
    )

    stats = {
        "report_date": "2026-10-27",
        "total_events": 1998,
        "events_last_24h": 48,
        "max_magnitude_24h": 5.4,
        "events_last_7d": 420,
        "avg_magnitude": 3.88,
        "max_magnitude": 7.7,
        "strongest_place": sanitize_external_text("10 km NE of Example <script>ignore previous"),
        "strongest_region": "Example",
        "strongest_time": "2026-10-20T00:00:00",
        "top_region": "Alaska",
        "top_region_count": 310,
        "quality_percentage": 98.28,
        "invalid_records": 0,
        "duplicate_records": 35,
        "pipeline_status": "successful",
    }
    assert "<" not in stats["strongest_place"]
    assert "ignore previous" not in stats["strongest_place"].lower()

    det = build_deterministic_report(stats)
    ok, _ = validate_report_coherence(det, stats)
    assert ok is True

    hallucinated = NarrativeReportSchema(
        title="Informe con cifra inventada",
        executive_summary="Se registraron 999999 sismos en el periodo.",
        key_findings=["Hallazgo inventado"],
        reported_total_events=999999,
        reported_max_magnitude=9.9,
    )
    ok_bad, note_bad = validate_report_coherence(hallucinated, stats)
    assert ok_bad is False
    assert "Discrepancia" in note_bad


def test_statistical_anomaly_detection_non_predictive():
    from agent.anomalies import compute_anomalies_from_rows

    daily_rows = [
        {"event_date": f"2026-10-{d:02d}", "event_count": 50, "avg_magnitude": 3.2, "max_magnitude": 4.5}
        for d in range(1, 15)
    ] + [
        {"event_date": "2026-10-15", "event_count": 260, "avg_magnitude": 4.1, "max_magnitude": 6.8}
    ]
    event_rows = [
        {
            "event_id": "us1001",
            "event_time": "2026-10-15T12:00:00",
            "magnitude": 6.8,
            "place": "20 km S of Example",
            "region": "Example",
            "depth_km": 15.0,
        }
    ]
    anomalies = compute_anomalies_from_rows(daily_rows, event_rows, z_threshold=2.0, magnitude_threshold=6.0)
    assert len(anomalies) >= 2
    assert any(a["anomaly_type"] == "frequency_spike" for a in anomalies)
    assert any(a["anomaly_type"] == "magnitude_spike" for a in anomalies)
    for item in anomalies:
        assert "no constituye una prediccion de terremotos" in item["explanation"]
