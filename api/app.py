"""API de solo lectura de SismoWatch."""

from __future__ import annotations

from datetime import date
import uuid

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from agent.agent import SUGGESTED_QUESTIONS, GoldAgent
from agent.anomalies import detect_and_store_anomalies, fetch_anomalies
from agent.audit import fetch_ai_audit_logs
from agent.reports import export_report, fetch_daily_reports, generate_daily_report
from api import queries
from config.settings import get_settings
from db.connection import connection
from pipeline.gold_store import GoldStore

settings = get_settings()
gold_store = GoldStore(settings)
app = FastAPI(title="SismoWatch API", version="3.0.0")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class AgentQuestion(BaseModel):
    question: str = Field(min_length=1, max_length=500)


def get_connection():
    with connection(settings, readonly=True) as conn:
        yield conn


def get_gold_store() -> GoldStore:
    return gold_store


def require_api_key(x_api_key: str | None = Depends(api_key_header)):
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="API key invalida o ausente")


def require_write_role(x_user_role: str | None = Header(default="admin")):
    role = (x_user_role or "admin").strip().lower()
    if role == "viewer":
        raise HTTPException(
            status_code=403,
            detail="El rol 'viewer' solo tiene permisos de lectura y no puede ejecutar esta operacion.",
        )
    if role not in {"admin", "analyst"}:
        raise HTTPException(status_code=403, detail=f"Rol no autorizado: {role}")
    return role


router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


@app.get("/health")
def health():
    try:
        with connection(settings, readonly=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL no disponible: {exc}") from exc
    if not gold_store.health():
        raise HTTPException(status_code=503, detail="MinIO no disponible")
    return {"status": "ok", "database": "ok", "gold": "ok"}


@router.get("/summary")
def summary(store: GoldStore = Depends(get_gold_store)):
    return {"data": queries.fetch_summary(store)}


@router.get("/events")
def events(
    limit: int = Query(1000, ge=1, le=5000),
    min_magnitude: float | None = Query(None, ge=-2.0, le=10.0),
    start_date: date | None = None,
    end_date: date | None = None,
    region: str | None = None,
    place: str | None = None,
    store: GoldStore = Depends(get_gold_store),
):
    return {
        "data": queries.fetch_events(
            store,
            limit=limit,
            min_magnitude=min_magnitude,
            start_date=start_date,
            end_date=end_date,
            region=region,
            place=place,
        )
    }


@router.get("/daily-activity")
def daily_activity(
    days: int = Query(30, ge=1, le=3650),
    store: GoldStore = Depends(get_gold_store),
):
    return {"data": queries.fetch_daily_activity(store, days=days)}


@router.get("/magnitude-distribution")
def magnitude_distribution(store: GoldStore = Depends(get_gold_store)):
    return {"data": queries.fetch_magnitude_distribution(store)}


@router.get("/top-places")
def top_places(
    limit: int = Query(20, ge=1, le=500),
    store: GoldStore = Depends(get_gold_store),
):
    return {"data": queries.fetch_top_places(store, limit=limit)}


@router.get("/top-regions")
def top_regions(
    limit: int = Query(20, ge=1, le=500),
    store: GoldStore = Depends(get_gold_store),
):
    return {"data": queries.fetch_top_regions(store, limit=limit)}


@router.get("/quality")
def quality(
    limit: int = Query(20, ge=1, le=500),
    conn=Depends(get_connection),
):
    return {"data": queries.fetch_quality(conn, limit=limit)}


@router.get("/runs")
def runs(
    limit: int = Query(20, ge=1, le=500),
    conn=Depends(get_connection),
):
    return {"data": queries.fetch_runs(conn, limit=limit)}


@router.get("/metadata/summary")
def metadata_summary(conn=Depends(get_connection)):
    return {"data": queries.fetch_metadata_summary(conn)}


@router.get("/metadata/runs")
def metadata_runs(
    limit: int = Query(20, ge=1, le=500),
    conn=Depends(get_connection),
):
    return {"data": queries.fetch_runs(conn, limit=limit)}


@router.get("/metadata/quality")
def metadata_quality(
    limit: int = Query(20, ge=1, le=500),
    conn=Depends(get_connection),
):
    return {"data": queries.fetch_quality(conn, limit=limit)}


@router.get("/metadata/layers")
def metadata_layers(
    limit: int = Query(50, ge=1, le=500),
    run_id: str | None = None,
    conn=Depends(get_connection),
):
    if run_id:
        try:
            run_id = str(uuid.UUID(run_id))
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail="run_id invalido") from exc
    return {"data": queries.fetch_metadata_layers(conn, limit=limit, run_id=run_id)}


@router.get("/agent/suggestions")
def agent_suggestions():
    return {"data": SUGGESTED_QUESTIONS}


@router.post("/agent/query")
def agent_query(
    payload: AgentQuestion,
    store: GoldStore = Depends(get_gold_store),
    conn=Depends(get_connection),
):
    def execute(plan: dict):
        return queries.execute_plan(store, plan, conn=conn)

    current_settings = get_settings()
    agent = GoldAgent(executor=execute, settings=current_settings)
    try:
        answer = agent.ask(payload.question)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "answer": answer.answer,
        "plan": answer.plan,
        "mode": answer.mode,
        "warning": answer.warning,
        "columns": answer.columns,
        "rows": answer.rows,
        "sql_evidence": answer.sql_evidence,
        "model": answer.model,
        "prompt_version": answer.prompt_version,
        "latency_ms": answer.latency_ms,
        "tokens": answer.tokens,
        "stats": answer.stats,
    }


@router.get("/reports")
def list_reports(
    limit: int = Query(15, ge=1, le=100),
    store: GoldStore = Depends(get_gold_store),
    conn=Depends(get_connection),
):
    rows = fetch_daily_reports(conn, limit=limit)
    if not rows:
        meta = queries.fetch_metadata_summary(conn)
        generated = generate_daily_report(
            store,
            settings=get_settings(),
            run_id=(meta.get("run") or {}).get("run_id"),
            quality_metrics=meta.get("quality"),
            latest_run=meta.get("run"),
            persist=True,
        )
        return {"data": [generated]}
    return {"data": rows}


@router.post("/reports/generate")
def create_report(
    _role: str = Depends(require_write_role),
    store: GoldStore = Depends(get_gold_store),
    conn=Depends(get_connection),
):
    meta = queries.fetch_metadata_summary(conn)
    generated = generate_daily_report(
        store,
        settings=get_settings(),
        run_id=(meta.get("run") or {}).get("run_id"),
        quality_metrics=meta.get("quality"),
        latest_run=meta.get("run"),
        persist=True,
    )
    return {"data": generated}


@router.get("/reports/export")
def export_daily_report(
    format: str = Query("md", pattern="^(md|json|csv)$"),
    store: GoldStore = Depends(get_gold_store),
    conn=Depends(get_connection),
):
    rows = fetch_daily_reports(conn, limit=1)
    if rows:
        report = rows[0]
    else:
        meta = queries.fetch_metadata_summary(conn)
        report = generate_daily_report(
            store,
            settings=get_settings(),
            run_id=(meta.get("run") or {}).get("run_id"),
            quality_metrics=meta.get("quality"),
            latest_run=meta.get("run"),
            persist=False,
        )
    content, media_type = export_report(report, fmt=format)
    filename = f"sismowatch_report_{report.get('report_date', 'today')}.{format}"
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/anomalies")
def list_anomalies(
    limit: int = Query(30, ge=1, le=200),
    store: GoldStore = Depends(get_gold_store),
    conn=Depends(get_connection),
):
    rows = fetch_anomalies(conn, limit=limit)
    if not rows:
        rows = detect_and_store_anomalies(store, settings=get_settings(), persist=True)[:limit]
    return {"data": rows}


@router.post("/anomalies/detect")
def run_anomaly_detection(
    z_threshold: float = Query(2.0, ge=1.0, le=10.0),
    magnitude_threshold: float = Query(6.0, ge=3.0, le=10.0),
    _role: str = Depends(require_write_role),
    store: GoldStore = Depends(get_gold_store),
):
    detected = detect_and_store_anomalies(
        store,
        settings=get_settings(),
        z_threshold=z_threshold,
        magnitude_threshold=magnitude_threshold,
        persist=True,
    )
    return {"data": detected, "count": len(detected)}


@router.get("/ai-audit")
def list_ai_audit(
    limit: int = Query(30, ge=1, le=200),
    conn=Depends(get_connection),
):
    return {"data": fetch_ai_audit_logs(conn, limit=limit)}


app.include_router(router)
