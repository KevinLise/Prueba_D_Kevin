"""Generacion de informe narrativo diario con validacion Pydantic, anti-alucinacion y fallback determinista."""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
import io
import json
import logging
import re
import time
import urllib.request
import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator

from agent.audit import record_ai_audit
from config.settings import Settings, get_settings

LOGGER = logging.getLogger(__name__)

REPORT_PROMPT_VERSION = "report-v1.0"

REPORT_SYSTEM_PROMPT = """Eres un analista sismologico senior de SismoWatch.
Redacta un informe narrativo diario en espanol estrictamente basado en las estadisticas proporcionadas.
PROHIBIDO inventar cifras, magnitudes, conteos, fechas o regiones que no esten en los datos de entrada.
Devuelve UNICAMENTE un objeto JSON valido con esta estructura exacta:
{
  "title": "Titulo breve del informe diario",
  "executive_summary": "Resumen ejecutivo de 2 a 4 oraciones citando exactamente total_events y max_magnitude reales.",
  "key_findings": [
    "Hallazgo 1 con cifras exactas de los datos proporcionados",
    "Hallazgo 2 sobre regiones y actividad de las ultimas 24h",
    "Hallazgo 3 sobre calidad de datos y estado del pipeline"
  ],
  "reported_total_events": 0,
  "reported_max_magnitude": 0.0
}
"""


def sanitize_external_text(value: Any, max_len: int = 120) -> str:
    """Sanitiza textos externos (place, region) antes de enviarlos al prompt del modelo."""
    raw = str(value or "").strip()
    cleaned = re.sub(r"[\x00-\x1f\x7f`<>\\{}]", " ", raw)
    cleaned = re.sub(r"(?i)(ignore\s+previous|system\s*:|assistant\s*:|user\s*:)", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len] or "No especificado"


class NarrativeReportSchema(BaseModel):
    title: str = Field(min_length=5, max_length=220)
    executive_summary: str = Field(min_length=20, max_length=2000)
    key_findings: list[str] = Field(min_length=1, max_length=8)
    reported_total_events: int = Field(ge=0)
    reported_max_magnitude: float = Field(ge=-2.0, le=10.5)

    @field_validator("key_findings")
    @classmethod
    def _clean_findings(cls, items: list[str]) -> list[str]:
        cleaned = [str(item).strip() for item in items if str(item).strip()]
        if not cleaned:
            raise ValueError("key_findings no puede estar vacio")
        return cleaned[:8]


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
    return None


def compute_real_report_stats(
    store,
    quality_metrics: dict[str, Any] | None = None,
    latest_run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Calcula las estadisticas reales en Python a partir de Gold y ops."""
    summary_rows = store.read("summary")
    summary = summary_rows[0] if summary_rows else {}
    events = store.read("events")
    daily = store.read("daily_activity")
    top_regions = store.read("top_regions")

    total_events = int(summary.get("total_events") or len(events) or 0)
    avg_magnitude = round(float(summary.get("avg_magnitude") or 0.0), 2)
    max_magnitude = round(float(summary.get("max_magnitude") or 0.0), 2)

    parsed_events = []
    for row in events:
        dt = _parse_dt(row.get("event_time"))
        if dt is not None:
            parsed_events.append((dt, row))

    ref_now = max((dt for dt, _ in parsed_events), default=datetime.now(timezone.utc).replace(tzinfo=None))
    cutoff_24h = ref_now - timedelta(hours=24)
    events_24h = [ev for dt, ev in parsed_events if dt >= cutoff_24h]
    max_24h = max((float(ev.get("magnitude") or 0.0) for ev in events_24h), default=0.0)

    strongest_event = None
    if events:
        strongest_event = max(
            (ev for ev in events if ev.get("magnitude") is not None),
            key=lambda ev: float(ev["magnitude"]),
            default=None,
        )
        if max_magnitude == 0.0 and strongest_event is not None:
            max_magnitude = round(float(strongest_event["magnitude"]), 2)

    sorted_regions = sorted(
        top_regions,
        key=lambda r: int(r.get("event_count") or 0),
        reverse=True,
    )
    top_region_row = sorted_regions[0] if sorted_regions else {}

    sorted_daily = sorted(
        [d for d in daily if d.get("event_date") is not None],
        key=lambda d: str(d["event_date"]),
    )
    last_7d = sorted_daily[-7:] if sorted_daily else []
    events_7d = sum(int(d.get("event_count") or 0) for d in last_7d)

    q = quality_metrics or {}
    r = latest_run or {}

    return {
        "report_date": datetime.now(timezone.utc).date().isoformat(),
        "total_events": total_events,
        "events_last_24h": len(events_24h),
        "max_magnitude_24h": round(max_24h, 2),
        "events_last_7d": events_7d,
        "avg_magnitude": avg_magnitude,
        "max_magnitude": max_magnitude,
        "strongest_place": sanitize_external_text(
            strongest_event.get("place") if strongest_event else "Sin datos"
        ),
        "strongest_region": sanitize_external_text(
            strongest_event.get("region") if strongest_event else "Sin datos"
        ),
        "strongest_time": str(
            strongest_event.get("event_time") if strongest_event else summary.get("max_event_time") or "Sin datos"
        ),
        "top_region": sanitize_external_text(top_region_row.get("region") or "Sin datos"),
        "top_region_count": int(top_region_row.get("event_count") or 0),
        "quality_percentage": round(float(q.get("quality_percentage") or 0.0), 2)
        if q.get("quality_percentage") is not None
        else None,
        "invalid_records": int(q.get("invalid_records") or 0),
        "duplicate_records": int(q.get("duplicate_records") or 0),
        "bronze_records": int(q.get("bronze_records") or r.get("records_bronze") or 0),
        "silver_records": int(q.get("silver_records") or r.get("records_silver") or total_events),
        "pipeline_status": str(r.get("status") or "successful"),
        "last_pipeline_update": str(
            r.get("last_data_update") or r.get("completed_at") or summary.get("generated_at") or "Sin datos"
        ),
    }


def build_deterministic_report(stats: dict[str, Any]) -> NarrativeReportSchema:
    """Construye un informe determinista exacto cuando el LLM no responde o falla la validacion."""
    total = int(stats["total_events"])
    avg_mag = float(stats["avg_magnitude"])
    max_mag = float(stats["max_magnitude"])
    ev_24h = int(stats["events_last_24h"])
    ev_7d = int(stats["events_last_7d"])
    top_reg = stats["top_region"]
    top_reg_cnt = int(stats["top_region_count"])
    strongest_place = stats["strongest_place"]
    q_pct = stats.get("quality_percentage")
    invalid = int(stats.get("invalid_records") or 0)
    dups = int(stats.get("duplicate_records") or 0)

    if total == 0:
        return NarrativeReportSchema(
            title=f"Informe Sísmico Diario SismoWatch ({stats['report_date']})",
            executive_summary=(
                "No existen datos sísmicos suficientes en la capa Gold para el periodo consultado. "
                "Ejecute el DAG de Airflow para actualizar las métricas."
            ),
            key_findings=[
                "Total de eventos registrados en Gold: 0.",
                f"Estado reportado del pipeline: {stats['pipeline_status']}.",
            ],
            reported_total_events=0,
            reported_max_magnitude=0.0,
        )

    q_text = f"{q_pct:.2f}%" if q_pct is not None else "No disponible"
    summary_text = (
        f"En la ventana consolidada de 30 días se registraron {total} sismos validados en la capa Gold, "
        f"con una magnitud promedio de {avg_mag:.2f} y una magnitud máxima de {max_mag:.2f} "
        f"({strongest_place}). Durante las últimas 24 horas se contabilizaron {ev_24h} eventos "
        f"y en los últimos 7 días {ev_7d} eventos."
    )
    findings = [
        f"Evento de mayor magnitud: M{max_mag:.2f} ubicado en {strongest_place} ({stats['strongest_time']}).",
        f"Región con mayor actividad acumulada: {top_reg} con {top_reg_cnt} eventos registrados.",
        f"Actividad reciente: {ev_24h} sismos en las últimas 24 horas (máxima M{stats['max_magnitude_24h']:.2f}) y {ev_7d} en la última semana.",
        f"Calidad operativa del pipeline: {q_text} de calidad ({dups} duplicados filtrados y {invalid} registros inválidos rechazados).",
    ]
    return NarrativeReportSchema(
        title=f"Informe Sísmico Diario SismoWatch — {stats['report_date']}",
        executive_summary=summary_text,
        key_findings=findings,
        reported_total_events=total,
        reported_max_magnitude=max_mag,
    )


def validate_report_coherence(
    report: NarrativeReportSchema,
    stats: dict[str, Any],
) -> tuple[bool, str]:
    """Comprueba que las cifras del reporte generado por IA coincidan con las estadisticas reales."""
    expected_total = int(stats["total_events"])
    expected_max = float(stats["max_magnitude"])

    if report.reported_total_events != expected_total:
        return (
            False,
            f"Discrepancia en total_events: reporte={report.reported_total_events} vs real={expected_total}",
        )
    if abs(float(report.reported_max_magnitude) - expected_max) > 0.15:
        return (
            False,
            f"Discrepancia en max_magnitude: reporte={report.reported_max_magnitude} vs real={expected_max}",
        )

    # Verificar que el texto mencione el total real o la magnitud maxima real sin contradecirlos
    combined_text = (report.executive_summary + " " + " ".join(report.key_findings)).replace(",", "").replace(".", ".")
    total_str = str(expected_total)
    total_thousands_dot = f"{expected_total:,}".replace(",", ".")
    total_thousands_comma = f"{expected_total:,}"
    raw_combined = report.executive_summary + " " + " ".join(report.key_findings)
    if (
        total_str not in combined_text
        and total_thousands_dot not in raw_combined
        and total_thousands_comma not in raw_combined
    ):
        return (
            False,
            f"El texto narrativo no incluye el conteo real de eventos ({expected_total}).",
        )
    return True, "Validacion numerica exitosa frente a las estadisticas reales de Gold y ops."


def _extract_json_dict(text: str) -> dict[str, Any]:
    content = (text or "").strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z]*\s*", "", content)
        content = re.sub(r"\s*```$", "", content)
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            return json.loads(content[start : end + 1])
        raise


def _call_ollama_report(
    stats: dict[str, Any],
    settings: Settings,
) -> tuple[NarrativeReportSchema, str, float, int | None, int | None, int | None]:
    base_url = (settings.ollama_base_url or "").strip().rstrip("/")
    model = settings.ollama_model or settings.agent_llm_model or "qwen2.5:3b"
    timeout = max(5, int(settings.ollama_timeout or 45))

    user_prompt = (
        "Estadisticas reales verificadas (usa exclusivamente estos numeros):\n"
        f"{json.dumps(stats, ensure_ascii=False, indent=2)}\n\n"
        f"IMPORTANTE: reported_total_events debe ser {stats['total_events']}, "
        f"reported_max_magnitude debe ser {stats['max_magnitude']}, "
        f"y executive_summary debe mencionar explicitamente {stats['total_events']} sismos y magnitud maxima {stats['max_magnitude']}.\n"
        "Genera el JSON del informe diario."
    )

    t0 = time.perf_counter()
    if base_url:
        url = f"{base_url}/api/chat"
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": REPORT_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "format": "json",
            "stream": False,
            "options": {"temperature": 0, "num_predict": 320},
        }
        req = urllib.request.Request(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(body).encode("utf-8"),
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        latency_ms = (time.perf_counter() - t0) * 1000.0
        content = (raw.get("message") or {}).get("content") or ""
        p_tokens = raw.get("prompt_eval_count")
        c_tokens = raw.get("eval_count")
        t_tokens = (int(p_tokens) + int(c_tokens)) if (p_tokens is not None and c_tokens is not None) else None
        parsed = NarrativeReportSchema.model_validate(_extract_json_dict(content))
        return parsed, model, latency_ms, p_tokens, c_tokens, t_tokens

    if settings.agent_llm_api_key:
        from openai import OpenAI

        client = OpenAI(
            api_key=settings.agent_llm_api_key,
            base_url=settings.agent_llm_base_url or None,
            timeout=timeout,
        )
        completion = client.chat.completions.create(
            model=settings.agent_llm_model,
            temperature=0,
            messages=[
                {"role": "system", "content": REPORT_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        latency_ms = (time.perf_counter() - t0) * 1000.0
        content = completion.choices[0].message.content or ""
        usage = getattr(completion, "usage", None)
        p_tokens = getattr(usage, "prompt_tokens", None) if usage else None
        c_tokens = getattr(usage, "completion_tokens", None) if usage else None
        t_tokens = getattr(usage, "total_tokens", None) if usage else None
        parsed = NarrativeReportSchema.model_validate(_extract_json_dict(content))
        return parsed, settings.agent_llm_model, latency_ms, p_tokens, c_tokens, t_tokens

    raise RuntimeError("No hay proveedor Ollama ni LLM configurado")


def persist_daily_report(report_row: dict[str, Any], settings: Settings | None = None) -> None:
    runtime = settings or get_settings()
    if not runtime.sismowatch_db_user or not runtime.sismowatch_db_password:
        return
    from psycopg2.extras import Json
    from db.connection import connection

    run_uuid = None
    if report_row.get("run_id"):
        try:
            run_uuid = uuid.UUID(str(report_row["run_id"]))
        except (ValueError, TypeError):
            run_uuid = None

    with connection(runtime, readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.daily_reports (
                    report_date, run_id, status, generation_mode, model_used,
                    prompt_version, title, executive_summary, key_findings,
                    stats_snapshot, validation_passed, validation_notes,
                    generated_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (report_date) DO UPDATE SET
                    run_id = COALESCE(EXCLUDED.run_id, ops.daily_reports.run_id),
                    status = EXCLUDED.status,
                    generation_mode = EXCLUDED.generation_mode,
                    model_used = EXCLUDED.model_used,
                    prompt_version = EXCLUDED.prompt_version,
                    title = EXCLUDED.title,
                    executive_summary = EXCLUDED.executive_summary,
                    key_findings = EXCLUDED.key_findings,
                    stats_snapshot = EXCLUDED.stats_snapshot,
                    validation_passed = EXCLUDED.validation_passed,
                    validation_notes = EXCLUDED.validation_notes,
                    updated_at = now()
                """,
                (
                    report_row["report_date"],
                    run_uuid,
                    report_row["status"],
                    report_row["generation_mode"],
                    report_row.get("model_used"),
                    report_row["prompt_version"],
                    report_row["title"],
                    report_row["executive_summary"],
                    Json(report_row["key_findings"]),
                    Json(report_row["stats_snapshot"]),
                    bool(report_row["validation_passed"]),
                    report_row.get("validation_notes"),
                ),
            )


def generate_daily_report(
    store,
    settings: Settings | None = None,
    run_id: str | None = None,
    quality_metrics: dict[str, Any] | None = None,
    latest_run: dict[str, Any] | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """Calcula estadisticas reales, genera el reporte con Ollama/LLM, valida coherencia y aplica fallback determinista."""
    runtime = settings or get_settings()
    stats = compute_real_report_stats(store, quality_metrics=quality_metrics, latest_run=latest_run)

    mode = "deterministic"
    status = "completed"
    model_used = None
    validation_passed = True
    validation_notes = "Generado con plantilla determinista verificada."
    latency_ms = None
    p_tokens = None
    c_tokens = None
    t_tokens = None

    if runtime.ollama_base_url or runtime.agent_llm_api_key:
        try:
            candidate, model_used, latency_ms, p_tokens, c_tokens, t_tokens = _call_ollama_report(
                stats, runtime
            )
            ok, notes = validate_report_coherence(candidate, stats)
            if ok:
                report_schema = candidate
                mode = "ollama" if runtime.ollama_base_url else "llm"
                status = "completed"
                validation_passed = True
                validation_notes = notes
                record_ai_audit(
                    operation="daily_report",
                    prompt_version=REPORT_PROMPT_VERSION,
                    mode=mode,
                    status="success",
                    model=model_used,
                    latency_ms=latency_ms,
                    prompt_tokens=p_tokens,
                    completion_tokens=c_tokens,
                    total_tokens=t_tokens,
                    fallback_used=False,
                    settings=runtime,
                )
            else:
                report_schema = build_deterministic_report(stats)
                mode = "deterministic"
                status = "fallback"
                validation_passed = False
                validation_notes = f"Fallback determinista activado: {notes}"
                record_ai_audit(
                    operation="daily_report",
                    prompt_version=REPORT_PROMPT_VERSION,
                    mode="deterministic",
                    status="fallback",
                    model=model_used,
                    latency_ms=latency_ms,
                    prompt_tokens=p_tokens,
                    completion_tokens=c_tokens,
                    total_tokens=t_tokens,
                    fallback_used=True,
                    error_message=notes,
                    settings=runtime,
                )
        except Exception as exc:
            LOGGER.warning("Fallo generacion LLM de informe diario; usando fallback determinista: %s", exc)
            report_schema = build_deterministic_report(stats)
            mode = "deterministic"
            status = "fallback"
            validation_passed = True
            validation_notes = f"Fallback determinista por indisponibilidad del modelo: {exc}"
            record_ai_audit(
                operation="daily_report",
                prompt_version=REPORT_PROMPT_VERSION,
                mode="deterministic",
                status="fallback",
                model=runtime.ollama_model if runtime.ollama_base_url else runtime.agent_llm_model,
                fallback_used=True,
                error_message=str(exc),
                settings=runtime,
            )
    else:
        report_schema = build_deterministic_report(stats)
        record_ai_audit(
            operation="daily_report",
            prompt_version=REPORT_PROMPT_VERSION,
            mode="deterministic",
            status="success",
            fallback_used=False,
            settings=runtime,
        )

    result = {
        "report_date": stats["report_date"],
        "run_id": run_id,
        "status": status,
        "generation_mode": mode,
        "model_used": model_used,
        "prompt_version": REPORT_PROMPT_VERSION,
        "title": report_schema.title,
        "executive_summary": report_schema.executive_summary,
        "key_findings": report_schema.key_findings,
        "stats_snapshot": stats,
        "validation_passed": validation_passed,
        "validation_notes": validation_notes,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if persist:
        try:
            persist_daily_report(result, settings=runtime)
        except Exception as exc:
            LOGGER.warning("No se pudo guardar el informe diario en ops.daily_reports: %s", exc)
    return result


def fetch_daily_reports(conn, limit: int = 15) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor

    safe_limit = max(1, min(int(limit or 15), 100))
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT report_id, report_date::text AS report_date, run_id::text AS run_id,
                   status, generation_mode, model_used, prompt_version, title,
                   executive_summary, key_findings, stats_snapshot,
                   validation_passed, validation_notes, generated_at, updated_at
            FROM ops.daily_reports
            ORDER BY report_date DESC
            LIMIT %s
            """,
            (safe_limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def export_report(report: dict[str, Any], fmt: str = "md") -> tuple[str, str]:
    """Exporta un informe diario en formato markdown (md), json o csv."""
    fmt_clean = (fmt or "md").strip().lower()
    if fmt_clean == "json":
        return json.dumps(report, ensure_ascii=False, indent=2, default=str), "application/json"
    if fmt_clean == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "report_date",
                "title",
                "generation_mode",
                "model_used",
                "prompt_version",
                "status",
                "total_events",
                "events_last_24h",
                "max_magnitude",
                "avg_magnitude",
                "top_region",
                "quality_percentage",
                "executive_summary",
            ]
        )
        stats = report.get("stats_snapshot") or {}
        writer.writerow(
            [
                report.get("report_date"),
                report.get("title"),
                report.get("generation_mode"),
                report.get("model_used") or "deterministic",
                report.get("prompt_version"),
                report.get("status"),
                stats.get("total_events"),
                stats.get("events_last_24h"),
                stats.get("max_magnitude"),
                stats.get("avg_magnitude"),
                stats.get("top_region"),
                stats.get("quality_percentage"),
                report.get("executive_summary"),
            ]
        )
        return buf.getvalue(), "text/csv"

    findings_md = "\n".join(f"- {item}" for item in (report.get("key_findings") or []))
    stats = report.get("stats_snapshot") or {}
    md = (
        f"# {report.get('title', 'Informe Sísmico Diario')}\n\n"
        f"- **Fecha:** {report.get('report_date')}\n"
        f"- **Modo de generación:** {report.get('generation_mode')} "
        f"({report.get('model_used') or 'determinista'})\n"
        f"- **Versión de prompt:** {report.get('prompt_version')}\n"
        f"- **Estado de validación:** {report.get('validation_notes')}\n\n"
        f"## Resumen Ejecutivo\n\n{report.get('executive_summary')}\n\n"
        f"## Hallazgos Clave\n\n{findings_md}\n\n"
        f"## Estadísticas Verificadas (Gold + ops)\n\n"
        f"- **Total de eventos (30d):** {stats.get('total_events')}\n"
        f"- **Eventos últimas 24h:** {stats.get('events_last_24h')}\n"
        f"- **Eventos últimos 7d:** {stats.get('events_last_7d')}\n"
        f"- **Magnitud máxima:** {stats.get('max_magnitude')} ({stats.get('strongest_place')})\n"
        f"- **Magnitud promedio:** {stats.get('avg_magnitude')}\n"
        f"- **Región más activa:** {stats.get('top_region')} ({stats.get('top_region_count')} sismos)\n"
        f"- **Calidad de datos:** {stats.get('quality_percentage')}%\n"
    )
    return md, "text/markdown"
