"""Agente conversacional de solo lectura sobre MinIO Gold y PostgreSQL ops con anti-alucinacion."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import re
import time
from typing import Any, Callable
import unicodedata
import urllib.request

from agent.audit import record_ai_audit
from agent.plan_guard import validate_plan
from config.settings import Settings

LOGGER = logging.getLogger(__name__)

AGENT_PROMPT_VERSION = "agent-v2.0"

SCHEMA = """
events(event_id, event_time, event_date, magnitude, magnitude_class, place, region, latitude, longitude, depth_km, tsunami, url)
daily_activity(event_date, event_count, avg_magnitude, max_magnitude, avg_depth_km)
magnitude_distribution(magnitude_class, magnitude_min, magnitude_max, event_count, avg_magnitude)
top_places(place, event_count, avg_magnitude, max_magnitude)
top_regions(region, event_count, avg_magnitude, max_magnitude)
summary(total_events, avg_magnitude, max_magnitude, min_event_time, max_event_time, last_ingested_at, generated_at)
pipeline_runs(run_id, run_type, status, current_stage, started_at, completed_at, duration_seconds, records_received, records_processed, last_data_update, error_message)
quality_metrics(run_id, bronze_records, valid_records, invalid_records, duplicate_records, silver_records, quality_percentage, freshness_minutes, recorded_at)
anomalies(anomaly_id, window_start, window_end, scope, anomaly_type, severity, z_score, observed_count, max_magnitude, explanation)
""".strip()

SYSTEM_PROMPT = """Eres un analista de datos sismicos de SismoWatch. Genera un plan de consulta de solo lectura.
No generes SQL libre, codigo ni comandos. Devuelve UNICAMENTE un JSON con esta forma:
{"dataset":"events|daily_activity|magnitude_distribution|top_places|top_regions|summary|pipeline_runs|quality_metrics|anomalies",
 "filters":{"min_magnitude":4.0,"region":"...","place":"...","days":30,"hours":24},
 "sort":{"field":"magnitude","order":"desc"},
 "limit":20,
 "explicacion":"texto breve en espanol"}
Reglas:
- Usa solo datasets y campos del esquema indicado.
- filters solo se permite en events (min_magnitude, region, place, days, hours) y daily_activity (days). En los demas datasets usa "filters":{}.
- sort.field debe existir en los campos ordenables del dataset; order es asc o desc. En summary no uses sort.
- limit debe estar entre 1 y 500.
- No inventes datos ni campos.
"""

NARRATIVE_SYSTEM_PROMPT = """Eres el asistente sismologico de SismoWatch.
Responde en espanol claro y conciso (2 a 4 oraciones) la pregunta del usuario usando EXCLUSIVAMENTE los datos calculados por el sistema.
REGLA ANTI-ALUCINACION:
- NO inventes conteos, magnitudes, fechas, regiones ni porcentajes.
- Cita exactamente las cifras entregadas en 'hechos_calculados'.
- Si 'filas_devueltas' es 0, indica claramente que no hay datos suficientes para responder.
"""

SUGGESTED_QUESTIONS = [
    "¿Cuántos sismos hubo en las últimas 24 horas?",
    "¿Cuál fue el sismo de mayor magnitud en los últimos 30 días?",
    "¿Qué regiones registraron mayor actividad?",
    "¿Cómo cambió la actividad sísmica durante la última semana?",
    "¿Cuándo se ejecutó por última vez el pipeline?",
    "¿Cuántos registros fueron rechazados por los controles de calidad?",
]

QueryExecutor = Callable[[dict], tuple[list[str], list[dict]]]


@dataclass
class AgentAnswer:
    answer: str
    plan: dict
    columns: list[str]
    rows: list[dict]
    mode: str
    warning: str | None = None
    sql_evidence: str | None = None
    model: str | None = None
    prompt_version: str = AGENT_PROMPT_VERSION
    latency_ms: float | None = None
    tokens: dict[str, int | None] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)


def _plain(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii").lower()


def _parse_json(text: str) -> dict:
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


def _build_sql_evidence(plan: dict) -> str:
    """Construye la evidencia de consulta parametrizada/estructurada ejecutada en modo solo lectura."""
    dataset = plan.get("dataset")
    limit = int(plan.get("limit", 20))
    filters = plan.get("filters") or {}
    sort = plan.get("sort") or {}

    if dataset == "pipeline_runs":
        return (
            "SELECT run_id::text, run_type, status, current_stage, started_at, completed_at, "
            "duration_seconds, records_received, records_processed, last_data_update, error_message "
            f"FROM ops.runs ORDER BY started_at DESC LIMIT {limit}; -- [READ-ONLY]"
        )
    if dataset == "quality_metrics":
        return (
            "SELECT run_id::text, bronze_records, valid_records, invalid_records, duplicate_records, "
            "silver_records, quality_percentage, freshness_minutes, recorded_at "
            f"FROM ops.quality_metrics ORDER BY recorded_at DESC LIMIT {limit}; -- [READ-ONLY]"
        )
    if dataset == "anomalies":
        return (
            "SELECT anomaly_id, window_start, window_end, scope, anomaly_type, severity, "
            "z_score, observed_count, max_magnitude, explanation "
            f"FROM ops.anomalies ORDER BY detected_at DESC LIMIT {limit}; -- [READ-ONLY]"
        )

    where_clauses = []
    if "min_magnitude" in filters:
        where_clauses.append(f"magnitude >= {float(filters['min_magnitude'])}")
    if "hours" in filters:
        where_clauses.append(f"event_time >= (MAX(event_time) - INTERVAL '{int(filters['hours'])} hours')")
    if "days" in filters:
        where_clauses.append(f"event_date >= (CURRENT_DATE - INTERVAL '{int(filters['days'])} days')")
    if "region" in filters:
        where_clauses.append(f"LOWER(region) LIKE LOWER('%{filters['region']}%')")
    if "place" in filters:
        where_clauses.append(f"LOWER(place) LIKE LOWER('%{filters['place']}%')")

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    order_sql = f" ORDER BY {sort['field']} {sort.get('order', 'desc').upper()}" if sort.get("field") else ""
    return f"SELECT * FROM gold.{dataset}{where_sql}{order_sql} LIMIT {limit}; -- [PYARROW READ-ONLY]"


def compute_deterministic_summary(question: str, plan: dict, rows: list[dict]) -> tuple[str, dict[str, Any]]:
    """Calcula cifras exactas con Python (nunca con el LLM) y redacta una respuesta base verificada."""
    dataset = plan.get("dataset")
    filters = plan.get("filters") or {}

    if not rows:
        return (
            "No existen datos suficientes en el catálogo para responder la consulta con los criterios solicitados.",
            {"row_count": 0, "dataset": dataset},
        )

    if dataset == "summary":
        row = rows[0]
        total = int(row.get("total_events") or 0)
        avg_m = float(row.get("avg_magnitude") or 0.0)
        max_m = float(row.get("max_magnitude") or 0.0)
        max_t = row.get("max_event_time") or "N/D"
        stats = {
            "row_count": 1,
            "total_events": total,
            "avg_magnitude": round(avg_m, 2),
            "max_magnitude": round(max_m, 2),
            "max_event_time": str(max_t),
        }
        text = (
            f"El catálogo Gold registra un total de {total} sismos validados en los últimos 30 días, "
            f"con una magnitud promedio de {avg_m:.2f}, magnitud máxima de {max_m:.2f} "
            f"y último evento registrado el {max_t} UTC."
        )
        return text, stats

    if dataset == "pipeline_runs":
        latest = rows[0]
        completed = next((r for r in rows if r.get("status") in {"successful", "completed"}), latest)
        status = latest.get("status") or "desconocido"
        stage = latest.get("current_stage") or "N/D"
        started = latest.get("started_at") or "N/D"
        last_upd = completed.get("last_data_update") or completed.get("completed_at") or started
        rec_proc = int(completed.get("records_processed") or completed.get("records_silver") or 0)
        dur = completed.get("duration_seconds")
        dur_str = f"{float(dur):.1f} s" if dur is not None else "en curso"
        stats = {
            "row_count": len(rows),
            "latest_run_id": str(latest.get("run_id")),
            "latest_status": str(status),
            "current_stage": str(stage),
            "started_at": str(started),
            "last_completed_update": str(last_upd),
            "records_processed": rec_proc,
        }
        text = (
            f"La última ejecución del pipeline inició el {started} (estado actual: {status}, etapa: {stage}). "
            f"La última actualización completada fue el {last_upd} con {rec_proc} registros procesados en Silver/Gold "
            f"(duración: {dur_str})."
        )
        return text, stats

    if dataset == "quality_metrics":
        latest = rows[0]
        bronze = int(latest.get("bronze_records") or 0)
        valid = int(latest.get("valid_records") or 0)
        invalid = int(latest.get("invalid_records") or 0)
        dups = int(latest.get("duplicate_records") or 0)
        silver = int(latest.get("silver_records") or 0)
        q_pct = float(latest.get("quality_percentage") or 0.0)
        total_rejected = invalid + dups
        stats = {
            "row_count": len(rows),
            "bronze_records": bronze,
            "valid_records": valid,
            "invalid_records": invalid,
            "duplicate_records": dups,
            "total_rejected_or_dedup": total_rejected,
            "silver_records": silver,
            "quality_percentage": round(q_pct, 2),
        }
        text = (
            f"En el último control de calidad sobre {bronze} registros Bronze, se rechazaron {invalid} registros inválidos "
            f"y se filtraron {dups} registros duplicados ({total_rejected} descartados en total), "
            f"conservando {silver} registros válidos únicos en Silver con {q_pct:.2f}% de calidad."
        )
        return text, stats

    if dataset == "top_regions":
        top = rows[0]
        top3 = [f"{r.get('region')} ({int(r.get('event_count') or 0)} sismos, máx M{float(r.get('max_magnitude') or 0.0):.1f})" for r in rows[:3]]
        stats = {
            "row_count": len(rows),
            "top_region": str(top.get("region")),
            "top_region_count": int(top.get("event_count") or 0),
            "top_3": top3,
        }
        text = (
            f"Las regiones con mayor actividad sísmica son encabezadas por {', '.join(top3)}. "
            f"Se listan las {len(rows)} regiones más activas registradas en Gold."
        )
        return text, stats

    if dataset == "top_places":
        top3 = [f"{r.get('place')} ({int(r.get('event_count') or 0)} sismos)" for r in rows[:3]]
        stats = {
            "row_count": len(rows),
            "top_place": str(rows[0].get("place")),
            "top_place_count": int(rows[0].get("event_count") or 0),
        }
        text = f"Los lugares específicos con mayor concentración de eventos son: {', '.join(top3)}."
        return text, stats

    if dataset == "daily_activity":
        counts = [int(r.get("event_count") or 0) for r in rows]
        total_period = sum(counts)
        peak_row = max(rows, key=lambda r: int(r.get("event_count") or 0))
        min_row = min(rows, key=lambda r: int(r.get("event_count") or 0))
        first_cnt = counts[0]
        last_cnt = counts[-1]
        delta = last_cnt - first_cnt
        trend = "aumentó" if delta > 0 else ("disminuyó" if delta < 0 else "se mantuvo estable")
        stats = {
            "row_count": len(rows),
            "days_analyzed": len(rows),
            "total_events_period": total_period,
            "first_day": str(rows[0].get("event_date")),
            "first_day_count": first_cnt,
            "last_day": str(rows[-1].get("event_date")),
            "last_day_count": last_cnt,
            "peak_date": str(peak_row.get("event_date")),
            "peak_count": int(peak_row.get("event_count") or 0),
        }
        text = (
            f"Durante los {len(rows)} días analizados se registraron {total_period} sismos en total. "
            f"La actividad diaria {trend} desde {first_cnt} sismos ({rows[0].get('event_date')}) hasta "
            f"{last_cnt} sismos ({rows[-1].get('event_date')}), con pico máximo el {peak_row.get('event_date')} "
            f"({peak_row.get('event_count')} eventos) y mínimo el {min_row.get('event_date')} ({min_row.get('event_count')} eventos)."
        )
        return text, stats

    if dataset == "anomalies":
        top = rows[0]
        stats = {
            "row_count": len(rows),
            "top_severity": str(top.get("severity")),
            "top_type": str(top.get("anomaly_type")),
        }
        text = (
            f"Se encontraron {len(rows)} anomalías estadísticas registradas en el catálogo. "
            f"La más relevante es de tipo '{top.get('anomaly_type')}' (severidad {top.get('severity')}): "
            f"{top.get('explanation')}"
        )
        return text, stats

    if dataset == "events":
        mags = [float(r["magnitude"]) for r in rows if r.get("magnitude") is not None]
        max_row = max(
            (r for r in rows if r.get("magnitude") is not None),
            key=lambda r: float(r["magnitude"]),
            default=rows[0],
        )
        max_mag = float(max_row.get("magnitude") or 0.0)
        avg_mag = sum(mags) / len(mags) if mags else 0.0
        place = max_row.get("place") or max_row.get("region") or "ubicación no especificada"
        ev_time = max_row.get("event_time") or max_row.get("event_date") or "N/D"
        depth = max_row.get("depth_km")
        depth_str = f", profundidad {float(depth):.1f} km" if depth is not None else ""
        total_matched = int(max_row.get("_total_matched") or len(rows))
        stats = {
            "row_count": len(rows),
            "total_matched": total_matched,
            "max_magnitude": round(max_mag, 2),
            "avg_magnitude": round(avg_mag, 2),
            "strongest_place": str(place),
            "strongest_time": str(ev_time),
        }
        if filters.get("hours") == 24 or "24" in _plain(question):
            text = (
                f"En las últimas 24 horas se registraron {total_matched} sismos en el catálogo Gold "
                f"(magnitud promedio {avg_mag:.2f}). El evento de mayor magnitud alcanzó M{max_mag:.2f} "
                f"en {place} ({ev_time} UTC{depth_str})."
            )
        else:
            text = (
                f"El sismo de mayor magnitud registrado en la consulta alcanzó M{max_mag:.2f} "
                f"en {place} el {ev_time} UTC{depth_str} "
                f"(sobre {total_matched} eventos evaluados, magnitud promedio {avg_mag:.2f})."
            )
        return text, stats

    return f"Consulta ejecutada sobre {dataset}: {len(rows)} registros obtenidos.", {"row_count": len(rows)}


def _build_deterministic_rule_plan(question: str) -> tuple[dict | None, str]:
    """Mapea preguntas frecuentes del usuario a planes deterministas seguros antes de invocar al LLM."""
    text = _plain(question)

    # 1. Pipeline / ultima ejecucion
    if any(w in text for w in ("pipeline", "ejecut", "airflow", "dag", "corrida", "actualiz")):
        return (
            {
                "dataset": "pipeline_runs",
                "filters": {},
                "sort": {"field": "started_at", "order": "desc"},
                "limit": 10,
            },
            "Estado e historial de las últimas ejecuciones del pipeline.",
        )

    # 2. Controles de calidad / rechazados / duplicados / invalidos
    if any(w in text for w in ("rechazad", "calidad", "invalid", "duplicad", "descartad", "control")):
        return (
            {
                "dataset": "quality_metrics",
                "filters": {},
                "sort": {"field": "recorded_at", "order": "desc"},
                "limit": 10,
            },
            "Métricas de calidad de datos y registros rechazados o duplicados.",
        )

    # 3. Anomalias
    if "anomali" in text or "inusual" in text or "z-score" in text:
        return (
            {
                "dataset": "anomalies",
                "filters": {},
                "sort": {"field": "detected_at", "order": "desc"},
                "limit": 15,
            },
            "Anomalías estadísticas detectadas en la actividad sísmica.",
        )

    # 4. Sismos en las ultimas 24 horas / hoy
    if "24" in text or "ultimas 24" in text or "hoy" in text:
        return (
            {
                "dataset": "events",
                "filters": {"hours": 24},
                "sort": {"field": "event_time", "order": "desc"},
                "limit": 200,
            },
            "Sismos registrados durante las últimas 24 horas.",
        )

    # 5. Cambio durante la ultima semana / 7 dias
    if "semana" in text or "7 dias" in text or ("cambio" in text and "actividad" in text):
        return (
            {
                "dataset": "daily_activity",
                "filters": {"days": 7},
                "sort": {"field": "event_date", "order": "asc"},
                "limit": 7,
            },
            "Evolución de la actividad sísmica diaria durante la última semana (7 días).",
        )

    # 6. Regiones con mayor actividad
    if any(w in text for w in ("region", "pais", "zona")):
        return (
            {
                "dataset": "top_regions",
                "filters": {},
                "sort": {"field": "event_count", "order": "desc"},
                "limit": 15,
            },
            "Regiones con mayor cantidad de eventos.",
        )

    # 7. Sismos de mayor magnitud
    if any(w in text for w in ("mayor magnitud", "mas fuerte", "maxima magnitud")) or (
        "magnitud" in text and any(w in text for w in ("mayor", "maxim", "cual", "fuerte"))
    ):
        filters = {"days": 30} if "30" in text else {}
        return (
            {
                "dataset": "events",
                "filters": filters,
                "sort": {"field": "magnitude", "order": "desc"},
                "limit": 10,
            },
            "Sismos con mayor magnitud registrados.",
        )

    return None, ""


class GoldAgent:
    def __init__(self, executor: QueryExecutor, settings: Settings) -> None:
        self.executor = executor
        self.settings = settings

    @property
    def _has_llm_configured(self) -> bool:
        return bool(self.settings.agent_llm_api_key or self.settings.ollama_base_url)

    def ask(self, question: str) -> AgentAnswer:
        question = (question or "").strip()
        if not question:
            raise ValueError("La pregunta esta vacia")

        if self._has_llm_configured:
            try:
                return self._ask_llm(question)
            except Exception as exc:
                LOGGER.exception("Fallo el agente LLM; se usa el modo local determinista")
                answer = self._ask_rules(question)
                answer.warning = f"El modelo IA no estuvo disponible; se respondió con el motor determinista local. Detalle: {exc}"
                record_ai_audit(
                    operation="agent_query",
                    question=question,
                    prompt_version=AGENT_PROMPT_VERSION,
                    mode="reglas",
                    status="fallback",
                    model=self.settings.ollama_model if self.settings.ollama_base_url else self.settings.agent_llm_model,
                    fallback_used=True,
                    error_message=str(exc),
                    settings=self.settings,
                )
                return answer

        answer = self._ask_rules(question)
        record_ai_audit(
            operation="agent_query",
            question=question,
            prompt_version=AGENT_PROMPT_VERSION,
            mode="reglas",
            status="success",
            fallback_used=False,
            settings=self.settings,
        )
        return answer

    def _execute(self, plan: dict) -> tuple[dict, list[str], list[dict]]:
        safe_plan = validate_plan(plan)
        columns, rows = self.executor(safe_plan)
        return safe_plan, columns, rows

    def _call_chat_model(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = False,
    ) -> tuple[str, str, float, dict[str, int | None]]:
        """Invoca Ollama oficial (/api/chat) o cliente compatible con OpenAI y retorna metricas reales."""
        timeout = max(5, int(getattr(self.settings, "ollama_timeout", 45) or 45))
        ollama_base = (getattr(self.settings, "ollama_base_url", "") or "").strip().rstrip("/")
        t0 = time.perf_counter()

        if ollama_base:
            model = getattr(self.settings, "ollama_model", "") or self.settings.agent_llm_model or "qwen2.5:3b"
            body: dict[str, Any] = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "stream": False,
                "options": {"temperature": 0, "num_predict": 140},
            }
            if json_mode:
                body["format"] = "json"
            req = urllib.request.Request(
                f"{ollama_base}/api/chat",
                headers={"Content-Type": "application/json"},
                data=json.dumps(body).encode("utf-8"),
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
            latency_ms = (time.perf_counter() - t0) * 1000.0
            content = (raw.get("message") or {}).get("content") or ""
            p_tok = raw.get("prompt_eval_count")
            c_tok = raw.get("eval_count")
            t_tok = (int(p_tok) + int(c_tok)) if (p_tok is not None and c_tok is not None) else None
            return (
                content,
                model,
                latency_ms,
                {"prompt_tokens": p_tok, "completion_tokens": c_tok, "total_tokens": t_tok},
            )

        from openai import OpenAI

        model = self.settings.agent_llm_model or "gpt-4o-mini"
        client = OpenAI(
            api_key=self.settings.agent_llm_api_key,
            base_url=self.settings.agent_llm_base_url or None,
            timeout=timeout,
        )
        completion = client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        latency_ms = (time.perf_counter() - t0) * 1000.0
        content = completion.choices[0].message.content or ""
        usage = getattr(completion, "usage", None)
        p_tok = getattr(usage, "prompt_tokens", None) if usage else None
        c_tok = getattr(usage, "completion_tokens", None) if usage else None
        t_tok = getattr(usage, "total_tokens", None) if usage else None
        return (
            content,
            model,
            latency_ms,
            {"prompt_tokens": p_tok, "completion_tokens": c_tok, "total_tokens": t_tok},
        )

    def _ask_llm(self, question: str) -> AgentAnswer:
        # Paso 1: Para preguntas frecuentes, usar plan determinista autorizado; si no, pedir plan JSON al modelo.
        preset_plan, preset_explanation = _build_deterministic_rule_plan(question)
        total_latency = 0.0
        tokens_accum: dict[str, int | None] = {
            "prompt_tokens": None,
            "completion_tokens": None,
            "total_tokens": None,
        }
        model_used = getattr(self.settings, "ollama_model", "") or self.settings.agent_llm_model

        if preset_plan is not None:
            safe_plan, columns, rows = self._execute(preset_plan)
            plan_explanation = preset_explanation
        else:
            user_prompt = (
                f"Datasets disponibles:\n{SCHEMA}\n\n"
                f"Pregunta del usuario: {question}\n\n"
                "Devuelve solo el objeto JSON."
            )
            content, model_used, lat1, tok1 = self._call_chat_model(
                SYSTEM_PROMPT, user_prompt, json_mode=True
            )
            total_latency += lat1
            tokens_accum = tok1
            payload = _parse_json(content)
            plan_explanation = str(
                payload.get("explicacion")
                or payload.get("explanation")
                or "Consulta generada por el modelo."
            )
            safe_plan, columns, rows = self._execute(payload)

        # Paso 2: Calcular estadisticas reales con Python (regla anti-alucinacion)
        deterministic_text, stats = compute_deterministic_summary(question, safe_plan, rows)

        # Paso 3: Redactar respuesta con el LLM anclada a los datos reales y validar que no contradiga las cifras
        final_answer = deterministic_text
        try:
            narrative_prompt = (
                f"Pregunta: {question}\n"
                f"filas_devueltas: {len(rows)}\n"
                f"hechos_calculados: {json.dumps(stats, ensure_ascii=False, default=str)}\n"
                f"resumen_determinista_verificado: {deterministic_text}\n\n"
                "Redacta una respuesta breve (2 a 3 oraciones) en espanol manteniendo exactamente las cifras de resumen_determinista_verificado."
            )
            llm_text, model_used, lat2, tok2 = self._call_chat_model(
                NARRATIVE_SYSTEM_PROMPT, narrative_prompt, json_mode=False
            )
            total_latency += lat2
            for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
                if tok2.get(k) is not None:
                    tokens_accum[k] = int(tokens_accum.get(k) or 0) + int(tok2[k])

            cleaned_llm = (llm_text or "").strip()
            # Verificacion anti-alucinacion: comprobar que la cifra principal calculada por Python aparezca en la respuesta
            primary_numbers = []
            for key in ("total_matched", "total_events", "max_magnitude", "records_processed", "silver_records", "top_region_count", "total_events_period"):
                if key in stats and stats[key] is not None:
                    primary_numbers.append(str(stats[key]))
                    break

            normalized_llm = cleaned_llm.replace(",", ".")
            if cleaned_llm and (
                not primary_numbers
                or any(num in normalized_llm or num in cleaned_llm.replace(".", "").replace(",", "") for num in primary_numbers)
            ):
                final_answer = cleaned_llm
        except Exception as exc:
            if preset_plan is not None:
                raise
            LOGGER.debug("Se conserva el resumen determinista verificado: %s", exc)

        clean_rows = [{k: v for k, v in r.items() if k != "_total_matched"} for r in rows]
        mode_name = "ollama" if getattr(self.settings, "ollama_base_url", "") else "llm"
        record_ai_audit(
            operation="agent_query",
            question=question,
            prompt_version=AGENT_PROMPT_VERSION,
            mode=mode_name,
            status="success",
            model=model_used,
            latency_ms=total_latency,
            prompt_tokens=tokens_accum.get("prompt_tokens"),
            completion_tokens=tokens_accum.get("completion_tokens"),
            total_tokens=tokens_accum.get("total_tokens"),
            fallback_used=False,
            settings=self.settings,
        )
        return AgentAnswer(
            answer=final_answer,
            plan=safe_plan,
            columns=columns,
            rows=clean_rows,
            mode="llm",
            sql_evidence=_build_sql_evidence(safe_plan),
            model=model_used,
            prompt_version=AGENT_PROMPT_VERSION,
            latency_ms=round(total_latency, 2),
            tokens=tokens_accum,
            stats=stats,
        )

    def _ask_rules(self, question: str) -> AgentAnswer:
        preset_plan, preset_explanation = _build_deterministic_rule_plan(question)
        text = _plain(question)

        if preset_plan is not None:
            plan = preset_plan
            explanation = preset_explanation
        elif any(word in text for word in ("cuantos", "cuantas", "total", "conteo", "promedio", "media")):
            plan = {"dataset": "summary", "filters": {}, "limit": 1}
            explanation = "Resumen general de eventos y magnitudes."
        elif any(word in text for word in ("region", "pais", "zona")):
            plan = {
                "dataset": "top_regions",
                "filters": {},
                "sort": {"field": "event_count", "order": "desc"},
                "limit": 15,
            }
            explanation = "Regiones con mayor cantidad de eventos."
        elif any(word in text for word in ("lugar", "ubicacion")):
            plan = {
                "dataset": "top_places",
                "filters": {},
                "sort": {"field": "event_count", "order": "desc"},
                "limit": 15,
            }
            explanation = "Lugares con mayor cantidad de eventos."
        elif any(word in text for word in ("mayor", "maxim", "fuerte", "intens", "magnitud")):
            filters = {"days": 30} if "30" in text else {}
            plan = {
                "dataset": "events",
                "filters": filters,
                "sort": {"field": "magnitude", "order": "desc"},
                "limit": 10,
            }
            explanation = "Sismos con mayor magnitud registrados."
        elif any(word in text for word in ("reciente", "ultim")):
            plan = {
                "dataset": "events",
                "filters": {},
                "sort": {"field": "event_time", "order": "desc"},
                "limit": 20,
            }
            explanation = "Eventos mas recientes disponibles."
        elif any(word in text for word in ("dia", "evolucion", "fecha", "tiempo", "actividad")):
            plan = {
                "dataset": "daily_activity",
                "filters": {"days": 30},
                "sort": {"field": "event_date", "order": "asc"},
                "limit": 31,
            }
            explanation = "Evolucion diaria de la actividad sismica."
        elif "profundidad" in text:
            plan = {
                "dataset": "events",
                "filters": {},
                "sort": {"field": "depth_km", "order": "desc"},
                "limit": 15,
            }
            explanation = "Eventos con mayor profundidad registrada."
        else:
            plan = {"dataset": "summary", "filters": {}, "limit": 1}
            explanation = "Resumen general disponible en Gold."

        safe_plan, columns, rows = self._execute(plan)
        deterministic_text, stats = compute_deterministic_summary(question, safe_plan, rows)
        clean_rows = [{k: v for k, v in r.items() if k != "_total_matched"} for r in rows]
        return AgentAnswer(
            answer=deterministic_text or explanation,
            plan=safe_plan,
            columns=columns,
            rows=clean_rows,
            mode="reglas",
            sql_evidence=_build_sql_evidence(safe_plan),
            model="deterministic-rules",
            prompt_version=AGENT_PROMPT_VERSION,
            stats=stats,
        )
