"""Auditoria de operaciones de IA en ops.ai_audit_logs."""

from __future__ import annotations

import logging
from typing import Any

from config.settings import Settings, get_settings

LOGGER = logging.getLogger(__name__)


def record_ai_audit(
    operation: str,
    prompt_version: str,
    mode: str,
    status: str,
    question: str | None = None,
    model: str | None = None,
    latency_ms: float | None = None,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    total_tokens: int | None = None,
    fallback_used: bool = False,
    error_message: str | None = None,
    settings: Settings | None = None,
) -> None:
    """Registra una operacion de IA sin inventar metricas ni interrumpir el flujo si no hay BD."""
    runtime = settings or get_settings()
    if not runtime.sismowatch_db_user or not runtime.sismowatch_db_password:
        return
    try:
        from db.connection import connection

        with connection(runtime, readonly=False, statement_timeout_ms=5000) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.ai_audit_logs (
                        operation, question, model, prompt_version, mode,
                        status, latency_ms, prompt_tokens, completion_tokens,
                        total_tokens, fallback_used, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation,
                        (question or "")[:500] if question else None,
                        model,
                        prompt_version,
                        mode,
                        status if status in {"success", "fallback", "error"} else "fallback",
                        round(float(latency_ms), 2) if latency_ms is not None else None,
                        int(prompt_tokens) if prompt_tokens is not None else None,
                        int(completion_tokens) if completion_tokens is not None else None,
                        int(total_tokens) if total_tokens is not None else None,
                        bool(fallback_used),
                        (error_message or "")[:2000] if error_message else None,
                    ),
                )
    except Exception as exc:
        LOGGER.debug("No se pudo registrar auditoria de IA en PostgreSQL: %s", exc)


def fetch_ai_audit_logs(conn, limit: int = 30) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor

    safe_limit = max(1, min(int(limit or 30), 500))
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT audit_id, operation, question, model, prompt_version, mode,
                   status, latency_ms, prompt_tokens, completion_tokens,
                   total_tokens, fallback_used, error_message, created_at
            FROM ops.ai_audit_logs
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (safe_limit,),
        )
        return [dict(row) for row in cur.fetchall()]
