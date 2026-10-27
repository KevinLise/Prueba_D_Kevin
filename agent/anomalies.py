"""Deteccion estadistica de anomalias sismicas sobre datos reales de Gold."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta
import logging
import math
from typing import Any

from config.settings import Settings, get_settings

LOGGER = logging.getLogger(__name__)

NON_PREDICTIVE_NOTE = (
    "Nota: Esta es una anomalia estadistica sobre datos historicos observados; "
    "no constituye una prediccion de terremotos."
)


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = sum(values) / len(values)
    if len(values) < 2:
        return mean, 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return mean, math.sqrt(variance)


def _to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
    return None


def compute_anomalies_from_rows(
    daily_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    z_threshold: float = 2.0,
    magnitude_threshold: float = 6.0,
    min_periods: int = 5,
) -> list[dict[str, Any]]:
    """Calcula anomalias estadisticas por z-score horario/diario y picos de magnitud."""
    anomalies: list[dict[str, Any]] = []

    # 1. Anomalias de frecuencia por ventana diaria (z-score)
    valid_daily = [
        row for row in daily_rows if row.get("event_date") is not None and row.get("event_count") is not None
    ]
    if len(valid_daily) >= min_periods:
        counts = [float(row["event_count"]) for row in valid_daily]
        mean, std = _mean_std(counts)
        if std > 0:
            for row in valid_daily:
                count = int(row["event_count"])
                z_val = (count - mean) / std
                if z_val >= z_threshold:
                    dt_start = _to_datetime(row["event_date"]) or datetime.utcnow()
                    dt_end = dt_start + timedelta(days=1)
                    severity = "high" if z_val >= 3.0 else "medium"
                    max_mag = float(row.get("max_magnitude") or 0.0)
                    anomalies.append(
                        {
                            "window_start": dt_start,
                            "window_end": dt_end,
                            "scope": "daily_global",
                            "anomaly_type": "frequency_spike",
                            "severity": severity,
                            "z_score": round(z_val, 2),
                            "observed_count": count,
                            "baseline_mean": round(mean, 2),
                            "baseline_std": round(std, 2),
                            "max_magnitude": round(max_mag, 2),
                            "evidence": {
                                "event_date": str(row["event_date"]),
                                "observed_count": count,
                                "baseline_mean": round(mean, 2),
                                "baseline_std": round(std, 2),
                                "z_score": round(z_val, 2),
                                "avg_magnitude": round(float(row.get("avg_magnitude") or 0.0), 2),
                            },
                            "explanation": (
                                f"Incremento estadistico de frecuencia el {row['event_date']}: "
                                f"{count} sismos observados frente a una media de {mean:.1f} "
                                f"(z-score={z_val:.2f}). {NON_PREDICTIVE_NOTE}"
                            ),
                        }
                    )

    # 2. Anomalias por ventana horaria y por magnitud extrema sobre eventos Gold
    parsed_events: list[tuple[datetime, dict[str, Any]]] = []
    for ev in event_rows:
        dt = _to_datetime(ev.get("event_time"))
        if dt is not None and ev.get("magnitude") is not None:
            parsed_events.append((dt, ev))

    if parsed_events:
        hourly_buckets: Counter[datetime] = Counter()
        hourly_max_mag: dict[datetime, float] = {}
        hourly_top_region: dict[datetime, str] = {}
        for dt, ev in parsed_events:
            hour_dt = dt.replace(minute=0, second=0, microsecond=0)
            hourly_buckets[hour_dt] += 1
            mag = float(ev["magnitude"])
            if mag >= hourly_max_mag.get(hour_dt, -99.0):
                hourly_max_mag[hour_dt] = mag
                hourly_top_region[hour_dt] = str(ev.get("region") or ev.get("place") or "Global")

        if len(hourly_buckets) >= max(min_periods, 10):
            h_counts = [float(c) for c in hourly_buckets.values()]
            h_mean, h_std = _mean_std(h_counts)
            if h_std > 0:
                for hour_dt, count in sorted(hourly_buckets.items(), key=lambda x: x[1], reverse=True)[:15]:
                    z_val = (count - h_mean) / h_std
                    if z_val >= max(z_threshold, 2.5):
                        severity = "high" if z_val >= 3.5 else "medium"
                        region_name = hourly_top_region.get(hour_dt, "Global")
                        max_mag = hourly_max_mag.get(hour_dt, 0.0)
                        anomalies.append(
                            {
                                "window_start": hour_dt,
                                "window_end": hour_dt + timedelta(hours=1),
                                "scope": f"hourly:{region_name[:80]}",
                                "anomaly_type": "frequency_spike",
                                "severity": severity,
                                "z_score": round(z_val, 2),
                                "observed_count": int(count),
                                "baseline_mean": round(h_mean, 2),
                                "baseline_std": round(h_std, 2),
                                "max_magnitude": round(max_mag, 2),
                                "evidence": {
                                    "hour_utc": hour_dt.isoformat(),
                                    "observed_count": int(count),
                                    "baseline_hourly_mean": round(h_mean, 2),
                                    "baseline_hourly_std": round(h_std, 2),
                                    "z_score": round(z_val, 2),
                                    "dominant_region": region_name,
                                },
                                "explanation": (
                                    f"Concentracion horaria inusual ({hour_dt:%Y-%m-%d %H:00} UTC): "
                                    f"{count} sismos en 1 hora (media={h_mean:.1f}, z={z_val:.2f}), "
                                    f"destacando {region_name}. {NON_PREDICTIVE_NOTE}"
                                ),
                            }
                        )

        # 3. Eventos de magnitud alta (magnitude_spike)
        mags = [float(ev["magnitude"]) for _, ev in parsed_events]
        m_mean, m_std = _mean_std(mags)
        for dt, ev in sorted(parsed_events, key=lambda item: float(item[1]["magnitude"]), reverse=True)[:10]:
            mag = float(ev["magnitude"])
            if mag >= magnitude_threshold:
                z_mag = (mag - m_mean) / m_std if m_std > 0 else 0.0
                severity = "high" if mag >= 6.5 else "medium"
                place = str(ev.get("place") or ev.get("region") or "Ubicacion no especificada")
                region = str(ev.get("region") or "Global")
                hour_start = dt.replace(minute=0, second=0, microsecond=0)
                anomalies.append(
                    {
                        "window_start": hour_start,
                        "window_end": hour_start + timedelta(hours=1),
                        "scope": f"event:{ev.get('event_id') or region[:60]}",
                        "anomaly_type": "magnitude_spike",
                        "severity": severity,
                        "z_score": round(z_mag, 2),
                        "observed_count": 1,
                        "baseline_mean": round(m_mean, 2),
                        "baseline_std": round(m_std, 2),
                        "max_magnitude": round(mag, 2),
                        "evidence": {
                            "event_id": ev.get("event_id"),
                            "event_time": dt.isoformat(),
                            "place": place,
                            "region": region,
                            "magnitude": round(mag, 2),
                            "depth_km": ev.get("depth_km"),
                            "z_score": round(z_mag, 2),
                        },
                        "explanation": (
                            f"Sismo de magnitud elevada M{mag:.2f} registrado en {place} "
                            f"({dt:%Y-%m-%d %H:%M} UTC, z-score={z_mag:.2f} respecto a media {m_mean:.2f}). "
                            f"{NON_PREDICTIVE_NOTE}"
                        ),
                    }
                )

    # Deduplicar por clave unica (window_start, window_end, scope, anomaly_type)
    deduped: dict[tuple[Any, Any, str, str], dict[str, Any]] = {}
    for item in anomalies:
        key = (item["window_start"], item["window_end"], item["scope"], item["anomaly_type"])
        deduped[key] = item
    result = list(deduped.values())
    result.sort(
        key=lambda x: (
            1 if x["severity"] == "high" else 0,
            float(x.get("z_score") or 0.0),
            float(x.get("max_magnitude") or 0.0),
        ),
        reverse=True,
    )
    return result[:30]


def persist_anomalies(anomalies: list[dict[str, Any]], settings: Settings | None = None) -> int:
    """Guarda anomalias de forma idempotente mediante UPSERT en ops.anomalies."""
    if not anomalies:
        return 0
    runtime = settings or get_settings()
    if not runtime.sismowatch_db_user or not runtime.sismowatch_db_password:
        return 0
    from psycopg2.extras import Json
    from db.connection import connection

    saved = 0
    with connection(runtime, readonly=False) as conn:
        with conn.cursor() as cur:
            for item in anomalies:
                cur.execute(
                    """
                    INSERT INTO ops.anomalies (
                        window_start, window_end, scope, anomaly_type, severity,
                        z_score, observed_count, baseline_mean, baseline_std,
                        max_magnitude, evidence, explanation, detected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (window_start, window_end, scope, anomaly_type)
                    DO UPDATE SET
                        severity = EXCLUDED.severity,
                        z_score = EXCLUDED.z_score,
                        observed_count = EXCLUDED.observed_count,
                        baseline_mean = EXCLUDED.baseline_mean,
                        baseline_std = EXCLUDED.baseline_std,
                        max_magnitude = EXCLUDED.max_magnitude,
                        evidence = EXCLUDED.evidence,
                        explanation = EXCLUDED.explanation,
                        detected_at = now()
                    """,
                    (
                        item["window_start"],
                        item["window_end"],
                        item["scope"],
                        item["anomaly_type"],
                        item["severity"],
                        item["z_score"],
                        item["observed_count"],
                        item["baseline_mean"],
                        item["baseline_std"],
                        item["max_magnitude"],
                        Json(item["evidence"]),
                        item["explanation"],
                    ),
                )
                saved += 1
    return saved


def detect_and_store_anomalies(
    store,
    settings: Settings | None = None,
    z_threshold: float = 2.0,
    magnitude_threshold: float = 6.0,
    persist: bool = True,
) -> list[dict[str, Any]]:
    daily_rows = store.read("daily_activity")
    event_rows = store.read("events")
    anomalies = compute_anomalies_from_rows(
        daily_rows=daily_rows,
        event_rows=event_rows,
        z_threshold=z_threshold,
        magnitude_threshold=magnitude_threshold,
    )
    if persist and anomalies:
        try:
            persist_anomalies(anomalies, settings=settings)
        except Exception as exc:
            LOGGER.warning("No se pudieron persistir anomalias en ops.anomalies: %s", exc)
    return [
        {
            **item,
            "window_start": item["window_start"].isoformat()
            if hasattr(item["window_start"], "isoformat")
            else str(item["window_start"]),
            "window_end": item["window_end"].isoformat()
            if hasattr(item["window_end"], "isoformat")
            else str(item["window_end"]),
        }
        for item in anomalies
    ]


def fetch_anomalies(conn, limit: int = 30) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor

    safe_limit = max(1, min(int(limit or 30), 500))
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT anomaly_id, window_start, window_end, scope, anomaly_type,
                   severity, z_score, observed_count, baseline_mean,
                   baseline_std, max_magnitude, evidence, explanation, detected_at
            FROM ops.anomalies
            ORDER BY
                CASE severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                window_start DESC
            LIMIT %s
            """,
            (safe_limit,),
        )
        return [dict(row) for row in cur.fetchall()]
