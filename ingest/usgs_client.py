"""Cliente HTTP para el catalogo publico de USGS."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import Settings

LOGGER = logging.getLogger(__name__)
USER_AGENT = "SismoWatch/1.0 (proyecto academico; contacto: local)"


class UsgsError(RuntimeError):
    """Error controlado al consultar o interpretar USGS."""


def build_session(retries: int = 5, backoff_factor: float = 0.5) -> requests.Session:
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/geo+json"})
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_json(
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    params: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    response = session.get(url, params=params, timeout=timeout_seconds)
    response.raise_for_status()
    text = response.text
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise UsgsError("USGS devolvio una respuesta que no es JSON valido") from exc
    validate_feature_collection(payload)
    return text, payload


def validate_feature_collection(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise UsgsError("La respuesta de USGS no es un objeto JSON")
    features = payload.get("features")
    if not isinstance(features, list):
        raise UsgsError("La respuesta de USGS no contiene una lista 'features'")


def extract_features(payload: dict[str, Any]) -> list[dict[str, Any]]:
    validate_feature_collection(payload)
    return [feature for feature in payload["features"] if isinstance(feature, dict)]


def event_time_range(features: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    """Obtiene el rango temporal real de los eventos recibidos."""
    times = []
    for feature in features:
        properties = feature.get("properties") or {}
        value = properties.get("time")
        try:
            millis = float(value)
        except (TypeError, ValueError):
            continue
        if millis <= 0:
            continue
        try:
            times.append(
                datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc).replace(tzinfo=None)
            )
        except (OverflowError, OSError, ValueError):
            continue
    if not times:
        return None, None
    return min(times), max(times)


def fetch_recent(session: requests.Session, settings: Settings) -> tuple[str, dict[str, Any]]:
    LOGGER.info("Consultando feed reciente de USGS: %s", settings.usgs_recent_url)
    return _get_json(session, settings.usgs_recent_url, settings.usgs_timeout_seconds)


def fetch_historical(
    session: requests.Session,
    settings: Settings,
    start_date: date,
    end_date: date,
    min_magnitude: float | None = None,
) -> tuple[str, dict[str, Any]]:
    if start_date > end_date:
        raise ValueError("start_date no puede ser posterior a end_date")
    magnitude = settings.usgs_min_magnitude if min_magnitude is None else min_magnitude
    params = {
        "format": "geojson",
        "starttime": f"{start_date.isoformat()}T00:00:00",
        "endtime": f"{end_date.isoformat()}T23:59:59",
        "minmagnitude": magnitude,
        "eventtype": "earthquake",
        "orderby": "time-asc",
    }
    LOGGER.info(
        "Consultando historico de USGS: start=%s end=%s min_magnitude=%s",
        start_date,
        end_date,
        magnitude,
    )
    return _get_json(
        session,
        settings.usgs_history_url,
        settings.usgs_timeout_seconds,
        params=params,
    )
