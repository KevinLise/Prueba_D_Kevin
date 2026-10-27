"""Cliente HTTP del dashboard hacia la API de SismoWatch."""

from __future__ import annotations

from typing import Any

import requests


class ApiError(RuntimeError):
    """Error controlado de la API."""


class SismoWatchClient:
    def __init__(self, base_url: str, api_key: str = "") -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"X-API-Key": api_key})

    def _request(self, method: str, path: str, **kwargs) -> Any:
        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            timeout=kwargs.pop("timeout", 20),
            **kwargs,
        )
        if response.status_code >= 400:
            detail = response.text[:500]
            raise ApiError(f"API {response.status_code}: {detail}")
        return response.json()

    def health(self) -> dict:
        return self._request("GET", "/health")

    def summary(self) -> list[dict]:
        return self._request("GET", "/api/v1/summary")["data"]

    def events(
        self,
        limit: int,
        min_magnitude: float | None = None,
        region: str | None = None,
        place: str | None = None,
    ) -> list[dict]:
        params: dict[str, Any] = {"limit": limit}
        if min_magnitude is not None:
            params["min_magnitude"] = min_magnitude
        if region:
            params["region"] = region
        if place:
            params["place"] = place
        return self._request("GET", "/api/v1/events", params=params)["data"]

    def daily(self, days: int) -> list[dict]:
        return self._request("GET", "/api/v1/daily-activity", params={"days": days})["data"]

    def magnitudes(self) -> list[dict]:
        return self._request("GET", "/api/v1/magnitude-distribution")["data"]

    def places(self, limit: int = 20) -> list[dict]:
        return self._request("GET", "/api/v1/top-places", params={"limit": limit})["data"]

    def regions(self, limit: int = 20) -> list[dict]:
        return self._request("GET", "/api/v1/top-regions", params={"limit": limit})["data"]

    def quality(self, limit: int = 20) -> list[dict]:
        return self._request("GET", "/api/v1/quality", params={"limit": limit})["data"]

    def runs(self, limit: int = 20) -> list[dict]:
        return self._request("GET", "/api/v1/runs", params={"limit": limit})["data"]

    def metadata_summary(self) -> dict:
        return self._request("GET", "/api/v1/metadata/summary")["data"]

    def metadata_runs(self, limit: int = 20) -> list[dict]:
        return self._request(
            "GET", "/api/v1/metadata/runs", params={"limit": limit}
        )["data"]

    def metadata_quality(self, limit: int = 20) -> list[dict]:
        return self._request(
            "GET", "/api/v1/metadata/quality", params={"limit": limit}
        )["data"]

    def metadata_layers(self, limit: int = 50, run_id: str | None = None) -> list[dict]:
        params = {"limit": limit}
        if run_id:
            params["run_id"] = run_id
        return self._request(
            "GET", "/api/v1/metadata/layers", params=params
        )["data"]

    def ask(self, question: str) -> dict:
        return self._request(
            "POST",
            "/api/v1/agent/query",
            json={"question": question},
            timeout=60,
        )

    def suggestions(self) -> list[str]:
        return self._request("GET", "/api/v1/agent/suggestions")["data"]

    def reports(self, limit: int = 15) -> list[dict]:
        return self._request("GET", "/api/v1/reports", params={"limit": limit})["data"]

    def generate_report(self, role: str = "admin") -> dict:
        return self._request(
            "POST",
            "/api/v1/reports/generate",
            headers={"X-User-Role": role},
            timeout=60,
        )["data"]

    def export_report(self, fmt: str = "md") -> str:
        response = self.session.request(
            "GET",
            f"{self.base_url}/api/v1/reports/export",
            params={"format": fmt},
            timeout=30,
        )
        if response.status_code >= 400:
            raise ApiError(f"API {response.status_code}: {response.text[:500]}")
        return response.text

    def anomalies(self, limit: int = 30) -> list[dict]:
        return self._request("GET", "/api/v1/anomalies", params={"limit": limit})["data"]

    def detect_anomalies(
        self,
        z_threshold: float = 2.0,
        magnitude_threshold: float = 6.0,
        role: str = "admin",
    ) -> dict:
        return self._request(
            "POST",
            "/api/v1/anomalies/detect",
            params={"z_threshold": z_threshold, "magnitude_threshold": magnitude_threshold},
            headers={"X-User-Role": role},
            timeout=60,
        )

    def ai_audit(self, limit: int = 30) -> list[dict]:
        return self._request("GET", "/api/v1/ai-audit", params={"limit": limit})["data"]
