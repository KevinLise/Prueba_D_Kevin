"""Configuracion central basada en variables de entorno.

No contiene credenciales por defecto. Los valores sensibles deben venir de .env
o del entorno del contenedor.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return float(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_endpoint_host: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    sismowatch_db_host: str
    sismowatch_db_port: int
    sismowatch_db_name: str
    sismowatch_db_user: str
    sismowatch_db_password: str
    usgs_recent_url: str
    usgs_history_url: str
    usgs_min_magnitude: float
    historical_days: int
    usgs_timeout_seconds: int
    spark_master_url: str
    api_url: str
    api_key: str
    agent_llm_api_key: str
    agent_llm_base_url: str
    agent_llm_model: str
    ollama_base_url: str = ""
    ollama_model: str = "qwen2.5:3b"
    ollama_timeout: int = 45

    @property
    def s3_bucket(self) -> str:
        return self.minio_bucket

    def s3_uri(self, key: str) -> str:
        return f"s3://{self.minio_bucket}/{key.lstrip('/')}"

    def s3a_uri(self, key: str) -> str:
        return f"s3a://{self.minio_bucket}/{key.lstrip('/')}"

    @property
    def postgres_jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.sismowatch_db_host}:"
            f"{self.sismowatch_db_port}/{self.sismowatch_db_name}"
        )

    def require_storage_credentials(self) -> None:
        if not self.minio_access_key or not self.minio_secret_key:
            raise RuntimeError(
                "Faltan MINIO_ACCESS_KEY y MINIO_SECRET_KEY. "
                "Copia .env.example a .env y configura las credenciales locales."
            )

    def require_db_credentials(self) -> None:
        if not self.sismowatch_db_user or not self.sismowatch_db_password:
            raise RuntimeError(
                "Faltan SISMOWATCH_DB_USER y SISMOWATCH_DB_PASSWORD. "
                "Copia .env.example a .env y configura las credenciales locales."
            )


def get_settings() -> Settings:
    endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    parsed = urlparse(endpoint)
    endpoint_host = _env(
        "MINIO_ENDPOINT_HOST",
        parsed.netloc or "minio:9000",
    )
    default_secure = endpoint.startswith("https://")
    return Settings(
        minio_endpoint=endpoint,
        minio_endpoint_host=endpoint_host,
        minio_access_key=_env("MINIO_ACCESS_KEY"),
        minio_secret_key=_env("MINIO_SECRET_KEY"),
        minio_bucket=_env("MINIO_BUCKET", "sismowatch"),
        minio_secure=_env_bool("MINIO_SECURE", default_secure),
        sismowatch_db_host=_env("SISMOWATCH_DB_HOST", "sismowatch-db"),
        sismowatch_db_port=_env_int("SISMOWATCH_DB_PORT", 5432),
        sismowatch_db_name=_env("SISMOWATCH_DB_NAME", "sismowatch"),
        sismowatch_db_user=_env("SISMOWATCH_DB_USER", "sismowatch"),
        sismowatch_db_password=_env("SISMOWATCH_DB_PASSWORD"),
        usgs_recent_url=_env(
            "USGS_RECENT_URL",
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        ),
        usgs_history_url=_env(
            "USGS_HISTORY_URL",
            "https://earthquake.usgs.gov/fdsnws/event/1/query",
        ),
        usgs_min_magnitude=_env_float("USGS_MIN_MAGNITUDE", 2.5),
        historical_days=_env_int("HISTORICAL_DAYS", 30),
        usgs_timeout_seconds=_env_int("USGS_TIMEOUT_SECONDS", 30),
        spark_master_url=_env("SPARK_MASTER_URL", "spark://spark-master:7077"),
        api_url=_env("SISMOWATCH_API_URL", "http://api:8000"),
        api_key=_env("API_KEY"),
        agent_llm_api_key=_env("AGENT_LLM_API_KEY") or _env("OPENAI_API_KEY"),
        agent_llm_base_url=_env("AGENT_LLM_BASE_URL") or _env("OPENAI_BASE_URL"),
        agent_llm_model=_env("AGENT_LLM_MODEL", "gpt-4o-mini"),
        ollama_base_url=_env("OLLAMA_BASE_URL"),
        ollama_model=_env("OLLAMA_MODEL", "qwen2.5:3b"),
        ollama_timeout=_env_int("OLLAMA_TIMEOUT", 45),
    )
