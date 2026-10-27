"""Conexiones a la base operativa de SismoWatch."""

from __future__ import annotations

from contextlib import contextmanager

import psycopg2
from psycopg2.extras import register_uuid

from config.settings import Settings, get_settings

register_uuid()


def connect(
    settings: Settings | None = None,
    readonly: bool = False,
    statement_timeout_ms: int = 30000,
):
    """Abre una conexion a la base operativa, separada de la de Airflow."""
    runtime = settings or get_settings()
    runtime.require_db_credentials()
    options = f"-c statement_timeout={int(statement_timeout_ms)}"
    if readonly:
        options += " -c default_transaction_read_only=on"
    conn = psycopg2.connect(
        host=runtime.sismowatch_db_host,
        port=runtime.sismowatch_db_port,
        dbname=runtime.sismowatch_db_name,
        user=runtime.sismowatch_db_user,
        password=runtime.sismowatch_db_password,
        connect_timeout=10,
        options=options,
    )
    conn.autocommit = False
    return conn


@contextmanager
def connection(
    settings: Settings | None = None,
    readonly: bool = False,
    statement_timeout_ms: int = 30000,
):
    conn = connect(
        settings,
        readonly=readonly,
        statement_timeout_ms=statement_timeout_ms,
    )
    try:
        yield conn
        if not readonly:
            conn.commit()
    except Exception:
        if not readonly:
            conn.rollback()
        raise
    finally:
        conn.close()
