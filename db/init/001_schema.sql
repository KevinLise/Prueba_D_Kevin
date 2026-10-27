-- PostgreSQL operativo de SismoWatch.
-- Los datos sismicos viven en MinIO: bronze/, silver/ y gold/.
-- No se crean los esquemas raw, staging ni curated.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.runs (
    run_id UUID PRIMARY KEY,
    run_type TEXT NOT NULL CHECK (run_type IN ('recent', 'historical')),
    source TEXT NOT NULL DEFAULT 'usgs',
    airflow_run_id TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'ingested', 'processed', 'completed', 'successful', 'failed')),
    current_stage TEXT,
    bronze_key TEXT,
    records_received BIGINT NOT NULL DEFAULT 0,
    records_processed BIGINT,
    records_bronze BIGINT,
    records_silver BIGINT,
    duration_seconds DOUBLE PRECISION,
    last_data_update TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ops_runs_started_at
    ON ops.runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ops_runs_status
    ON ops.runs (status);

CREATE TABLE IF NOT EXISTS ops.load_control (
    load_name TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    run_id UUID REFERENCES ops.runs (run_id),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.quality_metrics (
    run_id UUID PRIMARY KEY REFERENCES ops.runs (run_id),
    bronze_records BIGINT NOT NULL,
    valid_records BIGINT NOT NULL,
    invalid_records BIGINT NOT NULL,
    duplicate_records BIGINT NOT NULL,
    silver_records BIGINT NOT NULL,
    last_event_time TIMESTAMP,
    freshness_minutes DOUBLE PRECISION,
    quality_percentage DOUBLE PRECISION,
    generated_at TIMESTAMP,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ops_quality_recorded_at
    ON ops.quality_metrics (recorded_at DESC);

CREATE TABLE IF NOT EXISTS ops.layer_metadata (
    layer_metadata_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ops.runs (run_id) ON DELETE CASCADE,
    layer TEXT NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'successful', 'failed')),
    records_in BIGINT,
    records_out BIGINT,
    records_duplicates BIGINT,
    records_invalid BIGINT,
    location TEXT,
    format TEXT,
    event_time_min TIMESTAMP,
    event_time_max TIMESTAMP,
    details JSONB,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, layer)
);

CREATE INDEX IF NOT EXISTS idx_ops_layer_metadata_run_id
    ON ops.layer_metadata (run_id);
CREATE INDEX IF NOT EXISTS idx_ops_layer_metadata_layer
    ON ops.layer_metadata (layer, updated_at DESC);

-- Datos operativos de la aplicacion. No se usan para almacenar sismos.
CREATE TABLE IF NOT EXISTS ops.app_users (
    user_id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.app_roles (
    role_name TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS ops.app_user_roles (
    user_id BIGINT NOT NULL REFERENCES ops.app_users (user_id) ON DELETE CASCADE,
    role_name TEXT NOT NULL REFERENCES ops.app_roles (role_name) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_name)
);

CREATE TABLE IF NOT EXISTS ops.app_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ops.app_roles (role_name, description)
VALUES
    ('admin', 'Administracion de la aplicacion'),
    ('analyst', 'Consulta y analisis de datos sismicos'),
    ('viewer', 'Consulta de solo lectura')
ON CONFLICT (role_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS ops.daily_reports (
    report_id BIGSERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    run_id UUID REFERENCES ops.runs (run_id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK (status IN ('completed', 'fallback', 'failed')),
    generation_mode TEXT NOT NULL CHECK (generation_mode IN ('ollama', 'llm', 'deterministic')),
    model_used TEXT,
    prompt_version TEXT NOT NULL,
    title TEXT NOT NULL,
    executive_summary TEXT NOT NULL,
    key_findings JSONB NOT NULL DEFAULT '[]'::jsonb,
    stats_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_passed BOOLEAN NOT NULL DEFAULT TRUE,
    validation_notes TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ops_daily_reports_date
    ON ops.daily_reports (report_date DESC);

CREATE TABLE IF NOT EXISTS ops.anomalies (
    anomaly_id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    scope TEXT NOT NULL DEFAULT 'global',
    anomaly_type TEXT NOT NULL CHECK (anomaly_type IN ('frequency_spike', 'magnitude_spike')),
    severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
    z_score DOUBLE PRECISION,
    observed_count BIGINT NOT NULL,
    baseline_mean DOUBLE PRECISION,
    baseline_std DOUBLE PRECISION,
    max_magnitude DOUBLE PRECISION,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    explanation TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (window_start, window_end, scope, anomaly_type)
);

CREATE INDEX IF NOT EXISTS idx_ops_anomalies_detected_at
    ON ops.anomalies (detected_at DESC);

CREATE TABLE IF NOT EXISTS ops.ai_audit_logs (
    audit_id BIGSERIAL PRIMARY KEY,
    operation TEXT NOT NULL,
    question TEXT,
    model TEXT,
    prompt_version TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'fallback', 'error')),
    latency_ms DOUBLE PRECISION,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ops_ai_audit_created_at
    ON ops.ai_audit_logs (created_at DESC);
