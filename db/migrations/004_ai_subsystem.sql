-- Migracion 004: Subsistema de IA (informes diarios, anomalias estadisticas y auditoria de IA).
-- Todo permanece estrictamente en el esquema operativo ops.

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
