-- Migracion aditiva para metadatos del pipeline Medallion.
-- No elimina tablas ni datos. Se puede aplicar sobre la base existente.
BEGIN;

ALTER TABLE ops.runs
    ADD COLUMN IF NOT EXISTS airflow_run_id TEXT,
    ADD COLUMN IF NOT EXISTS current_stage TEXT,
    ADD COLUMN IF NOT EXISTS records_processed BIGINT,
    ADD COLUMN IF NOT EXISTS last_data_update TIMESTAMPTZ;

ALTER TABLE ops.runs DROP CONSTRAINT IF EXISTS runs_status_check;
ALTER TABLE ops.runs
    ADD CONSTRAINT runs_status_check
    CHECK (status IN ('pending', 'running', 'ingested', 'processed', 'completed', 'successful', 'failed'));

ALTER TABLE ops.quality_metrics
    ADD COLUMN IF NOT EXISTS quality_percentage DOUBLE PRECISION;

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

COMMIT;
