from pathlib import Path


SQL = Path("db/init/001_schema.sql").read_text(encoding="utf-8").lower()


def test_only_operational_schema_is_created():
    assert "create schema if not exists ops" in SQL
    assert "create schema if not exists raw" not in SQL
    assert "create schema if not exists staging" not in SQL
    assert "create schema if not exists curated" not in SQL


def test_operational_tables_exist():
    required = [
        "ops.runs",
        "ops.load_control",
        "ops.quality_metrics",
        "ops.layer_metadata",
        "ops.app_users",
        "ops.app_roles",
        "ops.app_user_roles",
        "ops.app_config",
    ]
    for table in required:
        assert table in SQL


def test_no_seismic_tables_in_postgres():
    for table in [
        "curated.earthquakes",
        "staging.silver_events",
        "staging.gold_",
        "raw.ingest_runs",
    ]:
        assert table not in SQL


def test_pipeline_metadata_columns_exist():
    for column in [
        "airflow_run_id",
        "current_stage",
        "records_processed",
        "last_data_update",
        "quality_percentage",
    ]:
        assert column in SQL


def test_no_quarantine_table_or_schema():
    assert "quarantine" not in SQL
    assert "cuarentena" not in SQL
