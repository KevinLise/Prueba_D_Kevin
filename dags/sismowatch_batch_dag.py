"""DAG principal de la arquitectura Medallion de SismoWatch."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from config.settings import get_settings
from db.runs import history_completed, mark_run_failed, mark_stage
from ingest.ingest_historical import run as run_historical
from ingest.ingest_recent import run as run_recent
from pipeline.record_metrics import record_metrics as persist_metrics

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

DOC_MD = """
# SismoWatch Medallion

1. `bootstrap_history`: carga una sola vez los ultimos 30 dias si
   `ops.load_control` no tiene la marca `historical_30d`.
2. `ingest_recent`: consulta el feed reciente de USGS y escribe Bronze.
3. `run_spark_batch`: lee todo Bronze, valida, normaliza, deduplica y escribe
   Silver/Gold en MinIO.
4. `verify_gold`: comprueba los Parquet Gold en MinIO.
5. `record_metrics`: lee `gold/data_quality`, registra metricas, estado,
   porcentaje de calidad y metadatos de Silver/Gold en PostgreSQL operativo (`ops`).

Los datos sismicos viven en MinIO. PostgreSQL no contiene tablas sismicas.
El DAG se ejecuta cada cinco minutos y todo el procesamiento es Batch.
"""

def on_failure(context) -> None:
    ti = context.get("task_instance")
    if ti is None:
        return
    try:
        payload = ti.xcom_pull(task_ids="ingest_recent")
    except Exception:
        return
    run_id = payload.get("run_id") if isinstance(payload, dict) else None
    if not run_id:
        return
    error = context.get("exception")
    task_id = getattr(ti, "task_id", "unknown")
    stages = {
        "mark_spark_started": "spark",
        "run_spark_batch": "spark",
        "verify_gold": "gold_validation",
        "record_metrics": "gold",
    }
    try:
        mark_run_failed(run_id, f"Fallo en {task_id}: {error}", stage=stages.get(task_id, task_id))
    except Exception:
        # El callback no debe ocultar el fallo original del DAG.
        pass


default_args = {
    "owner": "sismowatch",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure,
}


def bootstrap_history(ti) -> dict:
    if history_completed("historical_30d"):
        return {"performed": False}
    settings = get_settings()
    today = datetime.now(timezone.utc).date()
    run_id = uuid.uuid5(uuid.NAMESPACE_URL, f"sismowatch:{ti.run_id}:historical")
    result = run_historical(
        start_date=today - timedelta(days=settings.historical_days),
        end_date=today,
        min_magnitude=settings.usgs_min_magnitude,
        mark_control=True,
        load_name="historical_30d",
        run_id=run_id,
        airflow_run_id=ti.run_id,
    )
    return {"performed": True, **result}


def ingest_recent_task(ti) -> dict:
    run_id = uuid.uuid5(uuid.NAMESPACE_URL, f"sismowatch:{ti.run_id}:recent")
    return run_recent(run_id=run_id, airflow_run_id=ti.run_id)


def mark_spark_started(ti) -> str:
    payload = ti.xcom_pull(task_ids="ingest_recent")
    if not payload or not payload.get("run_id"):
        raise ValueError("No se encontro run_id de ingest_recent en XCom")
    mark_stage(payload["run_id"], "spark")
    return payload["run_id"]


def record_metrics_task(ti) -> dict:
    payload = ti.xcom_pull(task_ids="ingest_recent")
    if not payload or not payload.get("run_id"):
        raise ValueError("No se encontro run_id de ingest_recent en XCom")
    return persist_metrics(payload["run_id"])




with DAG(
    dag_id="sismowatch_medallion",
    description="Ingesta USGS, Spark Batch y metricas operativas cada 5 minutos",
    doc_md=DOC_MD,
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["sismowatch", "medallion", "batch", "usgs"],
) as dag:
    bootstrap = PythonOperator(
        task_id="bootstrap_history",
        python_callable=bootstrap_history,
        execution_timeout=timedelta(minutes=45),
        retries=1,
    )

    ingest_recent = PythonOperator(
        task_id="ingest_recent",
        python_callable=ingest_recent_task,
        execution_timeout=timedelta(minutes=10),
        retries=2,
    )

    mark_spark = PythonOperator(
        task_id="mark_spark_started",
        python_callable=mark_spark_started,
        execution_timeout=timedelta(minutes=2),
        retries=1,
    )

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command=(
            "spark-submit "
            "--master ${SPARK_MASTER_URL:-spark://spark-master:7077} "
            "--deploy-mode client "
            f"--packages {SPARK_PACKAGES} "
            "--py-files /opt/airflow/spark_jobs/pure.py,"
            "/opt/airflow/spark_jobs/common.py "
            "--conf spark.sql.shuffle.partitions=8 "
            "--conf spark.driver.memory=1g "
            "--conf spark.executor.memory=1g "
            "--conf spark.driver.bindAddress=0.0.0.0 "
            "--conf spark.driver.host=airflow-scheduler "
            "/opt/airflow/spark_jobs/batch_job.py "
            "--run-id {{ ti.xcom_pull(task_ids='ingest_recent')['run_id'] }}"
        ),
        cwd="/opt/airflow",
        append_env=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),
    )

    verify_gold = BashOperator(
        task_id="verify_gold",
        bash_command="python -m pipeline.verify_gold --strict",
        cwd="/opt/airflow",
        append_env=True,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )

    record_metrics = PythonOperator(
        task_id="record_metrics",
        python_callable=record_metrics_task,
        execution_timeout=timedelta(minutes=10),
        retries=2,
    )

    bootstrap >> ingest_recent >> mark_spark >> run_spark_batch >> verify_gold >> record_metrics
