from pathlib import Path


COMPOSE = Path("docker-compose.yml").read_text(encoding="utf-8").lower()


def test_lambda_services_are_absent():
    for token in ["kafka", "kafka-init", "producer", "streaming", "duckdb"]:
        assert token not in COMPOSE


def test_medallion_services_are_present():
    for service in [
        "airflow-scheduler",
        "spark-master",
        "spark-worker",
        "minio",
        "minio-init",
        "sismowatch-db",
        "api",
        "dashboard",
    ]:
        assert f"  {service}:" in COMPOSE


def test_postgres_databases_are_separate():
    assert "airflow-db:" in COMPOSE
    assert "sismowatch-db:" in COMPOSE
