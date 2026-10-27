# Manual de Instalación, Operación y Resolución de Problemas — SismoWatch

Esta guía paso a paso permite instalar, configurar, levantar, verificar y depurar todos los componentes de **SismoWatch** en un entorno Linux con Docker Compose y Ollama. Todos los comandos incluidos han sido ejecutados y comprobados sobre el proyecto real.

---

## 1. Requisitos Previos

- **Sistema Operativo:** Linux (Ubuntu 22.04 / 24.04 o equivalente).
- **Docker Engine y Docker Compose v2:** verificables con `docker --version` y `docker compose version`.
- **Python 3.10+ (en el host):** para ejecutar la suite de pruebas unitarias rápidas (`pytest`).
- **Ollama (en el host):** para proveer el modelo de lenguaje local `qwen2.5:3b`.
- **Memoria RAM recomendada:** mínimo 8 GB disponibles (el job de Spark en Airflow está configurado con `--driver-memory 1g --executor-memory 1g` para evitar errores `SIGKILL -9` por falta de memoria).

---

## 2. Clonación del Repositorio y Configuración del Archivo `.env`

1. Clone el repositorio oficial desde GitHub (o ubíquese en la carpeta del proyecto descomprimido):
   ```bash
   git clone https://github.com/KevinLise/Prueba_D_Kevin.git
   cd Prueba_D_Kevin
   ```
2. Si no existe el archivo `.env`, cópielo desde la plantilla `.env.example`:
   ```bash
   cp -n .env.example .env
   ```
3. Verifique que `.env` contenga las variables clave de base de datos (`sismowatch-db`), MinIO, seguridad y Ollama:
   ```ini
   # PostgreSQL Operativo (esquema ops)
   SISMOWATCH_DB_HOST=sismowatch-db
   SISMOWATCH_DB_PORT=5432
   SISMOWATCH_DB_NAME=sismowatch
   SISMOWATCH_DB_USER=sismowatch
   SISMOWATCH_DB_PASSWORD=sismowatch_pass

   # MinIO / Silo (S3 compatible)
   MINIO_ENDPOINT=http://minio:9000
   MINIO_ENDPOINT_HOST=minio:9000
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin123
   MINIO_BUCKET=sismowatch

   # Seguridad API FastAPI
   API_KEY=clave-api-local

   # Subsistema de IA con Ollama Local
   OLLAMA_BASE_URL=http://host.docker.internal:11434
   OLLAMA_MODEL=qwen2.5:3b
   OLLAMA_TIMEOUT=45
   ```

---

## 3. Instalación y Prueba de Ollama (`qwen2.5:3b`)

1. Verifique que el servicio Ollama esté activo en el host y tenga descargado el modelo `qwen2.5:3b`:
   ```bash
   ollama list
   ```
2. Si `qwen2.5:3b` no aparece en la lista, descárguelo con:
   ```bash
   ollama pull qwen2.5:3b
   ```
3. Compruebe que el servidor HTTP de Ollama responde en el puerto `11434`:
   ```bash
   curl -s http://localhost:11434/api/tags | python3 -m json.tool
   ```

---

## 4. Arranque de Servicios con Docker Compose

1. Valide la sintaxis del archivo `docker-compose.yml`:
   ```bash
   docker compose config --quiet
   ```
2. Construya y levante todos los contenedores en segundo plano:
   ```bash
   docker compose up -d --build
   ```
3. Compruebe que los servicios estén en estado `Up` / `healthy`:
   ```bash
   docker compose ps
   ```
   **Puertos expuestos en `localhost`:**
   - **Dashboard Streamlit:** `http://localhost:8501`
   - **API FastAPI (Swagger Docs):** `http://localhost:8001/docs`
   - **Apache Airflow UI:** `http://localhost:8080`
   - **Spark Master UI:** `http://localhost:8082`
   - **MinIO Console:** `http://localhost:9001`
   - **PostgreSQL Operativo (`sismowatch-db`):** `localhost:5432`

---

## 5. Verificación de PostgreSQL (`esquema ops`) y MinIO

### 5.1. Comprobar Tablas y Conteos en PostgreSQL (`sismowatch-db`)
Ejecute el siguiente comando para verificar que el esquema `ops` contiene las tablas operativas y del subsistema de IA pobladas:
```bash
docker compose exec -T sismowatch-db psql -U sismowatch -d sismowatch -c "
SELECT
  (SELECT COUNT(*) FROM ops.runs) AS corridas_pipeline,
  (SELECT COUNT(*) FROM ops.quality_metrics) AS metricas_calidad,
  (SELECT COUNT(*) FROM ops.layer_metadata) AS metadatos_capas,
  (SELECT COUNT(*) FROM ops.anomalies) AS anomalias_detectadas,
  (SELECT COUNT(*) FROM ops.daily_reports) AS reportes_diarios,
  (SELECT COUNT(*) FROM ops.ai_audit_logs) AS logs_auditoria_ia;
"
```

### 5.2. Comprobar las Capas Medallion en MinIO (`bronze`, `silver`, `gold`)
```bash
docker compose exec -T api python -c "
from pipeline.gold_store import GoldStore
store = GoldStore()
print('Summary:', store.read_summary())
print('Total eventos en Gold:', len(store.read_dataset('events')))
"
```

---

## 6. Ejecución de Spark Batch y Supervisión de Airflow

### 6.1. Verificar el Estado del DAG `sismowatch_medallion` en Airflow
```bash
docker compose exec -T airflow-scheduler airflow dags list-runs -d sismowatch_medallion
```

### 6.2. Disparar Manualmente una Corrida Completa del Pipeline
```bash
docker compose exec -T airflow-scheduler airflow dags unpause sismowatch_medallion
docker compose exec -T airflow-scheduler airflow dags trigger sismowatch_medallion
```

### 6.3. Ejecutar el Job de Spark Batch Manualmente (Diagnóstico)
```bash
docker compose exec -T airflow-scheduler spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/airflow/project/spark_jobs/batch_job.py
```

---

## 7. Verificación de FastAPI y Subsistema de IA

1. **Comprobar salud de la API y metadatos:**
   ```bash
   curl -s http://localhost:8001/health | python3 -m json.tool
   curl -s -H "X-API-Key: clave-api-local" http://localhost:8001/api/v1/metadata/summary | python3 -m json.tool
   ```
2. **Probar una consulta al Agente Conversacional (Ollama `qwen2.5:3b`):**
   ```bash
   curl -s -X POST http://localhost:8001/api/v1/agent/query \
     -H "X-API-Key: clave-api-local" \
     -H "Content-Type: application/json" \
     -d '{"question": "¿Cuántos sismos se registraron en las últimas 24 horas?"}' | python3 -m json.tool
   ```
3. **Ejecutar Detección de Anomalías y Generar el Informe Narrativo Diario:**
   ```bash
   curl -s -X POST http://localhost:8001/api/v1/anomalies/detect \
     -H "X-API-Key: clave-api-local" \
     -H "X-User-Role: admin" | python3 -m json.tool

   curl -s -X POST http://localhost:8001/api/v1/reports/generate \
     -H "X-API-Key: clave-api-local" \
     -H "X-User-Role: admin" | python3 -m json.tool
   ```
4. **Exportar el Informe Diario en Markdown, JSON o CSV:**
   ```bash
   curl -s -H "X-API-Key: clave-api-local" \
     "http://localhost:8001/api/v1/reports/export?format=md"
   ```

---

## 8. Ejecución de Pruebas Automatizadas (`pytest` y `AppTest`)

1. **Ejecutar la suite completa de pruebas unitarias (`41 passed, 1 skipped`):**
   ```bash
   pytest -q
   ```
2. **Verificar el renderizado sin errores del Dashboard Streamlit (`AppTest`):**
   ```bash
   docker compose exec -T dashboard python -c "
   from streamlit.testing.v1 import AppTest
   at = AppTest.from_file('dashboard/app.py', default_timeout=30).run()
   assert len(at.exception) == 0, f'Excepciones en dashboard: {at.exception}'
   print('Dashboard verificado con 0 excepciones. Metricas renderizadas:', len(at.metric))
   "
   ```

---

## 9. Guía de Resolución de Problemas Comunes (Troubleshooting)

| Síntoma / Error | Causa Probable | Solución Paso a Paso |
| :--- | :--- | :--- |
| **Código `-9` (`SIGKILL`) en la tarea `run_spark_batch` de Airflow** | El proceso JVM de Spark excedió la memoria RAM libre del host. | Ya está mitigado en [`dags/sismowatch_batch_dag.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/dags/sismowatch_batch_dag.py) con `--driver-memory 1g --executor-memory 1g` y `max_active_runs=1`. Si ocurre nuevamente, libere memoria en el host y reinicie el worker: `docker compose restart spark-worker airflow-scheduler`. |
| **El Agente IA responde en modo `reglas` en lugar de `llm`** | El servicio Ollama del host está detenido, el modelo `qwen2.5:3b` no está descargado o el contenedor no resuelve `host.docker.internal`. | 1. En el host ejecute `curl http://localhost:11434/api/tags`.<br>2. Si no responde, inicie Ollama (`systemctl start ollama` o `ollama serve`).<br>3. Verifique desde el contenedor: `docker compose exec api python -c "import requests; print(requests.get('http://host.docker.internal:11434/api/tags').status_code)"`. |
| **Error `401 API key invalida` al llamar a FastAPI** | Falta el header `X-API-Key` o no coincide con `API_KEY` en `.env`. | Incluya siempre `-H "X-API-Key: clave-api-local"` en sus peticiones `curl`. |
| **Error `403 Rol no autorizado` al generar reportes o anomalías** | Se invocó un endpoint protegido con `X-User-Role: viewer`. | Envíe el encabezado `-H "X-User-Role: admin"` o `-H "X-User-Role: analyst"`. |
| **El Dashboard no refleja un cambio recién hecho en `dashboard/app.py`** | La imagen de `sismowatch-dashboard` empaqueta el código durante el build. | Reconstruya y reinicie el contenedor del dashboard: `docker compose up -d --build dashboard`. |
