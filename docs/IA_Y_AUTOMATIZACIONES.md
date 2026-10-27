# Subsistema de Inteligencia Artificial y Automatizaciones con Ollama — SismoWatch

Este documento detalla el diseño, la arquitectura de seguridad y el funcionamiento del subsistema de Inteligencia Artificial de **SismoWatch**, integrado con el motor local **Ollama (`qwen2.5:3b`)**, la capa **Gold** en MinIO y el esquema operativo `ops` de PostgreSQL (`sismowatch-db`).

---

## 1. Principio Rector: Cero Alucinación Numérica

En una plataforma de monitoreo sísmico, **un modelo de lenguaje nunca debe calcular promedios, conteos, desviaciones estándar ni inventar eventos**. Por ello, SismoWatch aplica una arquitectura de **Separación Estricta entre Cálculo y Narrativa**:

1. **Los datos y las matemáticas se ejecutan en Python, PyArrow y PostgreSQL:** Todos los conteos (`total_events`), magnitudes máximas (`max_magnitude`), promedios (`avg_magnitude`), porcentajes de calidad (`quality_percentage`) y puntajes estadísticos (`z_score`) son calculados de forma determinista sobre los datasets Gold y las tablas `ops`.
2. **Ollama actúa exclusivamente como redactor e intérprete estructurado:** El modelo `qwen2.5:3b` recibe un objeto JSON con los hechos ya calculados (`hechos_calculados`) y un borrador determinista verificado (`resumen_determinista_verificado`). Su única función es redactar una respuesta clara y ejecutiva en español manteniendo exactamente los mismos números.
3. **Respaldo determinista automático (Fallback):** Si el servicio Ollama está apagado, tarda más de `OLLAMA_TIMEOUT` (45 segundos) o devuelve una estructura inválida, el sistema **no falla**: entrega automáticamente la respuesta determinista en español (`mode="reglas"`) e informa al usuario mediante una advertencia explícita.

---

## 2. Por qué NO se Utiliza RAG Vectorial para Consultas Estructuradas

Una decisión arquitectónica fundamental de SismoWatch es **no utilizar RAG (Retrieval-Augmented Generation con embeddings vectoriales)** para responder preguntas sobre el catálogo sísmico:

| Criterio | RAG con Base Vectorial (Descartado) | Consultas Parametrizadas sobre Capa Gold + `ops` (Elegido) |
| :--- | :--- | :--- |
| **Precisión en agregaciones (`COUNT`, `MAX`, `AVG`)** | **Deficiente:** recuperar los *top-k* fragmentos por similitud semántica impide contar cuántos sismos ocurrieron en 24 horas o en 7 días. | **100% Exacta:** PyArrow y PostgreSQL calculan los conteos, máximos y promedios sobre el universo completo de registros validados. |
| **Riesgo de alucinación** | Alto cuando el usuario pregunta por rankings, fechas exactas o comparaciones numéricas. | Nulo en las cifras, porque Python precalcula los hechos antes de invocar a Ollama. |
| **Latencia y consumo de recursos** | Requiere indexar cada 5 minutos miles de vectores y mantener una base vectorial adicional (Chroma/Qdrant/pgvector). | Lectura directa sobre datasets Gold Parquet pre-agregados y tablas indexadas `ops`. |
| **Trazabilidad y auditoría** | Difícil explicar por qué un embedding recuperó un sismo e ignoró otro. | Cada respuesta incluye el `QueryPlan` exacto, los filtros aplicados y una vista previa de las filas reales (`data_preview`). |

---

## 3. Configuración de Ollama en el Proyecto

La configuración centralizada reside en [`config/settings.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/config/settings.py), [`.env`](file:///home/cohorte5/Documentos/Prueba_Kevin/.env) y [`docker-compose.yml`](file:///home/cohorte5/Documentos/Prueba_Kevin/docker-compose.yml):

| Variable de Entorno | Valor Configurado | Propósito |
| :--- | :--- | :--- |
| `OLLAMA_BASE_URL` | `http://host.docker.internal:11434` (en contenedores) / `http://localhost:11434` (en host) | Endpoint HTTP del servidor Ollama local (expone `/api/tags`, `/api/chat` y `/v1/chat/completions`). |
| `OLLAMA_MODEL` | `qwen2.5:3b` | Modelo ligero (1.9 GB), rápido en CPU/GPU local y con excelente soporte de español e instrucciones JSON. |
| `OLLAMA_TIMEOUT` | `45` | Tiempo máximo de espera en segundos antes de activar el respaldo determinista (`mode="reglas"`). |
| `AGENT_LLM_BASE_URL` / `AGENT_LLM_MODEL` | Compatibilidad heredada | Si están vacíos, `Settings.effective_llm_base_url` y `Settings.effective_llm_model` delegan automáticamente en `OLLAMA_BASE_URL` y `OLLAMA_MODEL`. |

En `docker-compose.yml`, los contenedores `api`, `airflow-scheduler` y `airflow-webserver` incluyen:
```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```
Esto permite que los servicios dentro de Docker se comuniquen de forma transparente con el proceso de Ollama instalado en la máquina anfitriona Linux.

---

## 4. El Agente Conversacional (`GoldAgent` + `PlanGuard`)

Implementado en [`agent/agent.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/agent/agent.py) y [`agent/plan_guard.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/agent/plan_guard.py), expone los endpoints `GET /api/v1/agent/suggestions` y `POST /api/v1/agent/query`, conectados con la pestaña **Agente IA** en Streamlit.

### 4.1. Las 6 Consultas Esenciales Soportadas de Punta a Punta
El método `_match_preset_plan()` resuelve de forma determinista y auditable las 6 consultas operativas clave (además de soportar preguntas abiertas mediante selección de `QueryPlan`):

1. **¿Cuántos sismos se registraron en las últimas 24 horas?** (`58 sismos`, máx `M5.80`).
2. **¿Cuál fue el sismo de mayor magnitud reciente?** (`M7.70` en `12 km WSW of Pitaloza Arriba, Panama`).
3. **¿Qué regiones concentran más eventos?** (`Alaska: 552`, `Panama: 121`, `Indonesia: 103`).
4. **¿Cómo se comportó la actividad en los últimos 7 días?** (`378 sismos` en 7 días, pico de `73` el `2026-10-22`).
5. **¿Cuál es el estado actual del pipeline y cuántos registros fueron rechazados por calidad?** (`Calidad: 90.24%` incremental / `98.28%` inicial, `0` inválidos, `2,016` sismos únicos en Silver).
6. **¿Qué anomalías estadísticas se detectaron recientemente?** (`17 anomalías` en los últimos 30 días).

### 4.2. Barrera de Seguridad `PlanGuard`
Ninguna consulta se ejecuta sin superar [`agent/plan_guard.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/agent/plan_guard.py):
- **Catálogo cerrado de datasets (`ALLOWED_DATASETS`):** únicamente `summary`, `events`, `daily_activity`, `magnitude_distribution`, `top_places`, `top_regions`, `data_quality`, `pipeline_metadata` y `anomalies`.
- **Bloqueo de palabras clave destructivas (`FORBIDDEN_SQL_PATTERN`):** rechaza inmediatamente cualquier cadena que contenga `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `CREATE`, `GRANT`, `REVOKE`, `EXEC`, comentarios SQL (`--`, `/* */`) o punto y coma (`;`).
- **Límite estricto de filas (`MAX_LIMIT = 100`):** impide volcados masivos de memoria.

---

## 5. Automatización 1: Generación del Informe Narrativo Diario

Implementada en [`agent/reports.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/agent/reports.py) y almacenada en la tabla `ops.daily_reports`.

### 5.1. Flujo de Trabajo y Validación
1. **Recolección de estadísticas reales:** Obtiene de los datasets Gold y de `ops` las métricas exactas del día (eventos en 24h, total 30d, magnitud máxima y promedio, top regiones, estado de calidad y anomalías).
2. **Sanitización de entradas (`sanitize_external_text`):** Limpia nombres de lugares y regiones eliminando caracteres de control, etiquetas o intentos de *prompt injection* antes de construir el prompt.
3. **Prompt versionado (`REPORT_PROMPT_VERSION = "report-v1.0"`):** Solicita a Ollama un objeto JSON estructurado con título, resumen ejecutivo, hallazgos clave, nota de calidad, nota de anomalías y advertencia metodológica.
4. **Validación de Esquema con Pydantic (`NarrativeReportSchema`):** Verifica tipos de datos y longitudes mínimas/máximas.
5. **Verificación de Coherencia Numérica (`validate_numeric_coherence`):** Comprueba que el texto generado no contradiga las cifras reales calculadas; si falla o si Ollama no responde, construye el informe determinista (`mode="fallback"`).
6. **Persistencia y Exportación:** Guarda con `ON CONFLICT (report_date) DO UPDATE` en `ops.daily_reports` y permite exportar en **Markdown (`.md`)**, **JSON (`.json`)** y **CSV (`.csv`)** mediante `GET /api/v1/reports/export?format={md|json|csv}` y los botones de descarga del dashboard.

---

## 6. Automatización 2: Detección Analítica de Anomalías Estadísticas

Implementada en [`agent/anomalies.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/agent/anomalies.py) y almacenada en la tabla `ops.anomalies`.

### 6.1. Reglas Estadísticas Implementadas
El motor analiza las series históricas de Gold aplicando detectores estadísticos con soporte mínimo de periodos base (`MIN_BASELINE_PERIODS = 5`):

1. **Pico de frecuencia diaria (`daily_count_spike`):**
   - Calcula la media ($\mu$) y desviación estándar ($\sigma$) del conteo diario de sismos.
   - Si un día presenta $Z = \frac{x - \mu}{\sigma} \ge 2.0$, se registra como anomalía (`high` si $Z \ge 3.0$, `medium` si $2.0 \le Z < 3.0$).
   - *Hallazgo real en el sistema:* El `2026-10-10` registró `141 sismos` frente a una media de `65.1` ($Z = 2.66$).
2. **Pico de magnitud (`magnitude_spike`):**
   - Detecta cualquier evento con magnitud $M \ge 6.0$ y calcula su Z-Score frente a la distribución global de magnitudes.
   - *Hallazgo real en el sistema:* El evento de **`M7.70`** en `12 km WSW of Pitaloza Arriba, Panama` ($Z = 6.91$, severidad `high`/`critical`).

### 6.2. Advertencia Científica: Detección Estadística vs. Predicción Sísmica
Cada registro en `ops.anomalies` y cada visualización en el dashboard incluye explícitamente la nota metodológica:
> *"Anomalía estadística observada sobre datos históricos registrados por USGS. NO constituye una predicción sísmica."*

---

## 7. Auditoría y Control de Acceso del Subsistema de IA

### 7.1. Tabla de Auditoría (`ops.ai_audit_logs`)
Toda llamada a `GoldAgent.ask()`, `generate_and_store_daily_report()` o `detect_and_store_anomalies()` registra una fila en `ops.ai_audit_logs` mediante [`agent/audit.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/agent/audit.py) con los siguientes campos reales:
- `component`: `'agent_query'`, `'daily_report'` o `'anomaly_detector'`.
- `model_name` y `prompt_version`: ej. `'qwen2.5:3b'` y `'agent-v1.0'` / `'report-v1.0'`.
- `mode`: `'llm'` o `'reglas'` / `'fallback'`.
- `status`: `'success'`, `'fallback'` o `'error'`.
- `latency_ms`: tiempo real de ejecución en milisegundos.
- `prompt_tokens` y `completion_tokens`: tokens reales devueltos por Ollama (`prompt_eval_count` y `eval_count`).

### 7.2. Control de Roles (`require_write_role`)
En [`api/app.py`](file:///home/cohorte5/Documentos/Prueba_Kevin/api/app.py), los endpoints que disparan la generación de reportes o detección de anomalías (`POST /api/v1/reports/generate` y `POST /api/v1/anomalies/detect`) exigen, además de `X-API-Key`, que el encabezado `X-User-Role` pertenezca al conjunto `{"admin", "analyst", "operator"}`. Si un cliente con rol `viewer` intenta disparar una generación, la API responde con `HTTP 403 Forbidden`.
