# Guía de Presentación y Defensa Académica — SismoWatch

Este documento está diseñado para que un estudiante de **Ingeniería de Sistemas** pueda presentar, explicar y defender con solvencia técnica el proyecto **SismoWatch** ante un jurado o docente evaluador.

---

## 1. Resumen Ejecutivo de la Solución

**SismoWatch** es una plataforma integral de ingeniería de datos e inteligencia artificial aplicada al monitoreo sísmico global. Consume datos sísmicos oficiales del **United States Geological Survey (USGS)** cada 5 minutos (con una ventana histórica inicial de 30 días), los procesa mediante una arquitectura **Medallion Batch (`Bronze` → `Silver` → `Gold`)** sobre **MinIO** y **Apache Spark Batch**, orquesta todo el flujo con **Apache Airflow**, publica los metadatos operativos, reportes y anomalías de forma transaccional e idempotente en **PostgreSQL (`esquema ops`)**, sirve los datasets Gold mediante **FastAPI + PyArrow**, los visualiza en un dashboard interactivo en **Streamlit + Plotly 6 (MapLibre)**, e integra un subsistema de **Inteligencia Artificial local con Ollama (`qwen2.5:3b`)** para consultas en lenguaje natural, generación de reportes narrativos diarios y detección estadística de anomalías.

---

## 2. Planteamiento del Problema

Los catálogos sísmicos públicos como el del USGS emiten boletines continuos en formato GeoJSON crudo. Sin embargo:
1. **Solapamiento y duplicación:** Los feeds horarios repiten los mismos eventos en capturas sucesivas y actualizan sus magnitudes a medida que más estaciones sismológicas revisan la onda sísmica.
2. **Falta de trazabilidad y métricas de calidad:** Consumir directamente el feed desde un dashboard impide auditar cuántos datos llegaron corruptos, cuántos se deduplicaron o conservar la historia en caso de caída de la fuente externa.
3. **Complejidad de interpretación:** Un operador o analista necesita identificar rápidamente anomalías estadísticas (picos de frecuencia diaria o sismos mayores), obtener reportes ejecutivos diarios y realizar preguntas en lenguaje natural sin riesgo de que un modelo de IA invente o alucine cifras sísmicas.

---

## 3. Objetivos del Proyecto

### Objetivo General
Diseñar e implementar una plataforma automatizada de ingeniería de datos y analítica asistida por IA bajo arquitectura Medallion (`Bronze`, `Silver`, `Gold`) para la ingesta, limpieza, agregación, visualización e interpretación segura de actividad sísmica global proveniente del USGS.

### Objetivos Específicos
1. **Orquestar un pipeline batch tolerante a fallos** en Apache Airflow (`sismowatch_medallion`) que ingiera datos cada 5 minutos y garantice una base histórica de 30 días en MinIO (`Bronze`).
2. **Procesar y auditar la calidad de los datos** con Apache Spark Batch (`spark_jobs/batch_job.py`), eliminando duplicados por `event_id` en la capa `Silver` y calculando datasets analíticos en la capa `Gold`.
3. **Publicar metadatos y resultados de forma idempotente** en PostgreSQL (`esquema ops`) registrando ejecuciones (`ops.runs`), métricas de calidad (`ops.quality_metrics`) y frescura por capa (`ops.layer_metadata`).
4. **Exponer y visualizar los resultados** mediante una API autenticada en FastAPI y un dashboard interactivo en Streamlit con mapa geográfico MapLibre, evolución temporal y monitoreo operativo.
5. **Integrar un subsistema de IA local con Ollama (`qwen2.5:3b`)** de solo lectura que responda preguntas sobre datos reales y ejecute dos automatizaciones: generación de reportes narrativos diarios validados con Pydantic y detección estadística de anomalías mediante Z-Score.

---

## 4. Justificación de las Tecnologías Elegidas

- **Por qué MinIO + PostgreSQL (y no uno solo):** MinIO actúa como Data Lakehouse S3 preservando los archivos GeoJSON crudos (`Bronze`) y los Parquet columnares comprimidos con Snappy (`Silver` y `Gold`), mientras que PostgreSQL (`sismowatch-db`, esquema `ops`) actúa como base operativa transaccional para auditoría de corridas, calidad, anomalías, informes diarios y bitácora de IA.
- **Por qué Apache Spark Batch (y no Streaming complejo):** La ventana de actualización de 5 minutos se resuelve con mayor estabilidad y menor costo operativo mediante micro-lotes batch idempotentes que deduplican con funciones de ventana (`ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC)`).
- **Por qué Apache Airflow:** Permite gestionar dependencias entre tareas, reintentos automáticos, prevención de ejecuciones simultáneas (`max_active_runs=1`) y callbacks de fallo (`on_failure_callback`).
- **Por qué Ollama (`qwen2.5:3b`) y Planes de Consulta Validados (sin RAG vectorial):** Los datos sísmicos son cuantitativos y estructurados. Usar RAG vectorial produciría errores graves al contar eventos o calcular magnitudes máximas. En SismoWatch, Python y PyArrow/PostgreSQL hacen el 100% de los cálculos matemáticos y Ollama redacta la narrativa sobre cifras verificadas, sin costo de APIs externas.

---

## 5. Explicación de la Arquitectura y del Flujo de Datos (Guion para la Exposición)

Durante la presentación, puedes explicar el flujo en **4 bloques claros**:

1. **Bloque de Ingesta y Orquestación:** *"Cada 5 minutos, Airflow dispara el DAG `sismowatch_medallion`. Registra el inicio en `ops.runs` con un `UUIDv5` determinista y descarga el feed GeoJSON del USGS hacia el bucket `sismowatch/bronze/` en MinIO."*
2. **Bloque de Procesamiento Medallion con Spark:** *"El job `spark_jobs/batch_job.py` lee `Bronze`, valida coordenadas y magnitudes, normaliza fechas a UTC y elimina duplicados del mismo `event_id`, escribiendo Parquet limpio en `Silver`. Luego calcula los datasets analíticos en `Gold` (`summary`, `daily_activity`, `top_regions`, `events`, `data_quality`)."*
3. **Bloque de Calidad y Automatizaciones Post-Pipeline:** *"Al cerrar el pipeline, `verify_gold.py` comprueba la integridad de los Parquet y `record_metrics.py` calcula el porcentaje de calidad `(valid - duplicate) / bronze * 100` en `ops.quality_metrics`, ejecuta el detector estadístico de anomalías (`Z-Score`) y actualiza el reporte narrativo diario."*
4. **Bloque de Servicio, Visualización e IA:** *"FastAPI protege el acceso mediante `X-API-Key` y roles (`X-User-Role`). El dashboard en Streamlit consume la API en 8 pestañas interactivas, y el Agente IA utiliza `PlanGuard` + Ollama `qwen2.5:3b` en modo de solo lectura."*

---

## 6. Explicación de los Indicadores, Gráficos y Automatizaciones de IA

- **Indicadores verificados en vivo:**
  - `2,234` registros leídos en Bronze → `2,016` sismos únicos en Silver/Gold (`0` inválidos, `218` duplicados filtrados por solapamiento horario, `90.24%` de calidad incremental y `98.28%` en carga inicial).
  - `58 sismos` en las últimas 24 horas ($M_{\max} = 5.80$ en Vanuatu).
  - Magnitud máxima en 30 días: **`M7.70`** en Panamá (`2026-10-09`), magnitud media **`M3.88`**, y **`4` alertas de tsunami**.
  - Top 3 regiones en 30 días: **Alaska (`552`)**, **Panama (`121`)** e **Indonesia (`103`)**.
- **Automatización 1 — Reporte Narrativo Diario (`agent/reports.py`):** Recopila las métricas exactas del día, sanitiza cadenas de texto externas (`place`, `region`), solicita a Ollama un JSON estructurado (`report-v1.0`), lo valida con Pydantic (`NarrativeReportSchema`) y comprueba coherencia numérica antes de guardarlo en `ops.daily_reports` y habilitar su exportación en `.md`, `.json` y `.csv`.
- **Automatización 2 — Detección de Anomalías (`agent/anomalies.py`):** Detecta picos de frecuencia diaria ($Z \ge 2.0$, como el 10 de octubre con `141 sismos`, $Z = 2.66$) y picos de magnitud ($M \ge 6.0$, como el `M7.70` de Panamá con $Z = 6.91$). Incluye siempre una nota explícita aclarando que es un **análisis descriptivo histórico y no una predicción sísmica**.

---

## 7. Seguridad, Calidad de Datos y Resultados de Pruebas

1. **Seguridad:**
   - Autenticación por header `X-API-Key` en los endpoints `/api/v1/*`.
   - Control de roles (`X-User-Role: admin | analyst | operator`) para proteger la generación de reportes y la ejecución manual del detector de anomalías (`403 Forbidden` para `viewer`).
   - `PlanGuard`: catálogo cerrado de datasets (`ALLOWED_DATASETS`), bloqueo por regex de `DROP/DELETE/UPDATE/INSERT/ALTER/TRUNCATE`, prohibición de punto y coma (`;`) y comentarios SQL (`--`, `/* */`).
2. **Pruebas Automatizadas Verificadas:**
   - **`41 passed, 1 skipped`** en `pytest -q` (cubriendo `test_agent.py`, `test_quality_formula.py`, `test_record_metrics.py`, `test_transforms.py`, `test_ingestion.py` y contratos de arquitectura).
   - **Verificación de UI con `AppTest` de Streamlit:** `0 excepciones` en el dashboard.

---

## 8. Limitaciones Actuales y Trabajo Futuro

- **Limitaciones actuales:**
  1. La frecuencia de ingesta es batch cada 5 minutos; por lo tanto, es un sistema de monitoreo casi en tiempo real (*near real-time*) con fines analíticos y académicos, no un sistema de alerta temprana de segundos (EEW).
  2. La inferencia de Ollama se ejecuta localmente en CPU/GPU del host (`5–14 s` por consulta con `qwen2.5:3b`).
- **Trabajo futuro:**
  1. Integrar notificaciones automáticas por Webhook/Telegram cuando el detector de anomalías registre un evento de severidad alta ($M \ge 7.0$).
  2. Incorporar capas GeoJSON de bordes de placas tectónicas sobre el mapa MapLibre.

---

## 9. Banco de Preguntas y Respuestas para la Defensa (Preguntas de Profesor)

### P1: *¿Por qué el porcentaje de calidad pasa de 98.28% en la carga inicial a ~90.24% en las corridas incrementales cada 5 minutos? ¿Significa que los datos vienen malos?*
> **Respuesta:** No; de hecho, `invalid_records` es `0`. Lo que ocurre es que el feed horario del USGS publica todos los sismos de los últimos 60 minutos. Como nuestro DAG de Airflow consulta ese feed cada 5 minutos, un mismo sismo aparece en varios archivos JSON de la capa Bronze durante una hora. La fórmula $\frac{\text{valid} - \text{duplicate}}{\text{bronze}} \times 100$ refleja cuántos registros crudos eran eventos únicos frente a cuántos eran repeticiones que Spark deduplicó exitosamente antes de pasar a Silver.

### P2: *¿Cómo garantizan que el modelo de IA (`qwen2.5:3b`) no invente la magnitud de un terremoto o el número de sismos?*
> **Respuesta:** Porque el LLM jamás calcula estadísticas ni ejecuta consultas libres. Primero, `PlanGuard` valida que el plan apunte únicamente a los datasets permitidos de Gold/`ops`. Segundo, Python calcula el conteo exacto, la magnitud máxima y el promedio sobre las filas reales. Tercero, le pasamos a Ollama un JSON con esos `hechos_calculados` y la instrucción estricta de redactar el texto sin cambiar ninguna cifra. Además, validamos con `validate_numeric_coherence()` que las cifras reales no se contradigan; si falla, se activa el fallback determinista.

### P3: *¿Qué pasa si alguien intenta inyectar SQL destructivo en el chat del Agente IA (por ejemplo: `'; DROP TABLE ops.runs; --`)?*
> **Respuesta:** Es bloqueado inmediatamente por `PlanGuard`:
> 1. `FORBIDDEN_SQL_PATTERN` detecta el `;`, el comentario `--` y la palabra prohibida `DROP`, lanzando `PlanValidationError`.
> 2. Además, el agente no ejecuta SQL arbitrario generado por el usuario, sino planes estructurados (`QueryPlan`) sobre datasets cerrados (`ALLOWED_DATASETS`).
> 3. El evento queda registrado en `ops.ai_audit_logs`.

### P4: *¿Qué ocurre si Spark falla a mitad de camino o si Ollama se apaga durante la exposición?*
> **Respuesta:**
> - **Si falla Spark:** La tarea `verify_gold` detiene el DAG antes de `record_metrics`, y el callback `on_failure_callback` registra el estado `failed` en `ops.runs` sin corromper los metadatos previos.
> - **Si se apaga Ollama:** Tanto el Agente IA como el Generador de Reportes capturan la excepción de conexión y conmutan automáticamente a modo `reglas` / `fallback`, entregando los datos reales en español con una advertencia visible sin romper el dashboard.

### P5: *¿El módulo de anomalías predice cuándo va a ocurrir un terremoto?*
> **Respuesta:** No. Los terremotos no se pueden predecir con fecha, hora y magnitud. Nuestro módulo (`agent/anomalies.py`) realiza **detección estadística descriptiva** sobre datos pasados usando Z-Score ($Z = \frac{x - \mu}{\sigma}$) para identificar días con actividad inusual o picos de magnitud ($M \ge 6.0$). Todas las salidas incluyen esta aclaración metodológica.
