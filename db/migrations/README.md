# Migraciones de PostgreSQL

La version Medallion ya no usa los esquemas `raw`, `staging` ni `curated`.
PostgreSQL conserva solo el esquema operativo `ops`.

## Metadatos del pipeline (migracion 003)

La migracion `003_pipeline_metadata.sql` es aditiva: agrega columnas a
`ops.runs`, `ops.quality_metrics` y crea `ops.layer_metadata`. No elimina datos.

```bash
docker compose exec -T sismowatch-db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < db/migrations/003_pipeline_metadata.sql
```

## Base nueva

El volumen nuevo ejecuta `db/init/001_schema.sql` y crea solo `ops`.

## Base existente con los esquemas anteriores

No se debe borrar nada automaticamente. Pasos sugeridos:

### 1. Backup

```bash
docker compose exec -T sismowatch-db sh -c \
  'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > sismowatch_backup.dump
```

### 2. Crear el esquema operativo `ops`

El script de inicializacion usa `CREATE ... IF NOT EXISTS`, por lo que se puede
aplicar sobre una base existente sin tocar `raw`, `staging` ni `curated`:

```bash
docker compose exec -T sismowatch-db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < db/init/001_schema.sql
```

### 3. Verificar dependencias

```bash
grep -RIn --exclude-dir=.git --exclude-dir=__pycache__ \
  -e 'raw\.' -e 'staging\.' -e 'curated\.' .
```

Tambien revisa que la API, el dashboard y Airflow funcionen leyendo Gold desde MinIO
y el esquema `ops` desde PostgreSQL.

### 4. Inventario de tablas antiguas

```bash
docker compose exec -T sismowatch-db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < db/migrations/002_drop_legacy_schemas.sql
```

### 5. Eliminacion controlada

Solo despues de backup, verificacion y autorizacion, descomenta los `DROP SCHEMA`
del archivo `002_drop_legacy_schemas.sql` y ejecutalo. La eliminacion es irreversible.
