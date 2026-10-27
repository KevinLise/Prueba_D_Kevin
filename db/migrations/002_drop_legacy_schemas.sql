-- ADVERTENCIA: migracion destructiva y manual.
-- No se ejecuta automaticamente. No borra datos por si sola.
--
-- Antes de usarla:
--   1. Haz un backup de la base funcional.
--   2. Confirma que la aplicacion ya no consulta raw, staging ni curated.
--   3. Confirma que los datos sismicos estan en MinIO y que la API lee Gold.
--   4. Autoriza explicitamente la eliminacion.
--
-- Consulta de inventario previa:
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('raw', 'staging', 'curated')
ORDER BY table_schema, table_name;

-- Descomenta y ejecuta SOLO despues de la verificacion y autorizacion:
-- DROP SCHEMA IF EXISTS curated CASCADE;
-- DROP SCHEMA IF EXISTS staging CASCADE;
-- DROP SCHEMA IF EXISTS raw CASCADE;
