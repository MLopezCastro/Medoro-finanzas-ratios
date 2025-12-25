# Medoro Finanzas Ratios

Repositorio técnico para replicar la base **Medoro_Finanzas_Ratios** a partir de archivos CSV generados por el pipeline (Excel → CSV), creando tablas **bronze** (raw) y **silver** (limpias) en SQL Server.

## Objetivo
- Estandarizar el armado de la DB para que el equipo pueda reconstruir el modelo en cualquier entorno.
- Garantizar que Power BI consuma datos consistentes (silver) con estructura estable.

## Requisitos
- SQL Server (con permisos para `CREATE DATABASE` y `BULK INSERT`)
- Acceso a una carpeta local/servidor con los CSV ya generados por el pipeline
- El usuario de SQL Server debe tener permisos sobre la carpeta de CSV (lectura)

## Flujo de datos
1. `input_excels/` (fuera de Git) contiene los Excels originales.
2. Pipeline convierte Excels → `data/staging_csv/` (fuera de Git).
3. SQL Server ejecuta script maestro y carga:
   - `bronze.*_Raw` (importación directa de CSV)
   - `silver.*` (tablas limpias y tipadas)

## Estructura del repo
- `sql/` scripts SQL (script maestro + validaciones)
- `pipeline/` documentación/ejecución del pipeline
- `docs/` documentación técnica (arquitectura, diccionario)
- `data/` carpetas locales (NO se suben al repo)
- `powerbi/` documentación para conexión de PBIX (PBIX no se versiona aquí)

## Quick start (SQL)
1. Generar CSV en una carpeta (ej: `C:\...\staging_csv\`)
2. Editar SOLO la variable `@RutaCSV` en el script maestro.
3. Ejecutar el script maestro completo.
4. Verificar conteos con el bloque de validación al final.

## Seguridad / Notas
- No subir CSV/XLSX ni PBIX al repositorio.
- En producción, la ruta de CSV será la del servidor donde corra el pipeline.
