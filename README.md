# Medoro Finanzas Ratios

Repositorio técnico para replicar la base **Medoro_Finanzas_Ratios** a partir de archivos CSV generados por el pipeline (Excel → CSV), creando tablas **bronze** (raw) y **silver** (limpias) en SQL Server.

## Objetivo
- Estandarizar el armado de la DB para que el equipo pueda reconstruir el modelo en cualquier entorno.
- Garantizar que Power BI consuma datos consistentes (capa **silver**) con estructura estable.

## Requisitos
- SQL Server (permisos para `CREATE DATABASE` y `BULK INSERT`)
- CSV generados por el pipeline en una carpeta accesible para SQL Server
- El usuario/servicio de SQL Server debe tener permisos de lectura sobre esa carpeta

## Flujo de datos (alto nivel)
1. **Excel** (fuente operativa) → `data/input_excels/`
2. Pipeline convierte Excel → **CSV** → `data/staging_csv/`
3. Script SQL crea y carga:
   - `bronze.*_Raw` (importación directa desde CSV)
   - `silver.*` (tablas limpias, tipadas y listas para Power BI)

## Estructura del repo
- `sql/` Script maestro SQL + validaciones
- `pipeline/` Script de pipeline + requirements + config de ejemplo
- `docs/` Documentación técnica (arquitectura / flujo)
- `powerbi/` Documentación para conexión del PBIX (el PBIX no se versiona aquí)
- `data/` Carpetas locales de trabajo (no se versionan)

## Quick start (SQL)
1. Ejecutar el pipeline para generar CSV (ej: `...\data\staging_csv\`)
2. Editar **solo** la variable `@RutaCSV` en `sql/01_master_create_db_and_silver.sql`
3. Ejecutar el script maestro completo
4. Validar conteos con el bloque final de “VALIDACIONES”

## Regla de compatibilidad (contrato de datos)
La capa **silver** es el **contrato** con Power BI.  
No renombrar tablas/columnas ni cambiar tipos sin actualizar el PBIX.

## Notas de seguridad / versionado
- No subir **XLSX**, **CSV** ni **PBIX** al repositorio.
- En servidor, `@RutaCSV` debe apuntar a la carpeta donde el pipeline deja los CSV.
