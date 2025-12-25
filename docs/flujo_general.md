# Flujo general – Medoro Finanzas Ratios

## Objetivo
Estandarizar un flujo reproducible para construir la base **Medoro_Finanzas_Ratios** y garantizar que Power BI consuma tablas **silver** con estructura estable.

## Entrada / salida
- **Entrada**: Excels originales (carpeta local o servidor).
- **Salida intermedia**: CSV generados por el pipeline (`staging_csv/`).
- **Salida final**: Tablas SQL Server en:
  - `bronze.*_Raw` (importación directa desde CSV)
  - `silver.*` (tipadas y limpias)

## Pasos operativos

### 1) Pipeline (Excel → CSV)
1. Ubicar los Excels en la carpeta definida como `input_excels/`.
2. Ejecutar `pipeline/run_pipeline.py`.
3. Verificar que se generaron todos los CSV en `data/staging_csv/`.

**Resultado esperado**: en `staging_csv/` deben existir estos archivos:
- `CHEQUES.csv`
- `CHEQUES_EMITIDOS_2.csv`
- `CTACTE_PROVEEDOR.csv`
- `EXTRA_RATIOS.csv`
- `IMPORTACIONES_ADUANA.csv`
- `IMPUESTOS.csv`
- `INTERESES.csv`
- `PROYECCION_COBRANZAS.csv`
- `PROYECCION_SUELDOS.csv`
- `SALDOS_BANCARIOS.csv`

### 2) SQL Server (CSV → bronze/silver)
1. Abrir `sql/01_master_create_db_and_silver.sql`.
2. Modificar **solo** la variable `@RutaCSV` para apuntar a `staging_csv\`.
3. Ejecutar el script completo.
4. Revisar el bloque final de validaciones (counts por tabla).

## Notas importantes
- SQL Server lee los archivos desde la perspectiva del **servidor / servicio de SQL**, no necesariamente desde el usuario local.
- El usuario del servicio de SQL Server debe tener permisos de lectura sobre la carpeta de CSV.
- El script no debe cambiar nombres de tablas/columnas si se quiere que el PBIX funcione sin cambios.
