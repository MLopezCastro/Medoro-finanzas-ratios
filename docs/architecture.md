# Arquitectura – Medoro Finanzas Ratios

## Visión general
Este repositorio implementa un flujo simple y reproducible:

**Excel (fuente)** → **Pipeline Python (CSV staging)** → **SQL Server (bronze/silver)** → **Power BI (modelo y medidas)**

El objetivo es que cualquier miembro del equipo pueda reconstruir la base y refrescar Power BI con datos consistentes.

## Componentes

### 1) Pipeline (Python)
- Ubicación: `pipeline/`
- Responsabilidad: convertir Excels en CSV normalizados para carga en SQL.
- Salida: `data/staging_csv/` (no versionado en Git)

### 2) SQL Server
- Base: `Medoro_Finanzas_Ratios`
- Esquemas:
  - `bronze`: tablas raw importadas directo desde CSV (sin tipado fuerte)
  - `silver`: tablas limpias y tipadas (fuente estable para Power BI)

El script maestro:
- crea DB y esquemas
- crea tablas bronze
- hace `BULK INSERT` desde CSV
- crea tablas silver
- inserta en silver aplicando limpieza y conversiones
- finaliza con validación de conteos

### 3) Power BI
- Consume preferentemente `silver.*`
- Las medidas y visuales dependen de que:
  - nombres de tablas y columnas en silver no cambien
  - tipos de datos sean consistentes

## Reglas de versionado (Git)
NO se suben al repo:
- `data/input_excels/`
- `data/staging_csv/`
- archivos `.pbix`

Sí se sube:
- SQL maestro
- scripts auxiliares
- pipeline y requirements
- documentación y guías de ejecución
