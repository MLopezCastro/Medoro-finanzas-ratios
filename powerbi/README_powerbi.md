# Power BI

## Conexión recomendada
- Fuente: SQL Server
- Base: `Medoro_Finanzas_Ratios`
- Esquema: `silver`

## Nota sobre replicación
El PBIX debe apuntar a las mismas tablas `silver.*`.
Si se reconstruye la DB con el script maestro, el modelo queda estable siempre que:
- Los nombres de tablas/columnas en silver no cambien.
- El pipeline mantenga el mismo set de CSV.
