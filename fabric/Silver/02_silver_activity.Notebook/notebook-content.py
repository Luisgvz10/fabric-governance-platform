# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "87ce6256-385a-4831-8489-84778c0006dc",
# META       "default_lakehouse_name": "lh_metadata",
# META       "default_lakehouse_workspace_id": "79d7380e-fa7c-4ce4-8223-74205adb768f",
# META       "known_lakehouses": [
# META         {
# META           "id": "87ce6256-385a-4831-8489-84778c0006dc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Silver — Limpieza y tipado de `bronze.activity`
# 
# **Objetivo de esta capa.** Bronze almacena el evento de actividad tal cual lo entrega la API de Power BI: JSON crudo, con los nombres de campo originales de Microsoft (`Id`, `CreationTime`, `UserId`...) y sin tipar — por ejemplo, `CreationTime` llega como texto, no como fecha real. Silver aplica sobre esos datos crudos el mismo tratamiento que ya se aplicó al inventario en `00_silver_inventory` (Fase 3): seleccionar solo los campos con valor analítico, renombrarlos a snake_case (convención del proyecto para todo lo que no sea el JSON de origen) y tipar correctamente las fechas, dejando la tabla lista para construir la capa Gold.
# 
# **Qué campos se conservan y por qué.** De todos los campos que devuelve el Activity Events, se seleccionan los que permiten responder las preguntas típicas de un análisis de gobierno de datos — *quién hizo qué, sobre qué recurso, cuándo, y si tuvo éxito*: identificador del evento, marca de tiempo, tipo de operación/actividad, usuario y su tipo, carga de trabajo (`Workload`), item y workspace afectados, y si la operación tuvo éxito. Se descartan deliberadamente campos de bajo valor para este análisis (IP de cliente, user agent, etc.); si hicieran falta más adelante, siguen disponibles sin pérdida en `bronze.activity`, que conserva el registro completo.
# 
# **Por qué `overwrite` y no `MERGE`.** La historificación real —conservar actividad más allá de la ventana de 28 días que ofrece la API— ya se resuelve en Bronze mediante `MERGE` (ver Bronze/01_bronze_activity). Silver no necesita repetir esa lógica: simplemente relee `bronze.activity` completa en cada ejecución y reconstruye `silver.activity` desde cero (`overwrite`). Es la misma decisión ya tomada para el inventario: correcto y barato a este volumen de datos, y evita mantener la deduplicación en dos sitios distintos.
# 
# **Nota de verificación.** Los nombres de columna usados (`Id`, `CreationTime`, etc.) corresponden al esquema estándar documentado por Microsoft para Activity Events. Como `bronze.activity` se construyó infiriendo el esquema directamente del JSON real (ver Bronze/01_bronze_activity), se verificó con `printSchema()` que los nombres reales coinciden antes de dar la selección por buena.


# CELL ********************

from pyspark.sql import functions as F

bronze_activity = spark.table("bronze.activity")

silver_activity = (
    bronze_activity
    .select(
        F.col("Id").alias("activity_id"),
        F.to_timestamp("CreationTime").alias("creation_time"),
        F.col("Operation").alias("operation"),
        F.col("Activity").alias("activity"),
        F.col("UserId").alias("user_id"),
        F.col("UserType").alias("user_type"),
        F.col("Workload").alias("workload"),
        F.col("ItemName").alias("item_name"),
        F.col("WorkSpaceName").alias("workspace_name"),
        F.col("IsSuccess").alias("is_success"),
    )
)

silver_activity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.activity")

print(f"silver.activity: {spark.table('silver.activity').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
