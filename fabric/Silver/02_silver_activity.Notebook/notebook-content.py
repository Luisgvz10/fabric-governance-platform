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

# ## Silver — `activity`
# 
# **Qué hace.** Bronze guarda el evento tal cual lo entrega la API (nombres de Microsoft, sin tipar). Silver aplica el mismo tratamiento que `01_silver_inventory`: selecciona solo los campos con valor analítico —identificador, marca de tiempo, operación, usuario y su tipo, workload, item y workspace afectados, éxito de la operación—, los renombra a snake_case y tipa `CreationTime` como timestamp. Se descartan campos de bajo valor (IP, user agent); siguen disponibles sin pérdida en `bronze.activity`.
# 
# **Por qué `overwrite` y no `MERGE`.** La historificación ya se resuelve en Bronze (`02_bronze_activity`). Silver relee `bronze.activity` completa y reconstruye `silver.activity` desde cero en cada ejecución — misma decisión que en inventario: barato a este volumen, evita duplicar la deduplicación en dos sitios.


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
