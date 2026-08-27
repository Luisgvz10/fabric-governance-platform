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
# META     },
# META     "environment": {
# META       "environmentId": "c50642e1-ed2e-98df-4121-a4d7e3b1b98e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Silver — `inventory` (capacidades, workspaces, items)
# 
# **Qué hace.** Limpia cada fuente de Bronze por separado, sin cruzarlas todavía: aplana estructuras anidadas, tipa correctamente, selecciona solo las columnas relevantes y las renombra a snake_case. Mantiene el mismo grano que su Bronze correspondiente — la unión entre capacidades, workspaces e items es responsabilidad exclusiva de Gold, para no repetir la limpieza si cambia la lógica de unión.
# 
# **Qué se limpia de cada fuente.** De `capacities`, se descartan campos administrativos (`admins`, `tenantKeyId`). De `items`, `creatorPrincipal` (anidado) se aplana a `creator_name`/`creator_type`, y `lastUpdatedDate` se tipa como `timestamp` — tratando como `NULL` las fechas anteriores al año 2000: la API devuelve el valor mínimo de .NET (`0001-01-01`) para items sin fecha real de actualización (p. ej. el item `App` que se crea al instalar una app desde AppSource), y escribirlo tal cual provoca un fallo de Spark/Parquet, que no admite fechas anteriores a 1900 sin un modo de *rebase* explícito. De todas las fuentes, se conservan los identificadores de relación (`capacityId`, `workspaceId`) para el join en Gold.
# 


# CELL ********************

from pyspark.sql.functions import col, to_timestamp, when, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# LIMPIEZA

silver_capacities = spark.table("bronze.capacities").select(
    col("id").alias("capacity_id"),
    col("displayName").alias("capacity_name"),
    col("sku").alias("capacity_sku"),
    col("region").alias("capacity_region"),
    col("state").alias("capacity_state"),
)

silver_capacities.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.capacities")

###########

silver_workspaces = spark.table("bronze.workspaces").select(
    col("id").alias("workspace_id"),
    col("name").alias("workspace_name"),
    col("type").alias("workspace_type"),
    col("state").alias("workspace_state"),
    col("capacityId").alias("capacity_id")
)

silver_workspaces.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.workspaces")

###########

silver_items = spark.table("bronze.items").select(
    col("id").alias("item_id"),
    col("name").alias("item_name"),
    col("type").alias("item_type"),
    col("state").alias("item_state"),
    when(to_timestamp(col("lastUpdatedDate")) < lit("2000-01-01"), None)
    .otherwise(to_timestamp(col("lastUpdatedDate")))
    .alias("item_last_updated"),
    col("workspaceId").alias("workspace_id"),
    col("folderId").alias("folder_id"),
    col("creatorPrincipal.displayName").alias("creator_name"),
    col("creatorPrincipal.type").alias("creator_type"),
)

silver_items.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.items")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VERIFICACIÓN 

print(f"silver_capacities: {silver_capacities.count()} filas")
print(f"silver_workspaces: {silver_workspaces.count()} filas")
print(f"silver_items: {silver_items.count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
