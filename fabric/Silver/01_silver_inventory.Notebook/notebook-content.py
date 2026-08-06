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

# Capa Silver — principio general. Toma los datos crudos de Bronze y los limpia: aplana estructuras anidadas, aplica tipos correctos, selecciona solo las columnas relevantes y las renombra con nombres claros para negocio. Sigue teniendo el mismo grano que su fuente Bronze correspondiente — todavía no se cruza información entre capacidades, workspaces e items; esa unión es responsabilidad exclusiva de la capa Gold. Mantener esta separación evita que un cambio en la lógica de unión obligue a repetir la limpieza de cada fuente.
# 
# Qué se limpia de cada fuente. De bronze_capacities, los campos administrativos (admins, tenantKeyId) se descartan por no aportar al modelo de inventario. De bronze_workspaces y bronze_items, se conservan los identificadores de relación (capacityId, workspaceId) para el join posterior en Gold. De bronze_items, creatorPrincipal —una estructura anidada— se aplana a dos columnas simples (creator_name, creator_type), y lastUpdatedDate se convierte de texto a tipo timestamp.


# CELL ********************

from pyspark.sql.functions import col, to_timestamp


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
    to_timestamp(col("lastUpdatedDate")).alias("item_last_updated"),
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
