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

# ## Silver — `item_access`
# 
# **Grano.** Una fila por *(item, principal, permiso)*. `bronze.item_access` trae una fila por *(item, principal)* con la lista completa de permisos anidada; aquí se "explota" esa lista (`itemAccessDetails.permissions`) para que cada permiso individual sea su propia fila, y se aplanan los campos anidados de `principal` e `itemAccessDetails` a columnas simples. Se descarta `additionalPermissions` (permisos específicos de cada workload, más variables entre tipos de item) para mantener el modelo centrado en los permisos estándar de Fabric.


# CELL ********************

%run shared_fabric_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run shared_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DEPENDENCIAS Y CLIENTES

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# APLANADO Y EXPLOSIÓN: de (item, principal) con lista de permisos, a (item, principal, permiso)

bronze_item_access = spark.table("bronze.item_access")

silver_item_access = (
    bronze_item_access
    .select(
        F.col("itemId").alias("item_id"),
        F.col("workspaceId").alias("workspace_id"),
        F.col("principal.id").alias("principal_id"),
        F.col("principal.displayName").alias("principal_display_name"),
        F.col("principal.userDetails.userPrincipalName").alias("user_principal_name"),
        F.col("principal.type").alias("principal_type"),
        F.col("itemAccessDetails.type").alias("item_type"),
        F.explode("itemAccessDetails.permissions").alias("permission"),
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN SILVER

silver_item_access.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.item_access")

print(f"silver.item_access: {spark.table('silver.item_access').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.item_access LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
