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

# ## Silver — `workspace_access`
# 
# **Grano.** Una fila por *(workspace, principal)* con su rol — el mismo grano que `bronze.workspace_access`, sin deduplicar. Es el paso contrario al que se hizo ayer para `dim_user`: allí se colapsaba esta misma tabla a un principal por fila, descartando workspace y rol; aquí se conservan todas las columnas, porque el objeto de esta tabla es precisamente el permiso — quién tiene qué rol en qué workspace.
# 
# **Transformación.** Solo selección y renombrado a snake_case, sin lógica adicional: los datos ya llegan limpios desde Bronze (no hay campos anidados ni tipos que forzar en este caso, a diferencia de `CreationTime` en actividad).


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

import json
from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN SILVER OVERWRITE

bronze_access = spark.table("bronze.workspace_access")

silver_workspace_access = (
    bronze_access
    .select(
        F.col("workspaceId").alias("workspace_id"),
        F.col("graphId").alias("principal_id"),
        F.col("groupUserAccessRight").alias("role"),
    )
)

silver_workspace_access.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.workspace_access")

print(f"silver.workspace_access: {spark.table('silver.workspace_access').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.workspace_access LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
