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

# ## Gold — `fact_workspace_access`
# 
# **Grano y por qué no hay unión.** Una fila por *(workspace, principal, rol)* — el mismo grano que `silver.workspace_access`. No se hace ningún `join` aquí: `principal_id` ya enlaza directamente con `gold.dim_user` (mismo `graphId` en ambas tablas), así que la relación con esa dimensión queda resuelta sin necesidad de cruzar nada en este paso. `workspace_id`, en cambio, se mantiene como columna descriptiva suelta —una "dimensión degenerada"—, porque el proyecto no tiene todavía un `dim_workspace` propio (solo `dim_inventory`, a nivel de item, no de workspace); es el mismo criterio ya aplicado en `fact_activity` con `item_name`: no se inventa una FK hacia una dimensión que no existe.


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

fabric = FabricClient()
powerbi = FabricClient(
    scope="https://analysis.windows.net/powerbi/api/.default",
    base_url="https://api.powerbi.com/v1.0/myorg"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN GOLD

silver_workspace_access = spark.table("silver.workspace_access")

gold_fact_workspace_access = silver_workspace_access

gold_fact_workspace_access.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_workspace_access")

print(f"gold.fact_workspace_access: {spark.table('gold.fact_workspace_access').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.fact_workspace_access LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
