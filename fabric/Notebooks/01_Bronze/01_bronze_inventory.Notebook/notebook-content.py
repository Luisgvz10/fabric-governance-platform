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

# ## Bronze — `inventory` (capacidades, workspaces, items)
# 
# **Qué se extrae.** El inventario completo de la plataforma — capacidades (`/admin/capacities`, Power BI), workspaces e items (`/admin/workspaces`, `/admin/items`, Fabric) — paginado con `get_paginated()`. Ninguna llamada requiere permisos adicionales a los ya concedidos en la Fase 1.
# 
# **Por qué esta capa.** Bronze guarda el dato tal cual llega, sin transformar, como punto de recuperación: si la limpieza de Silver cambia o falla, se reprocesa desde aquí sin repetir llamadas a una API limitada a 200 peticiones/hora (5.2.1). Se usa `spark.read.json()` en vez de `spark.createDataFrame()` porque el JSON de estas APIs es heterogéneo entre registros, y `overwrite`/`overwriteSchema` porque esta entidad todavía no tiene historificación (Fase 5).


# CELL ********************

%run shared_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run shared_fabric_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DEPENDENCIAS Y CLIENTES

import json

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

# EXTRACCIÓN

workspaces = fabric.get_paginated("/admin/workspaces", items_key="workspaces")
items = fabric.get_paginated("/admin/items", items_key="itemEntities")
capacities = powerbi.get("/admin/capacities")["value"]

print(f"{len(workspaces)} workspaces, {len(items)} items, {len(capacities)} capacidades")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PERSISTENCIA EN BRONZE

def to_bronze_df(records):
    json_lines = [json.dumps(r) for r in records]
    rdd = spark.sparkContext.parallelize(json_lines)
    return spark.read.json(rdd)

to_bronze_df(workspaces).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.workspaces")
to_bronze_df(items).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.items")
to_bronze_df(capacities).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.capacities")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
