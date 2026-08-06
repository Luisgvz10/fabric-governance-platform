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

# En una arquitectura Medallion, la capa Bronze almacena los datos exactamente como llegan de la fuente, sin transformar ni limpiar. Su función no es ser consultable directamente por el modelo de gobierno, sino actuar como punto de recuperación: si la lógica de limpieza de la capa Silver cambia o contiene un error, se puede reprocesar desde Bronze sin volver a llamar a la API — importante aquí porque las APIs de administración de Fabric y Power BI tienen un límite de 200 peticiones/hora (5.2.1 de la memoria), un recurso que conviene no gastar dos veces por el mismo dato.
# 
# Qué extrae este notebook. El dominio de "inventario" de la plataforma: capacidades, workspaces e items, cada uno desde su fuente correspondiente — /admin/capacities de la API de Power BI, y /admin/workspaces / /admin/items de la API de Fabric, paginados mediante continuationToken a través de get_paginated() (shared_fabric_client). Ninguna de las tres llamadas requiere permisos adicionales a los ya concedidos al Service Principal en la Fase 1.
# 
# Por qué spark.read.json en vez de spark.createDataFrame directo. Las respuestas de estas APIs son JSON heterogéneo: campos como creatorPrincipal o tags no siempre están presentes ni tienen la misma forma en todos los registros. createDataFrame infiere el esquema a partir de objetos Python de una vez y falla si no puede determinar el tipo de un campo; spark.read.json está diseñado específicamente para fusionar esquemas distintos entre registros, por lo que resulta mucho más robusto para datos de una API externa cuya forma exacta no se controla.
# 
# Por qué overwriteSchema. Cada ejecución sustituye por completo los datos anteriores (mode("overwrite")); como todavía no existe historificación (prevista para la Fase 5), el esquema también puede cambiar libremente de una ejecución a otra sin que eso suponga pérdida de información relevante.


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
