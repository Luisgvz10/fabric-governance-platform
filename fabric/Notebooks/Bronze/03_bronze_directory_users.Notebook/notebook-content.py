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

# ## Bronze — `directory_users` (Microsoft Graph)
# 
# **Qué se extrae.** El directorio completo de usuarios del tenant (`GET /users`), vía Microsoft Graph — la única fuente que expone identidad de persona individual con su `graphId`, ya que ni Fabric ni Power BI ofrecen un listado de usuarios independiente de los permisos concedidos (ver 5.3.4 de la memoria). No se extrae membresía de grupos aquí — esa pregunta (quién hay dentro de `sg-fabric-admins`) queda aparcada como ampliación futura, sin relación con lo que este notebook resuelve.
# 
# **Para qué sirve.** Es la tercera y última fuente de `dim_user`: junto a `bronze.workspace_access` y `bronze.item_access` (que ya dan `Group` y `App`), esta tabla aporta las filas de tipo `User` que ninguna de las otras dos puede dar, porque el proyecto nunca concede acceso a una persona de forma directa.
# 
# **Paginación.** Microsoft Graph pagina con `@odata.nextLink` (la URL completa de la siguiente página), a diferencia de `continuationToken`/`continuationUri` en Fabric/Power BI.


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
import requests

graph = FabricClient(
    scope="https://graph.microsoft.com/.default",
    base_url="https://graph.microsoft.com/v1.0"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PAGINACIÓN GENÉRICA PARA GRAPH

def get_all_graph(endpoint):
    results = []
    url = endpoint
    while url:
        if url.startswith("http"):
            response = requests.get(url, headers=graph.headers).json()
        else:
            response = graph.get(url)
        results.extend(response.get("value", []))
        url = response.get("@odata.nextLink")
    return results


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXTRACCIÓN: directorio completo de usuarios

users = get_all_graph("/users?$select=id,displayName,mail,userPrincipalName,userType")
print(f"{len(users)} usuarios extraídos del directorio")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# JSON CRUDO -> DATAFRAME

def to_bronze_df(records):
    json_lines = [json.dumps(r) for r in records]
    rdd = spark.sparkContext.parallelize(json_lines)
    return spark.read.json(rdd)

bronze_directory_users = to_bronze_df(users)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN BRONZE: overwrite

bronze_directory_users.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.directory_users")

print(f"bronze.directory_users: {spark.table('bronze.directory_users').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.directory_users LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
