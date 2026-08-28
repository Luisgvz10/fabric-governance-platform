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

# ## Bronze — `item_access`
# 
# **Qué se extrae.** Se llama al endpoint de administración de Fabric `GET /admin/workspaces/{workspaceId}/items/{itemId}/users`, una vez por cada item ya conocido (`bronze.items`, de la tubería de inventario). A diferencia del endpoint de workspace, aquí el permiso no es un rol único: es una lista de permisos concretos (`Read`, `Write`, `Reshare`, `Explore`, `Execute`) por cada principal con acceso al item — y, como vimos al verificar la documentación, este nivel sí puede incluir usuarios individuales compartidos directamente, no solo grupos.
# 
# **Detalle de la API.** Para cinco tipos de item (`Report`, `Dashboard`, `SemanticModel`, `App`, `Dataflow`) la API exige indicar el tipo como parámetro (`?type=...`); para el resto es opcional. Se añade siempre que `bronze.items` lo tenga, sin distinguir casos.
# 
# **Manejo de errores.** Igual que con los workspaces personales/administrativos de ayer, no todos los items van a responder con éxito (la documentación menciona explícitamente `ItemNotFound` e `InvalidItemType` como casos esperables — por ejemplo, un `SQLEndpoint` autogenerado podría no ser un tipo válido para este endpoint). El notebook captura el error, imprime el detalle completo (item, tipo, código, cuerpo de la respuesta) y continúa — nada se descarta en silencio, para poder revisar después qué se saltó y por qué.


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
from requests.exceptions import HTTPError

fabric = FabricClient()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ITEMS CONOCIDOS: sobre estos se va a preguntar quién tiene acceso

bronze_items = spark.table("bronze.items")
items = [
    (row["id"], row["workspaceId"], row["type"])
    for row in bronze_items.select("id", "workspaceId", "type").collect()
]

TYPE_REQUIRED_AS_PARAM = {"Report", "Dashboard", "SemanticModel", "App", "Dataflow"}

print(f"{len(items)} items a consultar")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXTRACCIÓN: una llamada por item, se registra y se salta con detalle cualquier error

all_item_access = []
skipped_items = []

for item_id, workspace_id, item_type in items:
    endpoint = f"/admin/workspaces/{workspace_id}/items/{item_id}/users"
    if item_type in TYPE_REQUIRED_AS_PARAM:
        endpoint += f"?type={item_type}"

    try:
        response = fabric.get(endpoint)
    except HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        body = e.response.text if e.response is not None else ""
        skipped_items.append((item_id, item_type, status, body))
        print(f"Saltando item {item_id} ({item_type}): {status} — {body[:200]}")
        continue

    for entry in response.get("accessDetails", []):
        entry["itemId"] = item_id
        entry["workspaceId"] = workspace_id
        all_item_access.append(entry)

print(f"{len(all_item_access)} registros de acceso extraídos de {len(items)} items (saltados {len(skipped_items)})")


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

bronze_item_access = to_bronze_df(all_item_access)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN BRONZE: overwrite
bronze_item_access.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.item_access")

print(f"bronze.item_access: {spark.table('bronze.item_access').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.item_access LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
