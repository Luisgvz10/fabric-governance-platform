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

# ## Bronze — `workspace_access`
# 
# **Qué se extrae.** Se llama al endpoint de administración de Power BI `GET /admin/groups/{workspaceId}/users`, una vez por cada workspace ya conocido (`bronze.workspaces`, de la tubería de inventario). Devuelve, por workspace, la lista de *principals* (usuarios, grupos o service principals) con acceso directo, junto con su rol (`Admin`/`Member`/`Contributor`/`Viewer`).
# 
# **Por qué esta tabla sirve para dos cosas.** Cada fila es, a la vez, un dato de identidad (quién es el principal) y un dato de permiso (qué rol tiene en qué workspace). No se puede separar en el origen: la API no ofrece un listado de usuarios aparte de sus permisos. Por eso esta misma tabla Bronze alimentará más adelante tanto `dim_user` (colapsando a un principal por fila) como `fact_workspace_access` (conservando el grano completo *workspace × principal*).
# 
# **Por qué se salta con seguridad algunos workspaces.** No todos los IDs de `bronze.workspaces` son "workspaces de grupo" válidos para este endpoint: el workspace `Admin Monitoring` (tipo `AdminWorkspace`) y los workspaces personales (`My Workspace`) devuelven `404 Not Found`, porque no son ese tipo de recurso. El notebook captura específicamente ese código (404) y continúa con el resto, sin ocultar ningún otro tipo de error — si la API devolviera un 401 o un 500, el notebook fallaría de verdad, como debe ser.


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


# --- 1. Workspaces conocidos: sobre estos se va a preguntar quién tiene acceso 
bronze_workspaces = spark.table("bronze.workspaces")
workspace_ids = [row["id"] for row in bronze_workspaces.select("id").distinct().collect()]

# --- 2. Una llamada por workspace; se salta con seguridad si la API dice 404 
all_access = []
skipped_workspaces = []

for workspace_id in workspace_ids:
    try:
        response = powerbi.get(f"/admin/groups/{workspace_id}/users")
    except HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            skipped_workspaces.append(workspace_id)
            print(f"Saltando workspace {workspace_id}: 404 Not Found en la API de Power BI")
            continue
        raise  # cualquier otro error se deja visible, no se oculta

    for entry in response.get("value", []):
        entry["workspaceId"] = workspace_id
        all_access.append(entry)

print(f"{len(all_access)} registros de acceso extraídos de {len(workspace_ids)} workspaces (saltados {len(skipped_workspaces)})")

# --- 3. JSON crudo -> DataFrame, con el mismo patrón usado en inventario y actividad 
def to_bronze_df(records):
    json_lines = [json.dumps(r) for r in records]
    rdd = spark.sparkContext.parallelize(json_lines)
    return spark.read.json(rdd)

bronze_users = to_bronze_df(all_access)

# --- 4. Escritura en Bronze: overwrite
bronze_users.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.workspace_access")

print(f"bronze.workspace_access: {spark.table('bronze.workspace_access').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.workspace_access LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
