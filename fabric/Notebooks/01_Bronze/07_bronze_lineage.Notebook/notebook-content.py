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

# ## Bronze — `workspace_scan` y `datasource_instances` (Scanner API)
# 
# **Qué se extrae.** El Scanner API de Power BI (`PostWorkspaceInfo` / `GetScanStatus` / `GetScanResult`) — la fuente de linaje real entre activos: qué dataset depende de qué dataflow, qué fuente de datos usa cada uno. A diferencia del resto de entidades Bronze, no es una sola llamada: es un flujo asíncrono de tres pasos (lanzar el scan, sondear su estado, recoger el resultado una vez termina).
# 
# **Workspaces reutilizados de `bronze.workspaces`.** El scan pide una lista explícita de IDs de workspace (hasta 100 por llamada); en vez de volver a listarlos a mano, se reutilizan los que ya extrajo `00_bronze_inventory` (Fase 3.2).
# 
# **`lineage=true`, `datasourceDetails=true`, sin `datasetSchema`/`datasetExpressions`.** Estos dos últimos exigen tener habilitado el escaneo completo de metadatos a nivel tenant, y dan esquema de tablas/columnas/medidas — un catálogo de datos, no linaje. Se dejan fuera por no ser necesarios para esta entidad.
# 
# **Dos tablas, no una.** La respuesta trae una lista de workspaces con items anidados (`reports`, `dashboards`, `datasets`, `dataflows`, `datamarts`, cada uno con su propio `upstreamDataflows`/`datasourceUsages`) y, aparte, una lista `datasourceInstances` a nivel de todo el scan, no de un workspace concreto — grano distinto, tabla distinta.
# 
# **Sin diseño de Silver todavía.** No se sabe de antemano cuánto de este contenido aparece poblado en los workspaces reales del proyecto. Bronze se limita a capturar la respuesta real, tal cual; Silver se diseña después de ver qué contiene de verdad.
# 07_bronze_lineage


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
import time

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

# LANZAR EL SCAN

workspace_ids = [row["id"] for row in spark.table("bronze.workspaces").select("id").collect()]

scan_request = powerbi.post(
    "/admin/workspaces/getInfo?lineage=True&datasourceDetails=True",
    {"workspaces": workspace_ids}
)
scan_id = scan_request["id"]

print(f"Scan lanzado sobre {len(workspace_ids)} workspaces (scanId: {scan_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SONDEAR EL ESTADO

scan_status = "NotStarted"
attempts = 0
max_attempts = 30

while scan_status not in ("Succeeded", "Failed") and attempts < max_attempts:
    time.sleep(5)
    scan_status = powerbi.get(f"/admin/workspaces/scanStatus/{scan_id}")["status"]
    attempts += 1

if scan_status != "Succeeded":
    raise Exception(f"El scan no terminó correctamente: estado final '{scan_status}' tras {attempts} sondeos")

print(f"Scan completado: {scan_status} ({attempts} sondeos)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RECOGER EL RESULTADO

scan_result = powerbi.get(f"/admin/workspaces/scanResult/{scan_id}")

workspace_scan = scan_result.get("workspaces", [])
datasource_instances = scan_result.get("datasourceInstances", [])

print(f"{len(workspace_scan)} workspaces escaneados, {len(datasource_instances)} datasource instances")


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

bronze_workspace_scan = to_bronze_df(workspace_scan)
bronze_datasource_instances = to_bronze_df(datasource_instances)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN BRONZE: overwrite

bronze_workspace_scan.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.workspace_scan")
bronze_datasource_instances.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.datasource_instances")

print(f"bronze.workspace_scan: {spark.table('bronze.workspace_scan').count()} filas")
print(f"bronze.datasource_instances: {spark.table('bronze.datasource_instances').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.workspace_scan LIMIT 1000")
display(df)

df = spark.sql("SELECT * FROM lh_metadata.bronze.datasource_instances LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
