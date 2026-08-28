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
# META     },
# META     "environment": {
# META       "environmentId": "c50642e1-ed2e-98df-4121-a4d7e3b1b98e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze — `activity`
# 
# **Qué se extrae.** El registro de auditoría de Activity Events (`/admin/activityevents`, Power BI), recorrido día a día — la API exige que `startDateTime`/`endDateTime` caigan en el mismo día UTC, así que no se puede pedir la ventana completa de 28 días en una sola llamada.
# 
# **Por qué `MERGE` y no `overwrite`.** La API solo conserva 28 días de actividad; con `overwrite` se perdería en cada ejecución lo que hubiera caducado desde la anterior. `bronze.activity` se actualiza por `MERGE` sobre `Id` (único e inmutable), acumulando histórico indefinidamente. Silver y Gold no necesitan este tratamiento — se reconstruyen enteras desde Bronze en cada ejecución.


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
from datetime import datetime, timedelta, timezone
from delta.tables import DeltaTable

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

def get_activity_events_for_day(day):
    endpoint = f"/admin/activityevents?startDateTime='{day.isoformat()}T00:00:00'&endDateTime='{day.isoformat()}T23:59:59'"
    events = []
    response = powerbi.get(endpoint)
    events.extend(response.get("activityEventEntities", []))
    next_uri = response.get("continuationUri")
    while next_uri:
        resp = requests.get(next_uri, headers=powerbi.headers)
        resp.raise_for_status()
        response = resp.json()
        events.extend(response.get("activityEventEntities", []))
        next_uri = response.get("continuationUri")
    return events

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from requests.exceptions import HTTPError

end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days=27)

all_events = []
current = start_date

while current <= end_date:
    try:
        # Intenta obtener los eventos de actividad para el día actual
        day_events = get_activity_events_for_day(current)
        all_events.extend(day_events)
    except HTTPError as e:
        # Captura específicamente errores HTTP 400 y registra información para depuración
        print(f"Error HTTP al obtener eventos para el día {current}: {e}")
        # Opcional: puedes decidir si romper el bucle o continuar con el siguiente día
        # break  # Descomentar si quieres detener la ejecución ante el primer error
    
    # Avanza al siguiente día
    current += timedelta(days=1)

print(f"{len(all_events)} eventos extraídos ({start_date} a {end_date})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

def to_bronze_df(records):
    json_lines = [json.dumps(r) for r in records]
    rdd = spark.sparkContext.parallelize(json_lines)
    return spark.read.json(rdd)

new_activity = to_bronze_df(all_events)

if spark.catalog.tableExists("bronze.activity"):
    delta_table = DeltaTable.forName(spark, "bronze.activity")
    delta_table.alias("target").merge(
        new_activity.alias("source"),
        "target.Id = source.Id"
    ).whenNotMatchedInsertAll().execute()
else:
    new_activity.write.format("delta").saveAsTable("bronze.activity")

print(f"bronze.activity: {spark.table('bronze.activity').count()} filas totales")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

OPTIMIZE bronze.activity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
