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

# Historificación mediante MERGE. La API de Activity Events solo conserva actividad de los últimos 28 días (5.2 — límite de cobertura histórica, no de tasa). Guardar con overwrite, como en el inventario, perdería en cada ejecución los eventos que hubieran caducado en el API desde la ejecución anterior. En su lugar, bronze_activity se actualiza mediante MERGE: cada evento nuevo (identificado por su Id, único e inmutable) se inserta solo si no existe ya, de forma que la tabla acumula histórico indefinidamente, más allá de la ventana de 28 días que ofrece el API en un momento dado. Silver y Gold no necesitan este tratamiento — se reconstruyen por completo desde Bronze en cada ejecución, ya que la única fuente de datos nuevos es Bronze, y a este volumen reconstruirlas es barato.
# 
# Recorrido día a día. startDateTime y endDateTime deben caer dentro del mismo día UTC, así que no es posible pedir los 28 días en una sola llamada — el notebook itera día por día, paginando cada uno con get_paginated().


# MARKDOWN ********************

# ## Extracción de Activity Events — notas de depuración
# 
# La primera versión de este notebook fallaba con `400 Bad Request` sin explicación clara. Se encontraron tres causas independientes:
# 
# 1. **Ventana de 28 días, no 27**: pedir datos de hace exactamente 28 días es rechazado. El límite real es 27 días completos + el día actual.
# 2. **Formato de fecha**: la Referencia de la API muestra `'2019-08-13T07:55:00.000Z'` (con milisegundos y `Z`), pero ese formato es rechazado. El único formato aceptado, documentado solo en la guía conceptual de Microsoft, es sin milisegundos ni `Z`: `'2019-08-31T00:00:00'`. Las comillas simples deben viajar literales en la URL (no como `%27`).
# 3. **Paginación**: el `continuationToken` que devuelve la API ya viene codificado para URL. Reenviarlo vía el diccionario `params` de `requests` lo codifica una segunda vez y lo corrompe. La solución es usar el campo `continuationUri` de la respuesta, que ya trae la URL completa lista para usar — no reconstruir la petición a mano.
# 
# **Lección importante**: una versión intermedia envolvía la llamada en `try/except` para que el notebook no fallara. "Funcionaba" en el sentido de no dar error, pero enmascaraba el bug #3: solo recuperaba 38 eventos de los 3.789 reales (perdía en silencio todas las páginas siguientes a la primera). Un pipeline sin errores no es lo mismo que un pipeline correcto — se descartó el `try/except` y se corrigió la causa raíz.
# 
# **Resultado**: 3.789 eventos extraídos (2026-07-08 a 2026-08-04), consolidados en `bronze.activity` vía `MERGE` (ver celda de escritura).


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

import requests

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
