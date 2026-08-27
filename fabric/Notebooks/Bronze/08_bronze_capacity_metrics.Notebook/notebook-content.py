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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze — `capacity_metrics`
# 
# **Qué se extrae.** El % de CU facturable (background/interactivo) por hora, desde el semantic model que instala la Microsoft Fabric Capacity Metrics App — la única fuente de este dato, no expuesta por ninguna API REST de administración plana.
# 
# **Requiere que el workspace de la app esté en una capacidad Fabric/Premium.** Las tablas con datos reales de consumo (`Timepoints`, `Metrics By...`) son `DirectQuery` contra un conector interno de Microsoft (`CapacityMetricsCES`); consultarlas vía API (XMLA o Execute Queries) exige que el workspace que aloja el semantic model esté asignado a una capacidad Fabric/Premium con el endpoint XMLA habilitado. Por defecto, el workspace que crea la app al instalarse no tiene ninguna capacidad asignada — hubo que asignarlo manualmente a `fabricgov`. Sin eso, cualquier consulta a esas tablas falla con `The credentials provided for the CapacityMetricsCES source are invalid`, aunque el informe nativo de la app sí cargue datos (usa el motor de renderizado propio de Microsoft, no una consulta DAX externa). Las tablas `Import` del mismo modelo (`Capacities`, `Items`...) no tienen esta restricción.
# 
# **Identidad de ejecución distinta al resto del proyecto.** A diferencia de todos los demás notebooks Bronze, este no usa `FabricClient`/el Service Principal — usa `sempy.fabric`, que se autentica con la identidad de quien ejecuta el notebook interactivamente. El Service Principal no tiene acceso a este workspace (lo crea la app, no nuestro aprovisionamiento).
# 
# **Ventana de datos limitada, igual que actividad.** La app solo expone un margen corto de días recientes; se extraen los últimos 14 días (excluyendo hoy, con datos todavía incompletos) y se historifica en Bronze vía `MERGE` por *(fecha, hora)* — mismo patrón que `bronze.activity` (5.3.3), por el mismo motivo: cada ejecución solo ve una ventana móvil, no todo el histórico.


# CELL ********************

# DEPENDENCIAS

import sempy.fabric as sempy_fabric
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# LOCALIZAR EL SEMANTIC MODEL Y LA CAPACIDAD 

metrics_item = (
    spark.table("bronze.items")
    .filter((F.col("name") == "Fabric Capacity Metrics") & (F.col("type") == "SemanticModel"))
    .select("id", "workspaceId")
    .collect()
)

if not metrics_item:
    raise Exception("No se encontró 'Fabric Capacity Metrics' en bronze.items -- ejecuta antes 01_bronze_inventory.")

dataset_id = metrics_item[0]["id"]
workspace_id = metrics_item[0]["workspaceId"]

capacity_id = (
    spark.table("bronze.capacities")
    .filter(F.col("displayName") == "fabricgov")
    .select("id")
    .collect()[0]["id"]
)

print(f"dataset_id={dataset_id}, workspace_id={workspace_id}, capacity_id={capacity_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CONSULTA DAX: % de CU background/interactivo por hora, últimos 14 días (excluyendo hoy)

dax_query = f"""
DEFINE
MPARAMETER 'CapacitiesList' = "{capacity_id}"

VAR __DateFilter =
FILTER(
    VALUES('Dates'[Day]),
    'Dates'[Day] >= UTCTODAY() - 14 && 'Dates'[Day] <= UTCTODAY() - 1
)

VAR __Core =
SUMMARIZECOLUMNS(
    'Dates'[Day],
    'Timepoints'[Start of hour],
    __DateFilter,
    "BackgroundPct", [Background billable CU %],
    "InteractivePct", [Interactive billable CU %]
)

EVALUATE __Core
ORDER BY 'Dates'[Day], 'Timepoints'[Start of hour]
"""

dax_result = sempy_fabric.evaluate_dax(
    workspace=workspace_id,
    dataset=dataset_id,
    dax_string=dax_query
)

print(f"{len(dax_result)} filas extraídas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# JSON/PANDAS -> DATAFRAME SPARK

bronze_capacity_metrics = (
    spark.createDataFrame(dax_result)
    .withColumnRenamed("Dates[Day]", "date")
    .withColumnRenamed("Timepoints[Start of hour]", "start_of_hour")
    .withColumnRenamed("[BackgroundPct]", "background_pct")
    .withColumnRenamed("[InteractivePct]", "interactive_pct")
    .withColumn("date", F.col("date").cast("date"))
    .withColumn("start_of_hour", F.col("start_of_hour").cast("timestamp"))
    .withColumn("capacity_id", F.lit(capacity_id))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# HISTORIFICACIÓN MEDIANTE MERGE: por (date, start_of_hour), mismo patrón que bronze.activity

if spark.catalog.tableExists("bronze.capacity_metrics"):
    delta_table = DeltaTable.forName(spark, "bronze.capacity_metrics")
    delta_table.alias("target").merge(
        bronze_capacity_metrics.alias("source"),
        "target.date = source.date AND target.start_of_hour = source.start_of_hour"
    ).whenNotMatchedInsertAll().execute()
else:
    bronze_capacity_metrics.write.format("delta").saveAsTable("bronze.capacity_metrics")

print(f"bronze.capacity_metrics: {spark.table('bronze.capacity_metrics').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.capacity_metrics LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
