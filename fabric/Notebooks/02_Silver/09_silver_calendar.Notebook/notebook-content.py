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

# ## Silver — `calendar`
# 
# **Qué hace.** Deriva de `start_of_hour` los atributos de calendario habituales — fecha, hora, día de la semana, fin de semana — igual que el resto de Silver: selección y tipado, sin lógica de negocio ni cruces con otras tablas.

# CELL ********************

from pyspark.sql import functions as F

bronze_calendar = spark.table("bronze.calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DERIVACIÓN DE ATRIBUTOS DE CALENDARIO

silver_calendar = (
    bronze_calendar
    .withColumn("date", F.to_date("start_of_hour"))
    .withColumn("hour", F.hour("start_of_hour"))
    .withColumn("day_of_week", F.date_format("start_of_hour", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("start_of_hour").isin(1, 7))
    .select("start_of_hour", "date", "hour", "day_of_week", "is_weekend")
)

silver_calendar.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.calendar")

print(f"silver.calendar: {spark.table('silver.calendar').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.calendar ORDER BY start_of_hour LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
