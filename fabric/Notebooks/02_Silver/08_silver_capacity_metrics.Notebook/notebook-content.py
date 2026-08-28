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

# ## Silver — `capacity_metrics`
# 
# **Poco que limpiar aquí, a diferencia del resto de entidades.** `bronze.capacity_metrics` no viene de JSON crudo de una API REST — viene de una consulta DAX (5.3.9) cuyo resultado ya se renombró a snake_case y se tipó correctamente dentro del propio Bronze, para evitar arrastrar nombres como `Dates[Day]`/`[BackgroundPct]` por todo el proyecto. Es una desviación consciente del principio general de Bronze ("sin transformar"), justificada porque el origen no es JSON anidado sino ya una tabla tabular. Silver se limita, por tanto, a seleccionar y confirmar el esquema, sin `join` ni transformación adicional — mismo grano que Bronze, *(capacidad, fecha, hora)*.


# CELL ********************

# DEPENDENCIAS

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PROMOCIÓN A SILVER

bronze_capacity_metrics = spark.table("bronze.capacity_metrics")

silver_capacity_metrics = bronze_capacity_metrics.select(
    F.col("date"),
    F.col("start_of_hour"),
    F.col("capacity_id"),
    F.col("background_pct"),
    F.col("interactive_pct"),
)

silver_capacity_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.capacity_metrics")

print(f"silver.capacity_metrics: {spark.table('silver.capacity_metrics').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.capacity_metrics ORDER BY date, start_of_hour LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
