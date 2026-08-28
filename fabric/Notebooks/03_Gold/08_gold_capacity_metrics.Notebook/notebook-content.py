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

# ## Gold — `fact_capacity_metrics`
# 
# **Fact table.** Grano *(capacidad, fecha, hora)*, con dos medidas reales: `background_pct` e `interactive_pct` (% de CU facturable). No hay `join`: `capacity_id` queda como referencia suelta — no existe una `dim_capacity` propia en el proyecto (la información de capacidad vive embebida por item dentro de `dim_inventory`, no como dimensión independiente), así que resolver el nombre de la capacidad se deja para el momento de la consulta, mismo criterio que el resto de fact tables del proyecto.
# 
# **Valores por encima de 100%.** No es un error: Fabric permite consumo puntual superior al 100% de la capacidad contratada mediante *smoothing* (toma prestado de capacidad futura antes de aplicar *throttling*) — con una capacidad F2 tan pequeña, se observa en la práctica en los propios datos.


# CELL ********************

# ESCRITURA EN GOLD

gold_fact_capacity_metrics = spark.table("silver.capacity_metrics")
gold_fact_capacity_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_capacity_metrics")

print(f"gold.fact_capacity_metrics: {spark.table('gold.fact_capacity_metrics').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.fact_capacity_metrics LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
