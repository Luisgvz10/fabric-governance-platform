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

# ## Gold — `dim_date`
# 
# **Promoción directa.** Igual que `dim_tenant_setting` (5.3.5), `silver.calendar` ya está en su forma final — no hay ningún cruce que hacer. Se relaciona en el modelo semántico con `fact_capacity_metrics` por `start_of_hour`, para que el informe de histórico de capacidad muestre también las horas sin actividad registrada (5.6).

# CELL ********************

# PROMOCIÓN A GOLD

gold_dim_date = spark.table("silver.calendar")
gold_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_date")

print(f"gold.dim_date: {spark.table('gold.dim_date').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.dim_date ORDER BY start_of_hour LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
