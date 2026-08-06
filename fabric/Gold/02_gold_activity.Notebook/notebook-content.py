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

# ## Gold — `fact_activity`
# 
# **Por qué no hay unión en este paso.** A diferencia de `dim_inventory`, que combina tres tablas Silver (`items`, `workspaces`, `capacities`), `fact_activity` no tiene todavía otras dimensiones Gold con las que cruzarse: `dim_user` y una dimensión de item/workspace están planificadas para fases posteriores y aún no existen. Por eso esta capa se limita a promover `silver.activity` a `gold.fact_activity` tal cual, sin transformación adicional — marca la tabla como lista para consumo (por ejemplo, desde Power BI) sin acoplar los informes directamente a Silver. Cuando existan las dimensiones pendientes, este notebook se ampliará para sustituir los campos descriptivos (`user_id`, `item_name`, `workspace_name`) por sus claves foráneas correspondientes.


# CELL ********************

# ESCRITURA EN GOLD

gold_fact_activity = spark.table("silver.activity")

gold_fact_activity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_activity")

print(f"gold.fact_activity: {spark.table('gold.fact_activity').count()} filas")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.fact_activity LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
