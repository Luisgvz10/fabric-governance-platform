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

# ## Gold — `fact_item_access`
# 
# **Grano.** Una fila por *(item, principal, permiso)* — el mismo que `silver.item_access`. No hay `join`: `item_id` ya enlaza con `gold.dim_inventory` y `principal_id` con `gold.dim_user`, así que el cruce queda para el momento de la consulta, no para esta tabla. Se mantienen `principal_display_name`, `principal_type` e `item_type` como columnas descriptivas sueltas —igual que `item_name` en `fact_activity`

# CELL ********************

# PROMOCIÓN A GOLD

silver_item_access = spark.table("silver.item_access")

gold_fact_item_access = silver_item_access


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN GOLD

gold_fact_item_access.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_item_access")

print(f"gold.fact_item_access: {spark.table('gold.fact_item_access').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.fact_item_access LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
