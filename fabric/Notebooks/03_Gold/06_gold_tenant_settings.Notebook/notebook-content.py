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

# ## Gold — `dim_tenant_setting`
# 
# **Sí lleva prefijo `dim_`, aunque hoy no la referencie ninguna fact table.** Es una tabla de configuración/referencia — una fotografía de los ajustes activos del tenant —, pero encaja como dimensión: las futuras tablas de overrides delegados (capacidad/dominio/workspace, memoria 8.5) la referenciarían por `setting_name`, igual que `fact_workspace_access`/`fact_item_access` referencian `dim_user`. Se mantiene la convención `dim_`/`fact_` del resto del Gold en vez de crear una excepción de nomenclatura para una sola tabla.
# 
# **Promoción directa.** Igual que las demás entidades de permisos (5.3.4), no hay ningún cruce que hacer aquí: `silver.tenant_settings` ya está en su forma final.


# CELL ********************

# ESCRITURA EN GOLD

gold_dim_tenant_setting = spark.table("silver.tenant_settings")
gold_dim_tenant_setting.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_tenant_setting")

print(f"gold.dim_tenant_setting: {spark.table('gold.dim_tenant_setting').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.dim_tenant_setting LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
