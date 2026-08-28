# Fabric notebook source


# MARKDOWN ********************

# ## Gold — `dim_date`
# 
# **Promoción directa.** Igual que `dim_tenant_setting` (5.3.5), `silver.calendar` ya está en su forma final — no hay ningún cruce que hacer. Se relaciona en el modelo semántico con `fact_capacity_metrics` por `start_of_hour`, para que el informe de histórico de capacidad muestre también las horas sin actividad registrada (5.6).

# CELL ********************

# PROMOCIÓN A GOLD

gold_dim_date = spark.table("silver.calendar")
gold_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_date")

print(f"gold.dim_date: {spark.table('gold.dim_date').count()} filas")

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.dim_date ORDER BY start_of_hour LIMIT 1000")
display(df)
