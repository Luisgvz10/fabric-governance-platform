# Fabric notebook source


# MARKDOWN ********************

# ## Silver — `calendar`
# 
# **Qué hace.** Deriva de `start_of_hour` los atributos de calendario habituales — fecha, hora, día de la semana, fin de semana — igual que el resto de Silver: selección y tipado, sin lógica de negocio ni cruces con otras tablas.

# CELL ********************

from pyspark.sql import functions as F

bronze_calendar = spark.table("bronze.calendar")

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

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.calendar ORDER BY start_of_hour LIMIT 1000")
display(df)
