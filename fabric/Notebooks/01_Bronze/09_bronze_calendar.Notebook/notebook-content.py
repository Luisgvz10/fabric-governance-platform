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

# ## Bronze — `calendar`
# 
# **Qué hace.** A diferencia del resto de entidades Bronze, no hay ninguna API que llamar aquí: se genera una secuencia horaria determinista de un año hacia atrás desde hoy, con reloj propio (`datetime.now()`), sin depender de los datos de ninguna otra tabla del proyecto — precisamente para no heredar los huecos de la fuente que viene a resolver (`capacity_metrics`, ver memoria 5.6).
# 
# **Por qué `overwrite` y no `MERGE`.** No hay nada que historificar: el rango se recalcula entero en cada ejecución y siempre cubre el último año completo hasta hoy, deslizándose hacia adelante solo. Es la misma razón por la que `bronze.workspaces`/`bronze.items` usan `overwrite` — es una fotografía del estado actual, no un evento que deba conservarse indefinidamente.

# CELL ********************

from datetime import datetime, timedelta, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GENERACIÓN DEL RANGO: último año hasta hoy, grano de una hora

end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days=365)

bronze_calendar = spark.sql(f"""
    SELECT explode(sequence(
        timestamp('{start_date} 00:00:00'),
        timestamp('{end_date} 23:00:00'),
        interval 1 hour
    )) AS start_of_hour
""")

bronze_calendar.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.calendar")

print(f"bronze.calendar: {spark.table('bronze.calendar').count()} filas ({start_date} a {end_date})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.calendar ORDER BY start_of_hour LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
