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

# ## Gold — `dim_user`
# 
# **Alcance.** Dimensión completa de *actores* del proyecto, no solo de usuarios: personas, grupos y Service Principals con permisos concedidos (unificados por `graphId` en `silver.users`), más los actores de la propia plataforma que generan eventos de actividad sin tener ningún permiso asignado — la propia Power BI Service, por ejemplo, aparece como `UserId` en operaciones internas (`GetDatasourceDetails`, `CreateCheckpoint`) sin ser nunca principal de ningún `workspace_access` ni `item_access`. Sin esta cuarta fuente, `fact_activity.principal_id` quedaría sin resolver para esos eventos — y una FK con `NULL` es peor que una FK explícita hacia un principal de tipo `Service`: no se puede filtrar, agrupar ni distinguir "actividad interna de la plataforma" de "dato pendiente de investigar".
# 
# **Detección de actores de servicio.** Se comparan los valores distintos de `silver.activity.user_id` contra los `principal_id` y `email` ya presentes en `silver.users` (comparación insensible a mayúsculas, por si un GUID llega con distinto casing entre APIs). Los que no matchean ninguno de los dos se incorporan como filas nuevas con `principal_type = "Service"`, usando el propio valor bruto como `display_name` — no hay ninguna fuente que aporte un nombre más descriptivo, así que se prefiere mostrar el dato real (p. ej. `"PowerBI"`, o el GUID `00000009-0000-0000-c000-000000000000`) antes que inventar una etiqueta no verificada.
# 
# **Dependencia entre tuberías.** Este notebook necesita que `Silver/02_silver_activity` ya se haya ejecutado (para leer `silver.activity`), además de las tres fuentes de permisos habituales — una dependencia cruzada a tener en cuenta al diseñar la orquestación con Data Pipeline (pendiente).


# CELL ********************

# DEPENDENCIAS

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PROMOCIÓN A GOLD

silver_users = spark.table("silver.users")
gold_dim_user = silver_users

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ACTORES DE SERVICIO: valores de user_id en la actividad sin match en silver_users

known_ids = silver_users.select(F.lower(F.col("principal_id")).alias("key"))
known_emails = (
    silver_users
    .filter(F.col("email").isNotNull())
    .select(F.lower(F.col("email")).alias("key"))
)
known_keys = known_ids.unionByName(known_emails).distinct()

service_actors = (
    spark.table("silver.activity")
    .select(F.col("user_id").alias("principal_id"))
    .distinct()
    .withColumn("key", F.lower(F.col("principal_id")))
    .join(known_keys, on="key", how="left_anti")
    .select(
        F.col("principal_id"),
        F.col("principal_id").alias("display_name"),
        F.lit("Service").alias("principal_type"),
        F.lit(None).cast("string").alias("email"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN GOLD

gold_dim_user = silver_users.unionByName(service_actors)

gold_dim_user.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_user")

print(f"gold.dim_user: {spark.table('gold.dim_user').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.dim_user LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
