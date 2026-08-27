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
# **Resolución de `principal_id` contra `dim_user`.** El campo `UserId` de Activity Events no es una clave estable: según el evento llega como UPN (`luisucm@...`), como GUID (que ya coincide con el `graphId` de `dim_user`), o como la cadena `"PowerBI"` cuando el evento lo genera el propio servicio y no hay un principal real detrás. Se construye una tabla de lookup a partir de `gold.dim_user` con dos claves apuntando al mismo `principal_id` — el propio `principal_id` y el `email` en minúsculas — y se cruza contra `silver.activity` normalizando `UserId` también a minúsculas. Los eventos sin match (como los de `"PowerBI"`) quedan con `principal_id = NULL`: es el resultado correcto, no un dato perdido, ya que inventar una FK sin un principal real detrás repetiría el mismo error de "ocultar en vez de resolver" que ya se evitó en la extracción de actividad (Bronze). El valor original se conserva sin traducir en `user_id_raw` para no perder trazabilidad.
# 
# **Por qué la unión va en Gold y no en Silver.** Es un cruce entre entidades distintas (actividad y usuarios), y ese tipo de lógica vive en Gold — mismo criterio ya aplicado en `dim_inventory`. `item_name` y `workspace_name` siguen como columnas descriptivas sueltas: todavía no existe una dimensión de item/workspace propia con la que enlazarlas.
# 


# CELL ********************

# DEPENDENCIAS

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RESOLUCIÓN DE PRINCIPAL_ID CONTRA DIM_USER

dim_user = spark.table("gold.dim_user")

lookup_by_id = dim_user.select(
    F.lower(F.col("principal_id")).alias("key"),
    F.col("principal_id").alias("resolved_principal_id"),
)

lookup_by_email = (
    dim_user.filter(F.col("email").isNotNull())
    .select(F.lower("email").alias("key"), F.col("principal_id").alias("resolved_principal_id"))
)
lookup = lookup_by_id.union(lookup_by_email)

# ESCRITURA EN GOLD

gold_fact_activity = (
    spark.table("silver.activity")
    .withColumnRenamed("user_id", "user_id_raw") 
    .join(lookup, F.lower(F.col("user_id_raw")) == F.col("key"), "left")
    .withColumnRenamed("resolved_principal_id", "principal_id")
    .drop("key")
)

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
