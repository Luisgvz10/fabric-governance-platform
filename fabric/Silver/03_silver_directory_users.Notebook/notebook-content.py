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

# ## Silver — `users` (dim_user unificado)
# 
# **Qué hace.** Combina las tres fuentes de identidad del proyecto en una sola tabla, una fila por *principal* (`User`/`Group`/`App`), usando `graphId` como clave común a las tres: `bronze.workspace_access` y `bronze.item_access` aportan `Group` y `App` (los únicos tipos con acceso directo concedido); `bronze.directory_users` aporta `User`, el tipo que ninguna de las otras dos fuentes puede dar.
# 
# **Normalización necesaria.** La API de Power BI (`workspace_access`) llama `"App"` al Service Principal; la API de Fabric (`item_access`) lo llama `"ServicePrincipal"` — mismo concepto, dos nombres. Se normaliza a `"App"` antes de deduplicar, para que `app-fabric-governance` no aparezca como dos tipos distintos según de qué fuente venga cada fila.
# 
# **Deduplicación segura.** Un mismo principal (el SP, por ejemplo) puede venir de varias fuentes a la vez. En vez de `dropDuplicates()` (que se queda con una fila al azar y podría descartar un dato válido de otra), se agrupa por `principal_id` y se toma el primer valor no nulo de cada columna.
# 


# CELL ********************

# DEPENDENCIAS Y CLIENTES

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FUENTE 1: Group/App desde permisos de workspace (API de Power BI)

# `identifier` es un campo plano, siempre presente: email real si es User, GUID si es App

principals_from_workspace = (
    spark.table("bronze.workspace_access")
    .select(
        F.col("graphId").alias("principal_id"),
        F.col("displayName").alias("display_name"),
        F.col("principalType").alias("principal_type"),
        F.when(F.col("principalType") == "User", F.col("identifier")).alias("email"),
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FUENTE 2: Group/App/User desde permisos de item (API de Fabric)

# Normalizamos "ServicePrincipal" -> "App"; userDetails.userPrincipalName solo existe para User

principals_from_items = (
    spark.table("bronze.item_access")
    .select(
        F.col("principal.id").alias("principal_id"),
        F.col("principal.displayName").alias("display_name"),
        F.when(F.col("principal.type") == "ServicePrincipal", "App")
         .otherwise(F.col("principal.type")).alias("principal_type"),
        F.when(
            F.col("principal.type") == "User",
            F.col("principal.userDetails.userPrincipalName")
        ).alias("email"),
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FUENTE 3: usuarios reales desde el directorio de Microsoft Graph

principals_from_directory = (
    spark.table("bronze.directory_users")
    .select(
        F.col("id").alias("principal_id"),
        F.col("displayName").alias("display_name"),
        F.lit("User").alias("principal_type"),
        F.coalesce(F.col("mail"), F.col("userPrincipalName")).alias("email"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UNIÓN Y DEDUPLICACIÓN SEGURA: por principal_id, quedándose con el primer valor no nulo de cada columna

silver_users = (
    principals_from_workspace
    .unionByName(principals_from_items)
    .unionByName(principals_from_directory)
    .groupBy("principal_id")
    .agg(
        F.first("display_name", ignorenulls=True).alias("display_name"),
        F.first("principal_type", ignorenulls=True).alias("principal_type"),
        F.first("email", ignorenulls=True).alias("email"),
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN SILVER

silver_users.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.users")

print(f"silver.users: {spark.table('silver.users').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.users LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
