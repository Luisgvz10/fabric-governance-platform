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

# ## Silver — `tenant_settings`
# 
# **Una sola tabla, deliberadamente simple.** `bronze.tenant_settings` trae campos anidados (`enabledSecurityGroups`, `excludedSecurityGroups`) que solo tienen contenido en un puñado de los 170 ajustes — a diferencia del modelo de permisos (5.3.4), aquí ese detalle no es el centro de la entidad, así que no se explota en una tabla aparte: se mantiene `can_specify_security_groups` (si el ajuste admite acotarse a grupos) sin bajar al detalle de qué grupo concreto.
# 
# **Columnas de delegación.** `delegate_to_capacity`/`delegate_to_domain`/`delegate_to_workspace` indican, por ajuste, si un admin de nivel inferior (capacidad, dominio o workspace) podría sobrescribir el valor del tenant para su propio ámbito. Se conservan aunque en la práctica salgan `NULL` en la mayoría de los 170 ajustes: documentan la *posibilidad* de delegación de cada ajuste, no si se ha ejercido — coherente con que este tenant no delega nada hoy (una única capacidad, sin dominios ni overrides configurados, ver memoria 8.5). El detalle de los overrides reales vive en tres APIs distintas, no en esta tabla — se documentan como ampliación futura.


# CELL ********************

# DEPENDENCIAS Y CLIENTES

from pyspark.sql import functions as F

bronze_tenant_settings = spark.table("bronze.tenant_settings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TENANT_SETTINGS: grano de un ajuste por fila

silver_tenant_settings = (
    bronze_tenant_settings
    .select(
        F.col("settingName").alias("setting_name"),
        F.col("title").alias("title"),
        F.col("tenantSettingGroup").alias("tenant_setting_group"),
        F.col("enabled").alias("enabled"),
        F.col("canSpecifySecurityGroups").alias("can_specify_security_groups"),
        F.col("delegateToCapacity").alias("delegate_to_capacity"),
        F.col("delegateToDomain").alias("delegate_to_domain"),
        F.col("delegateToWorkspace").alias("delegate_to_workspace"),
    )
)

silver_tenant_settings.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.tenant_settings")

print(f"silver.tenant_settings: {spark.table('silver.tenant_settings').count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.tenant_settings LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
