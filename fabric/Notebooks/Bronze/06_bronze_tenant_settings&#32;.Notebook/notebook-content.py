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

# ## Bronze — `tenant_settings` (Power BI Admin API)
# 
# **Qué se extrae.** El inventario completo de ajustes a nivel de tenant (`GET /admin/tenantsettings`), agrupados por categoría (`tenantSettingGroup`) — export y compartición, ajustes de desarrollador, workspaces, IA, etc. Sustituye la Tabla 4 de la memoria (4 ajustes documentados a mano sobre Service Principals) por el inventario real y completo: cada ajuste indica si está activado (`enabled`) y, cuando aplica, a qué grupos de seguridad está acotado (`enabledSecurityGroups`).
# 
# **Sin paginación.** A diferencia de `/admin/workspaces` o `/admin/items`, este endpoint no pagina — devuelve la lista completa de ajustes en una sola llamada, bajo la clave `tenantSettings`.
# 
# **Solo del tenant, no de la capacidad.** Esta API cubre ajustes de administración a nivel de todo el tenant. No incluye ajustes propios de la capacidad F2 (autoescalado, pausa/reanudación, tamaño de SKU) — esos viven en Azure Resource Manager, fuera del alcance de esta API y, por ahora, de este proyecto (ver memoria 8.5).


# CELL ********************

%run shared_fabric_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run shared_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DEPENDENCIAS Y CLIENTES

import json

fabric = FabricClient()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# EXTRACCIÓN

tenant_settings = fabric.get_paginated("/admin/tenantsettings", "value")

print(f"{len(tenant_settings)} ajustes de tenant extraídos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# JSON CRUDO -> DATAFRAME

def to_bronze_df(records):
    json_lines = [json.dumps(r) for r in records]
    rdd = spark.sparkContext.parallelize(json_lines)
    return spark.read.json(rdd)

bronze_tenant_settings = to_bronze_df(tenant_settings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ESCRITURA EN BRONZE

bronze_tenant_settings.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.tenant_settings")

print(f"bronze.tenant_settings: {spark.table('bronze.tenant_settings').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.bronze.tenant_settings LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
