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

# ## Gold — `fact_lineage`
# 
# **Fact table, no dimensión.** Cada fila es una relación observada entre dos items (*origen → depende de → destino*) — un hecho, no una entidad descriptiva. Grano: una fila por relación.
# 
# **Resolución de `target_name`.** `target_id` es ambiguo (ver Silver): a veces es un item real, a veces una fuente de datos sin dimensión propia. Se resuelve con un `LEFT JOIN` contra `gold.dim_inventory.item_id` — si hay match, `item_name`; si no, la etiqueta `"Fuente Externa"`. Esta unión entre entidades va en Gold, no en Silver.
# 
# **`source_id` no se resuelve aquí.** Siempre es un item real en las relaciones actuales — se deja como FK simple hacia `dim_inventory`, igual que `principal_id` en `fact_workspace_access`/`fact_item_access`.
# 07_gold_lineage

# CELL ********************

# DEPENDENCIAS Y CLIENTES

from pyspark.sql import functions as F

silver_lineage = spark.table("silver.lineage")
dim_inventory = spark.table("gold.dim_inventory").select("item_id", "item_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RESOLUCIÓN DE TARGET_NAME CONTRA DIM_INVENTORY

gold_fact_lineage = (
    silver_lineage
    .join(dim_inventory, silver_lineage.target_id == dim_inventory.item_id, "left")
    .select(
        silver_lineage["source_id"],
        silver_lineage["relationship_type"],
        silver_lineage["target_id"],
        silver_lineage["target_type"],
        F.coalesce(dim_inventory["item_name"], F.lit("Fuente Externa")).alias("target_name"),
    )
)

gold_fact_lineage.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_lineage")

print(f"gold.fact_lineage: {spark.table('gold.fact_lineage').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.fact_lineage LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
