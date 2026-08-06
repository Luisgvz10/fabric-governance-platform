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

# ## Gold — `dim_inventory`
# 
# **Qué hace.** Construye la dimensión de inventario combinando las tres tablas Silver mediante `join`: cada fila es un item de Fabric con los atributos de su workspace y de la capacidad de ese workspace ya incorporados, en vez de mantenerlos separados. Los `join` son `left` porque no todos los workspaces tienen capacidad asignada (los personales, por ejemplo) — un `inner join` perdería esos items en silencio. Es la única capa donde tiene sentido cruzar entidades: la única pensada para consultarse directamente desde herramientas de análisis.


# CELL ********************

# CONSTRUCIÓN DE LA DIMENSIÓN

silver_items = spark.table("silver.items")
silver_workspaces = spark.table("silver.workspaces")
silver_capacities = spark.table("silver.capacities")

gold_dim_inventory = (
    silver_items
    .join(silver_workspaces, on="workspace_id", how="left")
    .join(silver_capacities, on="capacity_id", how="left")
)

gold_dim_inventory.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_inventory")

print(f"gold_dim_inventory: {gold_dim_inventory.count()} filas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.dim_inventory LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
