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

# Capa Gold — principio general. Es la capa lista para consumo: combina las tablas Silver (limpias pero todavía separadas por fuente) en el modelo final que usará el modelo semántico de gobernanza. Aquí sí se aplican las reglas de unión entre entidades — es la única capa donde tiene sentido hacerlo, porque es la única pensada para ser consultada directamente por herramientas de análisis, no por otros procesos de transformación.
# 
# Qué construye este notebook. La dimensión de inventario (gold_dim_inventory) que se planteó al inicio de la Fase 2: cada fila es un item de Fabric, con los atributos de su workspace y de la capacidad de ese workspace ya incorporados mediante join, en lugar de mantenerlos en tablas separadas. Los joins son left porque no todos los workspaces tienen una capacidad asignada (los personales, por ejemplo) — un inner join perdería esos items silenciosamente.

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
