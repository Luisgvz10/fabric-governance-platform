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

# ## Gold — `dim_inventory` y `dim_workspace`
# 
# **Qué hace.** Construye dos dimensiones a partir de las mismas tres tablas Silver (`items`, `workspaces`, `capacities`), sin necesidad de una extracción nueva. `dim_inventory` combina las tres mediante `join` a grano item: cada fila es un item de Fabric con los atributos de su workspace y de la capacidad de ese workspace ya incorporados. `dim_workspace` combina solo `workspaces` y `capacities`, a grano workspace — la dimensión que faltaba para poder relacionar entidades de gobierno a nivel de workspace (como `fact_workspace_access`) sin forzar una relación contra `dim_inventory`, que no es única por workspace.
# 
# **Por qué `left` y no `inner`.** En ambos casos, un `inner join` descartaría en silencio los workspaces/items sin capacidad asignada (los personales, por ejemplo) — un `left join` los conserva.
# 
# **Nota de modelado (modelo semántico).** `dim_inventory` conserva `workspace_id`/`workspace_name` como columnas propias (denormalizadas), pero **no** debe relacionarse directamente con `dim_workspace` en el modelo semántico: como ambas comparten `workspace_id`, Fabric puede detectar esa relación automáticamente, y combinada con el resto de relaciones del modelo cierra un ciclo (`dim_workspace`→`fact_workspace_access`→`dim_user`→`fact_item_access`→`dim_inventory`→`dim_workspace`). `dim_workspace` se conecta únicamente con `fact_workspace_access`; `dim_inventory`, con `fact_item_access`.


# CELL ********************

# CONSTRUCIÓN DE LA DIMENSIÓN INVENTORY

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

# CONSTRUCCIÓN DE LA DIMENSIÓN DE WORKSPACE
gold_dim_workspace = (
    silver_workspaces
    .join(silver_capacities, on="capacity_id", how="left")
)

gold_dim_workspace.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_workspace")

print(f"gold_dim_workspace: {gold_dim_workspace.count()} filas")


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

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.gold.dim_workspace LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
