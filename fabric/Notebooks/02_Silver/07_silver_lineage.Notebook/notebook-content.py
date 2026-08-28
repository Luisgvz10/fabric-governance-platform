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

# ## Silver — `lineage`
# 
# **Modelo de lista de aristas.** El linaje es un grafo, no una entidad plana: un item depende de otro, que depende de otro. En vez de una tabla por tipo de relación, se modela como una única tabla de grano *relación* (`workspace_id`, `source_type`, `source_id`, `source_name`, `relationship_type`, `target_type`, `target_id`) — cualquier relación nueva que aparezca más adelante se añade como una fuente más al `union`, sin rediseñar la tabla.
# 
# **`datasourceInstanceId` no resuelve contra ninguna dimensión.** Las dos instancias reales (`MetricsDataConnector`, `AdminInsights`) son plomería interna de Microsoft, no fuentes externas del proyecto — se descartó modelarlas como dimensión. `target_id` queda como el GUID crudo.


# CELL ********************

# DEPENDENCIAS Y CLIENTES

from pyspark.sql import functions as F

bronze_workspace_scan = spark.table("bronze.workspace_scan")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RELACIÓN 1: dataset -> datasource (datasets[].datasourceUsages)

datasets_exploded = (
    bronze_workspace_scan
    .select(
        F.col("id").alias("workspace_id"),
        F.explode("datasets").alias("dataset"),
    )
)

dataset_datasource_edges = (
    datasets_exploded
    .select(
        F.col("workspace_id"),
        F.lit("Dataset").alias("source_type"),
        F.col("dataset.id").alias("source_id"),
        F.col("dataset.name").alias("source_name"),
        F.explode("dataset.datasourceUsages").alias("datasource_usage"),
    )
    .select(
        "workspace_id", "source_type", "source_id", "source_name",
        F.lit("DatasourceUsage").alias("relationship_type"),
        F.lit("Datasource").alias("target_type"),
        F.col("datasource_usage.datasourceInstanceId").alias("target_id"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RELACIÓN 2: report -> dataset (reports[].datasetId)

reports_exploded = (
    bronze_workspace_scan
    .select(
        F.col("id").alias("workspace_id"),
        F.explode("reports").alias("report"),
    )
)

report_dataset_edges = (
    reports_exploded
    .filter(F.col("report.datasetId").isNotNull())
    .select(
        F.col("workspace_id"),
        F.lit("Report").alias("source_type"),
        F.col("report.id").alias("source_id"),
        F.col("report.name").alias("source_name"),
        F.lit("DependsOnDataset").alias("relationship_type"),
        F.lit("Dataset").alias("target_type"),
        F.col("report.datasetId").alias("target_id"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RELACIÓN 2: report -> dataset (reports[].datasetId)

reports_exploded = (
    bronze_workspace_scan
    .select(
        F.col("id").alias("workspace_id"),
        F.explode("reports").alias("report"),
    )
)

report_dataset_edges = (
    reports_exploded
    .filter(F.col("report.datasetId").isNotNull())
    .select(
        F.col("workspace_id"),
        F.lit("Report").alias("source_type"),
        F.col("report.id").alias("source_id"),
        F.col("report.name").alias("source_name"),
        F.lit("DependsOnDataset").alias("relationship_type"),
        F.lit("Dataset").alias("target_type"),
        F.col("report.datasetId").alias("target_id"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UNIÓN Y ESCRITURA EN SILVER

silver_lineage = dataset_datasource_edges.unionByName(report_dataset_edges)

silver_lineage.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.lineage")

print(f"silver.lineage: {spark.table('silver.lineage').count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_metadata.silver.lineage LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
