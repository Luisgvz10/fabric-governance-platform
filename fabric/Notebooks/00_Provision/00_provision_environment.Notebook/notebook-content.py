# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Este notebook aprovisiona automáticamente, mediante FabricClient, el entorno que simula una organización real sobre la que se aplicarán las tareas de gobierno y observabilidad del proyecto. El escenario (cinco workspaces por área de negocio, con sus carpetas y activos) se describe como datos (WORKSPACES) y se aprovisiona en dos fases separadas por un paso manual: primero se crean los workspaces vacíos (Fase A); a continuación, un administrador humano asigna cada uno a la capacidad fabricgov (Fase B), ya que delegar ese permiso en el Service Principal exigiría concederle rol de Administrador de la capacidad, desproporcionado para una operación puntual; finalmente, con los workspaces ya asociados a una capacidad Fabric, se crean las carpetas y los activos analíticos dentro de cada uno (Fase C). La Fase A es idempotente: si un workspace ya existe, se omite su creación en lugar de fallar.

# CELL ********************

%run shared_settings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run shared_fabric_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric = FabricClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CONFIGURACIÓN DEL ESCENARIO

WORKSPACES = [
        {
        "name": "ws-sales",
        "description": "Datos y analítica del área de Ventas",
        "folders": ["Lakehouses", "Notebooks", "Data Science", "Pipelines"],
        "items": [
            {"type": "lakehouse", "name": "lh_sales", "folder": "Lakehouses"},
            {"type": "notebook", "name": "nb-sales-prep", "folder": "Notebooks"},
            {"type": "ml_experiment", "name": "mle-sales-churn", "folder": "Data Science"},
            {"type": "ml_model", "name": "mlm-sales-churn-predictor", "folder": "Data Science"},
            {"type": "pipeline", "name": "pl-sales-daily-load", "folder": "Pipelines"},
        ],
    },
    {
        "name": "ws-finance",
        "description": "Datos financieros y contables",
        "folders": ["Lakehouses", "Warehouses", "Notebooks"],
        "items": [
            {"type": "warehouse", "name": "wh_finance", "folder": "Warehouses"},
            {"type": "lakehouse", "name": "lh_finance", "folder": "Lakehouses"},
            {"type": "notebook", "name": "nb-finance-etl", "folder": "Notebooks"},
        ],
    },
    {
        "name": "ws-hr",
        "description": "Datos de Recursos Humanos (alta sensibilidad)",
        "folders": ["Lakehouses", "Notebooks"],
        "items": [
            {"type": "lakehouse", "name": "lh_hr", "folder": "Lakehouses"},
            {"type": "notebook", "name": "nb-hr-load", "folder": "Notebooks"},
        ],
    },
    {
        "name": "ws-marketing",
        "description": "Datos de campañas y marketing",
        "folders": ["Lakehouses", "Pipelines"],
        "items": [
            {"type": "lakehouse", "name": "lh_marketing", "folder": "Lakehouses"},
            {"type": "pipeline", "name": "pl-marketing-refresh", "folder": "Pipelines"},
        ],
    },
    {
        "name": "ws-operations",
        "description": "Monitorización en tiempo real y operaciones",
        "folders": ["Warehouses", "Real-Time", "Notebooks", "Pipelines"],
        "items": [
            {"type": "warehouse", "name": "wh_operations", "folder": "Warehouses"},
            {"type": "eventstream", "name": "es-operations-iot", "folder": "Real-Time"},
            {"type": "eventhouse", "name": "eh-operations", "folder": "Real-Time"},
            {"type": "notebook", "name": "nb-operations-monitoring-1", "folder": "Notebooks"},
            {"type": "notebook", "name": "nb-operations-monitoring-2", "folder": "Notebooks"},
            {"type": "pipeline", "name": "pl-operations-batch", "folder": "Pipelines"},
        ],
    },
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fase A: crear workspaces (idempotente):


existing_workspaces = {w["displayName"]: w["id"] for w in fabric.get("/workspaces")["value"]}

for ws_config in WORKSPACES:
    if ws_config["name"] in existing_workspaces:
        print(f"{ws_config['name']} ya existe, se omite.")
        continue
    workspace = fabric.create_workspace(ws_config["name"], ws_config["description"])
    print(f"{ws_config['name']} creado: {workspace['id']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fase B — manual, antes de seguir: ve a cada uno de los 5 workspaces → Workspace settings → License info → asigna fabricgov.

CREATE_ITEM = {
    "lakehouse": fabric.create_lakehouse,
    "warehouse": fabric.create_warehouse,
    "notebook": fabric.create_notebook,
    "pipeline": fabric.create_pipeline,
    "eventstream": fabric.create_eventstream,
    "eventhouse": fabric.create_eventhouse,
    "ml_experiment": fabric.create_ml_experiment,
    "ml_model": fabric.create_ml_model,
}

# Fase C: crear carpetas e items:

existing_workspaces = {w["displayName"]: w["id"] for w in fabric.get("/workspaces")["value"]}

for ws_config in WORKSPACES:
    workspace_id = existing_workspaces[ws_config["name"]]

    existing_folders = {f["displayName"]: f["id"] for f in fabric.get(f"/workspaces/{workspace_id}/folders")["value"]}
    folder_ids = {}
    for folder_name in ws_config["folders"]:
        if folder_name not in existing_folders:
            folder = fabric.create_folder(workspace_id, folder_name)
            existing_folders[folder_name] = folder["id"]
        folder_ids[folder_name] = existing_folders[folder_name]

    existing_items = {i["displayName"] for i in fabric.get(f"/workspaces/{workspace_id}/items")["value"]}
    for item in ws_config["items"]:
        if item["name"] in existing_items:
            continue
        create_fn = CREATE_ITEM[item["type"]]
        create_fn(workspace_id, item["name"], folder_ids[item["folder"]])

    print(f"{ws_config['name']} aprovisionado correctamente.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
