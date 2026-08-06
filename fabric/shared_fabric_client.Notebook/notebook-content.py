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

# Este notebook implementa FabricClient, una clase que centraliza toda la comunicación HTTP con APIs REST protegidas mediante Microsoft Entra ID — pensada inicialmente solo para la API de Fabric, y ampliada después para servir también a la API de Power BI, que comparte el mismo mecanismo de autenticación pero exige un scope y una URL base distintos. En lugar de que cada notebook realice sus propias llamadas con requests, todos delegan esa responsabilidad en este cliente común, evitando código duplicado y facilitando el mantenimiento a medida que se incorporen nuevas operaciones. Al construirse, FabricClient obtiene sus credenciales desde shared_settings y solicita un Access Token mediante el flujo OAuth2 Client Credentials (MSAL), sin necesidad de autenticación interactiva; tanto el scope solicitado como la URL base son parametrizables, lo que permite instanciar el mismo cliente contra Fabric o contra Power BI sin duplicar la lógica de autenticación. Una vez autenticado, expone métodos genéricos get() y post() que encapsulan la construcción de la URL, las cabeceras de autorización y el manejo de la respuesta, de forma que el resto de notebooks solo indican qué operación quieren realizar, no cómo comunicarse con la API.
# 
# Esta clase se amplía con un conjunto de métodos específicos para el aprovisionamiento automático de recursos: creación de workspaces, carpetas, y los distintos tipos de activos analíticos (Lakehouse, Warehouse, Notebook, Pipeline, Eventstream, Eventhouse, ML Experiment, ML Model). Todos comparten una misma lógica interna (_create_item), evitando duplicar código para cada tipo de objeto: cada método público solo indica el endpoint de la API correspondiente, delegando la construcción de la petición HTTP al método privado compartido.
# 
# Se añade además get_paginated(), que sigue automáticamente el mecanismo de paginación por continuationToken que usan las APIs de administración de Fabric y Power BI (hasta 10.000 registros por página), acumulando el resultado de varias páginas en una única lista sin que el notebook que la invoca tenga que gestionar el bucle de paginación.
# 
# El nombre de la clase se mantiene por continuidad con el resto del proyecto, aunque su alcance ya no se limita en sentido estricto a la API de Fabric; se reconsiderará si en fases posteriores se incorporan más APIs con necesidades de autenticación distintas.


# CELL ********************

import msal
import requests

class FabricClient:

    def __init__(self, tenant_id=None, client_id=None, client_secret=None,
                 scope="https://api.fabric.microsoft.com/.default",
                 base_url="https://api.fabric.microsoft.com/v1"):
        self.tenant_id = tenant_id or TENANT_ID
        self.client_id = client_id or CLIENT_ID
        self.client_secret = client_secret or CLIENT_SECRET
        self.scope = scope
        self.base_url = base_url

        self.token = self._get_access_token()

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def _get_access_token(self):
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )

        result = app.acquire_token_for_client(
            scopes=[self.scope]
        )

        if "access_token" not in result:
            raise Exception(result)

        return result["access_token"]

    def get(self, endpoint, params=None):
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint, body):
        response = requests.post(f"{self.base_url}{endpoint}", headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    def get_paginated(self, endpoint, items_key, params=None):
        results = []
        params = dict(params or {})
        while True:
            response = self.get(endpoint, params=params)
            results.extend(response.get(items_key, []))
            token = response.get("continuationToken")
            if not token:
                break
            params["continuationToken"] = token
        return results

    def _create_item(self, workspace_id, endpoint, display_name, folder_id=None):
        body = {"displayName": display_name}
        if folder_id:
            body["folderId"] = folder_id
        return self.post(f"/workspaces/{workspace_id}/{endpoint}", body)

    def create_workspace(self, display_name, description=None):
        body = {"displayName": display_name}
        if description:
            body["description"] = description
        workspace = self.post("/workspaces", body)

        # Sin esto, el workspace queda visible únicamente para el Service Principal
        # que lo creó, invisible para los administradores humanos.
        self.post(f"/workspaces/{workspace['id']}/roleAssignments", {
            "principal": {
                "id": "3780bf58-00ee-42a9-8b3a-530d328da07c",
                "type": "Group"
            },
            "role": "Admin"
        })

        return workspace

    def create_folder(self, workspace_id, display_name):
        return self.post(f"/workspaces/{workspace_id}/folders", {"displayName": display_name})

    def create_lakehouse(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "lakehouses", display_name, folder_id)

    def create_warehouse(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "warehouses", display_name, folder_id)

    def create_notebook(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "notebooks", display_name, folder_id)

    def create_pipeline(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "dataPipelines", display_name, folder_id)

    def create_eventstream(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "eventstreams", display_name, folder_id)

    def create_eventhouse(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "eventhouses", display_name, folder_id)

    def create_ml_experiment(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "mlExperiments", display_name, folder_id)

    def create_ml_model(self, workspace_id, display_name, folder_id=None):
        return self._create_item(workspace_id, "mlModels", display_name, folder_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
