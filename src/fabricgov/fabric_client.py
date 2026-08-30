"""FabricClient: cliente HTTP reutilizable para las APIs de administracion de
Microsoft Fabric y Power BI, autenticado via OAuth2 Client Credentials (MSAL).

Version standalone y testeable de la clase que vive en vivo dentro de
`fabric/Notebooks/shared_fabric_client.Notebook` (ejecutada alli via `%run`,
sin poder importarse con un test runner normal). Este modulo no depende de
`notebookutils` ni de variables globales de notebook: las credenciales se
pasan siempre de forma explicita al constructor.

Una diferencia deliberada frente a la version del notebook: `create_workspace`
no lleva hardcodeado ningun GUID de grupo de seguridad concreto; el principal
admin por defecto se recibe como parametro (`admin_principal_id`), para que el
paquete sea reutilizable fuera del tenant de este proyecto.
"""

import msal
import requests


class FabricClient:

    def __init__(self, tenant_id, client_id, client_secret,
                 scope="https://api.fabric.microsoft.com/.default",
                 base_url="https://api.fabric.microsoft.com/v1"):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.base_url = base_url

        self.token = self._get_access_token()

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _get_access_token(self):
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"

        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret,
        )

        result = app.acquire_token_for_client(scopes=[self.scope])

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

    def create_workspace(self, display_name, description=None, admin_principal_id=None):
        body = {"displayName": display_name}
        if description:
            body["description"] = description
        workspace = self.post("/workspaces", body)

        # Sin esto, el workspace queda visible unicamente para el Service Principal
        # que lo creo, invisible para los administradores humanos.
        if admin_principal_id:
            self.post(f"/workspaces/{workspace['id']}/roleAssignments", {
                "principal": {
                    "id": admin_principal_id,
                    "type": "Group",
                },
                "role": "Admin",
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
