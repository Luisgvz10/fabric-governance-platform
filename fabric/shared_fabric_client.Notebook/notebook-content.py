# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "c50642e1-ed2e-98df-4121-a4d7e3b1b98e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# Este notebook implementa FabricClient, una clase que centraliza toda la comunicación HTTP con la API REST de Microsoft Fabric. En lugar de que cada notebook realice sus propias llamadas con requests, todos delegan esa responsabilidad en este cliente común, evitando código duplicado y facilitando el mantenimiento a medida que se incorporen nuevas operaciones. Al construirse, FabricClient obtiene sus credenciales desde shared_settings y solicita un Access Token mediante el flujo OAuth2 Client Credentials (MSAL), sin necesidad de autenticación interactiva. Una vez autenticado, expone métodos genéricos get() y post() que encapsulan la construcción de la URL, las cabeceras de autorización y el manejo de la respuesta, de forma que el resto de notebooks solo indican qué operación de Fabric quieren realizar, no cómo comunicarse con la API.


# CELL ********************

import msal
import requests

class FabricClient:

    def __init__(self, tenant_id=None, client_id=None, client_secret=None):
        self.tenant_id = tenant_id or TENANT_ID
        self.client_id = client_id or CLIENT_ID
        self.client_secret = client_secret or CLIENT_SECRET

        self.base_url = "https://api.fabric.microsoft.com/v1"
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
            scopes=["https://api.fabric.microsoft.com/.default"]
        )

        if "access_token" not in result:
            raise Exception(result)

        return result["access_token"]

    def get(self, endpoint):
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint, body):
        response = requests.post(f"{self.base_url}{endpoint}", headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
