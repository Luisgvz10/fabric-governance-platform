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

# Este notebook valida de extremo a extremo el mecanismo de autenticación que utilizará toda la plataforma de gobierno. A diferencia de la primera versión (que obtenía el token directamente con MSAL y credenciales hardcodeadas en el propio notebook), esta validación reutiliza los componentes compartidos del proyecto: shared_settings resuelve la configuración y el secreto del Service Principal desde Azure Key Vault, y shared_fabric_client expone FabricClient, que encapsula la autenticación OAuth 2.0 Client Credentials y la comunicación HTTP con la API REST de Microsoft Fabric. Ejecutar correctamente este notebook demuestra dos cosas a la vez: que el Service Principal se autentica correctamente, y que ningún notebook del proyecto necesita conocer ni exponer el secreto en texto plano, ya que se resuelve en tiempo de ejecución desde Key Vault.

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

try:
    fabric = FabricClient()
    print("Autenticación correcta: Access Token obtenido desde Microsoft Entra ID.")
except Exception as e:
    print("Error de autenticación:")
    print(e)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaces = fabric.get("/workspaces")
workspaces


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
