# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************


TENANT_ID = "1db3d1d8-436f-4efe-92ac-8ae2decbe02d"
CLIENT_ID = "1b45d76d-7576-4978-8ad6-52255f197b30"

AKV_URL = "https://kv-lg-fabricgov.vault.azure.net/"
CLIENT_SECRET_NAME = "fabric-governance-client-secret"

CLIENT_SECRET = notebookutils.credentials.getSecret(AKV_URL, CLIENT_SECRET_NAME)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
