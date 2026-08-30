# fabric-governance-platform

[![tests](https://github.com/Luisgvz10/fabric-governance-platform/actions/workflows/tests.yml/badge.svg)](https://github.com/Luisgvz10/fabric-governance-platform/actions/workflows/tests.yml)

Plataforma de gobierno y observabilidad para Microsoft Fabric — TFM, Máster en Data Engineering &amp; Big Data (UCM).

## `fabricgov` — cliente de las APIs de Fabric/Power BI

`src/fabricgov/` contiene la versión standalone y testeada de `FabricClient`, la clase que autentica (OAuth2 Client Credentials vía MSAL) y llama a las APIs de administración de Microsoft Fabric y Power BI, usada en tiempo real desde `fabric/Notebooks/shared_fabric_client.Notebook`.

```bash
pip install -e ".[dev]"
pytest -v
```
