# fabric-governance-platform

[![tests](https://github.com/Luisgvz10/fabric-governance-platform/actions/workflows/tests.yml/badge.svg)](https://github.com/Luisgvz10/fabric-governance-platform/actions/workflows/tests.yml)

Plataforma de gobierno y observabilidad para Microsoft Fabric — TFM, Máster en Big Data & Data Engineering (UCM).

Consume las APIs de administración de Microsoft Fabric, Power BI y Microsoft Graph para centralizar en un único Lakehouse (arquitectura Medallion) el inventario de activos, la actividad de usuarios, los permisos, los ajustes de tenant, el linaje entre activos y el consumo de capacidad de un tenant de Fabric. Sobre esos datos se construye un modelo semántico y un informe de Power BI que validan, con datos reales, las preguntas de gobierno que se plantea el proyecto.

El detalle completo de diseño, decisiones y resultados está documentado en la memoria del TFM.

## Estructura del repositorio

| Carpeta | Contenido |
|---|---|
| `fabric/Notebooks/00_Provision` | Aprovisionamiento del entorno (identidad, workspaces simulados) vía API |
| `fabric/Notebooks/01_Bronze` a `04_Maintenance` | 27 notebooks PySpark, tres por entidad de gobierno, más el notebook de mantenimiento (`OPTIMIZE`/`VACUUM`) |
| `fabric/Data Pipeline` | 5 Data Pipelines: `pl-bronze`, `pl-silver`, `pl-gold`, `pl-governance-orchestration`, `pl-maintenance` |
| `fabric/Lakehouse` | `lh_metadata`, con schemas nativos `bronze`/`silver`/`gold` |
| `fabric/Semantic Models` | `sm-governance`, modelo Direct Lake con esquema en estrella |
| `fabric/Reports` | `rp-governance-validation`, informe de validación de seis páginas |
| `fabric/Environment` | Entorno de ejecución compartido por todos los notebooks |
| `src/fabricgov` | Cliente de las APIs, empaquetado y testeado de forma independiente (ver abajo) |
| `tests` | Tests unitarios de `fabricgov` |
| `.github/workflows` | CI: ejecuta los tests en cada push |

Todo lo que hay bajo `fabric/` se sincroniza automáticamente desde el propio workspace de Fabric mediante Git integration; `src/`, `tests/` y `.github/` se mantienen aparte, a mano.

## `fabricgov` — cliente de las APIs de Fabric/Power BI

`src/fabricgov/` contiene la versión standalone y testeada de `FabricClient`, la clase que autentica (OAuth2 Client Credentials vía MSAL) y llama a las APIs de administración de Microsoft Fabric y Power BI, usada en tiempo real desde `fabric/Notebooks/shared_fabric_client.Notebook`.

```bash
pip install -e ".[dev]"
pytest -v
```

Los tests cubren autenticación, construcción de peticiones HTTP y paginación, con `requests` y `msal` sustituidos por dobles de prueba (sin llamadas reales ni credenciales). Se ejecutan automáticamente en cada push mediante GitHub Actions.

---

Luis Alfonso Gutiérrez Martínez · Máster en Big Data & Data Engineering, Universidad Complutense de Madrid · Septiembre 2026
