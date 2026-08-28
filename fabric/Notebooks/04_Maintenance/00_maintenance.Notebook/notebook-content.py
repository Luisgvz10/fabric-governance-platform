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

# ## Mantenimiento — `OPTIMIZE` / `VACUUM`
# 
# **Qué hace.** Recorre todas las tablas de `bronze`, `silver` y `gold` de forma dinámica (`spark.catalog.listTables`, sin lista fija que mantener a mano — cubre automáticamente cualquier entidad nueva) aplicando dos operaciones de Delta Lake con propósitos distintos:
# 
# - **`OPTIMIZE`** compacta los archivos pequeños que deja cualquier escritura repetida. Se aplica aquí a todas las tablas **salvo** `bronze.activity` y `bronze.capacity_metrics` — las dos únicas del proyecto que usan `MERGE` (no `overwrite`), y por tanto las únicas con fragmentación incremental real; ya se compactan justo después de cada `MERGE`, dentro de su propio notebook (`Bronze/02`, `Bronze/08`), así que repetirlo aquí no tendría nada que compactar. El resto de tablas (`overwrite` puro, sin ningún paso de compactación en otro sitio) sí lo necesitan aquí.
# - **`VACUUM`** no elimina datos por antigüedad — elimina **archivos huérfanos**: los que quedaron atrás porque una escritura posterior (`overwrite`, `MERGE` u `OPTIMIZE`) ya los sustituyó y no forman parte de la versión actual de la tabla, siempre que ese archivo huérfano lleve ya más de 7 días (umbral de retención por defecto de Delta, sin reducir — reducirlo exige desactivar una comprobación de seguridad, y no hay necesidad real de liberar espacio antes con el volumen de este proyecto). Los datos vigentes, por antiguos que sean, no se tocan nunca. Se aplica a las 17 tablas sin excepción: todas se reescriben en cada ejecución del pipeline (`overwrite` o `MERGE`), así que todas generan archivos huérfanos con el tiempo, no solo las de mayor volumen.
# 
# **Por qué está fuera de `pl-governance-orchestration`, en su propio pipeline (`pl-maintenance`).** `VACUUM` solo tiene algo que limpiar pasado el umbral de retención de 7 días — ejecutarlo con la misma cadencia que la orquestación diaria de gobierno no aporta nada la mayoría de los días y desperdicia cómputo sin necesidad real. Este notebook se programa con un *trigger* semanal propio, independiente y no encadenado al resto de pipelines.


# CELL ********************

# MANTENIMIENTO: OPTIMIZE (salvo las tablas MERGE, ya compactadas en línea) + VACUUM (todas)

schemas = ["bronze", "silver", "gold"]
already_optimized_inline = {"bronze.activity", "bronze.capacity_metrics"}

for schema in schemas:
    for table in spark.catalog.listTables(schema):
        full_name = f"{schema}.{table.name}"
        if full_name not in already_optimized_inline:
            print(f"OPTIMIZE {full_name}")
            spark.sql(f"OPTIMIZE {full_name}")
        print(f"VACUUM {full_name}")
        spark.sql(f"VACUUM {full_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
