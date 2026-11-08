"""
Job Audit: ¿qué cambió en silver.transactions con el último lote?

Cada MERGE crea una versión nueva de la tabla Delta. Con time travel puedes
comparar la versión anterior con la actual y reconstruir exactamente qué
transacciones cambiaron, sin haber guardado una copia aparte.

Argumentos: --BASE <s3://bucket | carpeta local>
"""

from __future__ import annotations

from common import check, get_spark, parse_args, table_path
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

args = parse_args(["BASE"])
BASE = args["BASE"].rstrip("/")
spark = get_spark("paytrack-audit")

TXN_PATH = table_path(BASE, "silver", "transactions")

# Dado: el historial de la tabla, de la versión más reciente a la más antigua
history = DeltaTable.forPath(spark, TXN_PATH).history()
history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=80)
last_merge = history.filter("operation = 'MERGE'").orderBy(F.desc("version")).first()

# ── TODO 1: las dos versiones ────────────────────────────────────────────────
# Devuelve la tabla tal como estaba en la versión `version` (time travel).

#def read_version(version: int) -> DataFrame:
#    raise NotImplementedError("TODO 1: read_version")

def read_version(version: int) -> DataFrame:
    # Carga la tabla Delta en el punto exacto de la versión solicitada
    return spark.read \
        .format("delta") \
        .option("versionAsOf", version) \
        .load(TXN_PATH)


# ── TODO 2: cambios de estado ────────────────────────────────────────────────
# Compara `before` y `after` y devuelve las transacciones que existían en ambas
# versiones y cuyo status cambió, con las columnas:
#   transaction_id, customer_id, amount_cop, old_status, new_status

#def status_changes(before: DataFrame, after: DataFrame) -> DataFrame:
#    raise NotImplementedError("TODO 2: status_changes")

import pyspark.sql.functions as F

def status_changes(before: DataFrame, after: DataFrame) -> DataFrame:
    # 1. Renombrar la columna status en ambos DataFrames para evitar colisiones
    df_before = before.withColumnRenamed("status", "old_status")
    df_after = after.withColumnRenamed("status", "new_status")
    
    # 2. Hacer Inner Join por transaction_id (solo transacciones que existen en ambas)
    # 3. Filtrar donde el estado haya cambiado
    # 4. Seleccionar las columnas solicitadas
    changes_df = df_before.join(
        df_after, 
        on="transaction_id", 
        how="inner"
    ).filter(
        F.col("old_status") != F.col("new_status")
    ).select(
        "transaction_id",
        df_after["customer_id"],  # Especificamos origen para evitar ambigüedad si es necesario
        df_after["amount_cop"],
        "old_status",
        "new_status"
    ) 
    return changes_df

# ── Ejecución y validación (dadas) ───────────────────────────────────────────

before, after = read_version(last_merge.version - 1), read_version(last_merge.version)
changes = status_changes(before, after)
changes.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .save(table_path(BASE, "gold", "status_changes"))

metrics = last_merge.operationMetrics
print(f"Versión {last_merge.version}: {metrics['numTargetRowsInserted']} insertadas, "
      f"{metrics['numTargetRowsUpdated']} actualizadas")
changes.groupBy("old_status", "new_status").agg(
    F.count("*").alias("transacciones"), F.sum("amount_cop").alias("monto_cop")
).show()

check(set(changes.columns) == {"transaction_id", "customer_id", "amount_cop", "old_status", "new_status"},
      "status_changes: columnas correctas")
check(changes.count() == int(metrics["numTargetRowsUpdated"]),
      "status_changes: coincide con las filas que actualizó el MERGE")
check(changes.filter("old_status = new_status").count() == 0, "status_changes: solo cambios reales")
check(after.count() - before.count() == int(metrics["numTargetRowsInserted"]),
      "las versiones difieren exactamente en las filas insertadas por el MERGE")

print("\nAuditoría lista")
