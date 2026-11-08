"""
Job Silver: bronze (un lote) → silver (Delta, versión confiable de cada entidad).

Silver tiene una fila por llave, tipos correctos, reglas de calidad aplicadas y
datos personales protegidos. Lo que no pasa las reglas va a una cuarentena.
Cada lote se integra con MERGE: las transacciones nuevas se insertan y las que
ya existían se actualizan (por ejemplo, un contracargo).

Argumentos: --BASE <s3://bucket | carpeta local> --INGEST_DATE <YYYY-MM-DD>
"""

from __future__ import annotations

from common import check, get_spark, parse_args, read_table, table_exists, table_path
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window  # noqa: F401 (la usarás en un TODO)

AMOUNT_MAX = 100_000_000  # COP: montos iguales o mayores son errores de captura
VALID_STATUS = ["approved", "declined", "chargeback"]

args = parse_args(["BASE", "INGEST_DATE"])
BASE, INGEST_DATE = args["BASE"].rstrip("/"), args["INGEST_DATE"]
spark = get_spark("paytrack-silver")


def bronze_batch(dataset: str) -> DataFrame:
    """Dado: el lote INGEST_DATE de una tabla bronze."""
    return read_table(spark, table_path(BASE, "bronze", dataset)).filter(F.col("ingest_date") == INGEST_DATE)


def ensure_table(df: DataFrame, path: str, partition_by: list[str]) -> None:
    """Dado: crea la tabla Delta vacía (con el esquema de df) si todavía no existe."""
    if not table_exists(spark, path):
        df.limit(0).write.format("delta").partitionBy(*partition_by).save(path)

# ── TODO 1: deduplicar ───────────────────────────────────────────────────────
# Deja una sola fila por `key`: la de ingested_at más reciente. Usa una función
# de ventana (Window + row_number); dropDuplicates no te deja elegir cuál fila
# se conserva.

#def deduplicate(df: DataFrame, key: str) -> DataFrame:
#    raise NotImplementedError("TODO 1: deduplicate")

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

def deduplicate(df: DataFrame, key: str) -> DataFrame:
    # 1. Definir la ventana: agrupar por la clave y ordenar por fecha de ingesta descendente
    window_spec = Window.partitionBy(key).orderBy(col("ingested_at").desc())
    
    # 2. Enumerar las filas, filtrar la más reciente (1) y eliminar la columna temporal
    return df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")

# ── TODO 2: tipar ────────────────────────────────────────────────────────────
# En Bronze todo es texto. Devuelve las transacciones con:
#   - amount_cop como decimal(15,2) (si no se puede convertir, queda NULL)
#   - event_ts (timestamp) a partir de "timestamp", sin la columna "timestamp"
#   - is_fraud como boolean
#   - columnas de partición year ("YYYY") y month ("MM") derivadas de event_ts

#def standardize(df: DataFrame) -> DataFrame:
#    raise NotImplementedError("TODO 2: standardize")

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, lpad

def standardize(df: DataFrame) -> DataFrame:
    return df \
        .withColumn("amount_cop", col("amount_cop").cast("decimal(15,2)")) \
        .withColumn("event_ts", col("timestamp").cast("timestamp")) \
        .drop("timestamp") \
        .withColumn("is_fraud", col("is_fraud").cast("boolean")) \
        .withColumn("year", year(col("event_ts")).cast("string")) \
        .withColumn("month", lpad(month(col("event_ts")).cast("string"), 2, "0"))

# ── TODO 3: reglas de calidad ────────────────────────────────────────────────
# Agrega la columna dq_reason con el PRIMER problema de la fila, o NULL si está bien:
#   customer_id nulo                                → 'missing_customer'
#   amount_cop nulo, <= 0 o >= AMOUNT_MAX           → 'invalid_amount'
#   status que no está en VALID_STATUS              → 'invalid_status'

#def add_quality_flags(df: DataFrame) -> DataFrame:
#    raise NotImplementedError("TODO 3: add_quality_flags")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_quality_flags(df: DataFrame) -> DataFrame:
    # Definimos las condiciones secuenciales
    df_with_flags = df.withColumn(
        "dq_reason",
        F.when(F.col("customer_id").isNull(), "missing_customer")
        .when(
            (F.col("amount_cop").isNull()) | 
            (F.col("amount_cop") <= 0) | 
            (F.col("amount_cop") >= F.lit(AMOUNT_MAX)), 
            "invalid_amount"
        )
        .when(~F.col("status").isin(VALID_STATUS), "invalid_status")
        .otherwise(F.lit(None))
    )
    
    return df_with_flags

# ── TODO 4: upsert con MERGE ─────────────────────────────────────────────────
# Integra `valid` en la tabla Delta de `path` (que ya existe) usando la API
# DeltaTable.forPath(spark, path).merge(...):
#   - la llave es transaction_id
#   - si ya existe y su status cambió: actualiza status, is_fraud y silver_updated_at
#   - si ya existe y el status es el mismo: no hagas nada
#   - si no existe: insértala completa
# Re-ejecutar el job con el mismo lote no debe cambiar la tabla.

#def upsert_transactions(valid: DataFrame, path: str) -> None:
#    raise NotImplementedError("TODO 4: upsert_transactions")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def upsert_transactions(valid: DataFrame, path: str) -> None:
    # 1. Obtener la referencia a la tabla Delta existente usando la sesión de spark activa
    spark = valid.sparkSession
    delta_table = DeltaTable.forPath(spark, path)
    
    # 2. Ejecutar la operación MERGE
    delta_table.alias("target") \
        .merge(
            source=valid.alias("source"),
            condition="target.transaction_id = source.transaction_id"
        ) \
        .whenMatchedUpdate(
            # CONDICIÓN: Si ya existe Y el status cambió
            condition="target.status <> source.status",
            set={
                "status": "source.status",
                "is_fraud": "source.is_fraud",
                "silver_updated_at": "source.silver_updated_at"
            }
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

# ── TODO 5: proteger datos personales ────────────────────────────────────────
# Devuelve una expresión (Column) con el email enmascarado: los primeros 3
# caracteres del usuario + "***" + el dominio.
#   "carlos.perez@gmail.com" → "car***@gmail.com"
# Pista: F.regexp_replace

#def masked_email(col: str = "email"):
#    raise NotImplementedError("TODO 5: masked_email")

from pyspark.sql import functions as F
from pyspark.sql.column import Column

def masked_email(col: str = "email") -> Column:
    # 1. Separamos el usuario y el dominio usando el '@' como divisor
    user_part = F.substring_index(F.col(col), "@", 1)
    domain_part = F.substring_index(F.col(col), "@", -1)
    
    # 2. Tomamos los primeros 3 caracteres del usuario (o menos si el usuario es más corto)
    first_3 = F.substring(user_part, 1, 3)
    
    # 3. Construimos el email enmascarado uniendo las partes de forma exacta
    masked_expr = F.concat(first_3, F.lit("***@"), domain_part)
    
    # 4. Controlamos casos nulos o vacíos para que no dejen la fila como NULL
    return F.when(
        F.col(col).isNull() | (F.trim(F.col(col)) == "") | (~F.col(col).contains("@")), 
        F.lit("err***@invalid.com")
    ).otherwise(masked_expr)

# ── Ejecución (dada) ─────────────────────────────────────────────────────────

# Transacciones
txn_path = table_path(BASE, "silver", "transactions")
quarantine_path = table_path(BASE, "silver", "transactions_quarantine")
batch = bronze_batch("transactions")

checked = (
    add_quality_flags(standardize(deduplicate(batch, "transaction_id")))
    .drop("ingested_at", "source_file")
    .withColumn("silver_updated_at", F.current_timestamp())
    .localCheckpoint()  # se materializa una vez: válidas y cuarentena salen del mismo resultado
)
valid = checked.filter("dq_reason IS NULL").drop("dq_reason")
quarantine = checked.filter("dq_reason IS NOT NULL")

(quarantine.write.format("delta").mode("overwrite").partitionBy("ingest_date")
    .option("replaceWhere", f"ingest_date = '{INGEST_DATE}'").save(quarantine_path))

ensure_table(valid, txn_path, ["year", "month"])
previous_ids = read_table(spark, txn_path).select("transaction_id").localCheckpoint()
upsert_transactions(valid, txn_path)

# Clientes: actualización tipo 1 (se sobrescriben los cambios de ciudad/segmento)
customers_path = table_path(BASE, "silver", "customers")
customers = (
    deduplicate(bronze_batch("customers"), "customer_id")
    .drop("name", "ingested_at", "source_file")
    .withColumn("email", masked_email("email"))
    .withColumn("monthly_income_cop", F.col("monthly_income_cop").cast("decimal(15,2)"))
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("silver_updated_at", F.current_timestamp())
)
ensure_table(customers, customers_path, [])
(DeltaTable.forPath(spark, customers_path).alias("t")
    .merge(customers.alias("s"), "t.customer_id = s.customer_id")
    .whenMatchedUpdateAll(condition="t.city <> s.city OR t.segment <> s.segment OR t.email <> s.email")
    .whenNotMatchedInsertAll()
    .execute())

# Comercios: catálogo pequeño, se reemplaza completo
(deduplicate(bronze_batch("merchants"), "id")
    .select(F.col("id").alias("merchant_id"), F.col("name").alias("merchant_name"),
            F.col("category").alias("merchant_category"), "city")
    .write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .save(table_path(BASE, "silver", "merchants")))

# Eventos de fraude: solo se insertan los nuevos
fraud_path = table_path(BASE, "silver", "fraud_events")
fraud = (
    deduplicate(bronze_batch("fraud_events"), "event_id")
    .withColumn("risk_score", F.col("risk_score").cast("decimal(5,4)"))
    .withColumn("detected_at", F.to_timestamp("detected_at"))
    .drop("ingested_at", "source_file")
)
ensure_table(fraud, fraud_path, [])
(DeltaTable.forPath(spark, fraud_path).alias("t")
    .merge(fraud.alias("s"), "t.event_id = s.event_id")
    .whenNotMatchedInsertAll()
    .execute())

# ── Validación (dada) ────────────────────────────────────────────────────────

silver = read_table(spark, txn_path)
quarantined = read_table(spark, quarantine_path).filter(F.col("ingest_date") == INGEST_DATE)
dtypes = dict(silver.dtypes)
new_ids = valid.join(previous_ids, "transaction_id", "left_anti").count()

check(dtypes.get("amount_cop") == "decimal(15,2)" and dtypes.get("event_ts") == "timestamp"
      and dtypes.get("is_fraud") == "boolean", "silver.transactions: tipos correctos")
check(valid.count() + quarantined.count() == batch.select("transaction_id").distinct().count(),
      "lote: válidas + cuarentena = transacciones distintas del lote (no se perdió nada)")
check(quarantined.count() > 0 and quarantined.filter(
          F.col("dq_reason").isNull() | ~F.col("dq_reason").isin("missing_customer", "invalid_amount", "invalid_status")
      ).count() == 0, "cuarentena: cada fila tiene un dq_reason válido")
check(silver.count() == silver.select("transaction_id").distinct().count(), "silver: sin transaction_id duplicados")
check(silver.count() == previous_ids.count() + new_ids, f"silver: se insertaron las {new_ids} transacciones nuevas")
check(valid.alias("v").join(silver.alias("s"), "transaction_id")
      .filter(F.col("v.status") != F.col("s.status")).count() == 0,
      "silver: el status de cada transacción del lote quedó actualizado")
check(silver.filter(f"amount_cop <= 0 OR amount_cop >= {AMOUNT_MAX} OR customer_id IS NULL").count() == 0,
      "silver: todas las filas cumplen las reglas de calidad")
masked = read_table(spark, customers_path)
check("name" not in masked.columns and masked.filter(~F.col("email").rlike(r"^[^@]{1,3}\*\*\*@[^@]+$")).count() == 0,
      "silver.customers: sin nombres y con emails enmascarados")

print(f"\nSilver listo para el lote {INGEST_DATE}")
