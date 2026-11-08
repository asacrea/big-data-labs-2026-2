"""
Job Gold: silver → gold (Delta, una tabla por pregunta de negocio).

Las tablas Gold se recalculan completas desde Silver en cada ejecución y se
consultan desde Athena (ver sql/athena.sql).

Argumentos: --BASE <s3://bucket | carpeta local>
"""

from __future__ import annotations

from common import check, get_spark, parse_args, read_table, table_path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

args = parse_args(["BASE"])
BASE = args["BASE"].rstrip("/")
spark = get_spark("paytrack-gold")

txn = read_table(spark, table_path(BASE, "silver", "transactions"))
merchants = read_table(spark, table_path(BASE, "silver", "merchants"))
customers = read_table(spark, table_path(BASE, "silver", "customers"))
fraud = read_table(spark, table_path(BASE, "silver", "fraud_events"))

# ── TODO 1: daily_merchant_summary ───────────────────────────────────────────
# Una fila por DÍA y COMERCIO, para el dashboard de operaciones:
#   day                     fecha de la transacción (date)
#   merchant_id, merchant_name, merchant_category
#   num_transactions        número de transacciones (todos los estados)
#   approved_cop            suma de amount_cop de las aprobadas
#   chargeback_cop          suma de amount_cop de los contracargos (0 si no hay)
#   approval_rate_pct       % de transacciones aprobadas, 2 decimales
#   year, month             columnas de partición ("YYYY", "MM")

#def daily_merchant_summary() -> DataFrame:
#    raise NotImplementedError("TODO 1: daily_merchant_summary")

from pyspark.sql.types import DateType

def daily_merchant_summary() -> DataFrame:
    # 1. Unir especificando las tablas para evitar conflictos de columnas duplicadas
    df_joined = txn.join(merchants, on="merchant_id", how="inner")
    
    # 2. Extraer la fecha y los componentes de tiempo para la partición
    df_with_dates = df_joined.withColumn("day", F.to_date(F.col("event_ts"))) \
                             .withColumn("year", F.date_format(F.col("event_ts"), "yyyy")) \
                             .withColumn("month", F.date_format(F.col("event_ts"), "MM"))
    
    # 3. Agrupar usando las referencias explícitas del DataFrame 'merchants' 
    #    para romper la ambigüedad en las columnas duplicadas
    summary_df = df_with_dates.groupBy(
        F.col("day"),
        F.col("merchant_id"),
        merchants["merchant_name"],
        merchants["merchant_category"],
        F.col("year"),
        F.col("month")
    ).agg(
        # Número total de transacciones (todos los estados)
        F.count("*").alias("num_transactions"),
        
        # Suma de amount_cop solo para transacciones aprobadas
        F.sum(F.when(F.col("status") == "approved", F.col("amount_cop")).otherwise(0)).alias("approved_cop"),
        
        # Suma de amount_cop solo para contracargos (garantiza 0 si no hay ninguno en el grupo)
        F.coalesce(
            F.sum(F.when(F.col("status") == "chargeback", F.col("amount_cop"))), 
            F.lit(0)
        ).alias("chargeback_cop"),
        
        # % de aprobación con 2 decimales: (aprobadas / totales) * 100
        F.round(
            (F.count(F.when(F.col("status") == "approved", 1)) / F.count("*")) * 100, 
            2
        ).alias("approval_rate_pct")
    )
    
    # 4. Renombrar las columnas resultantes para que el output final sea limpio y sin el prefijo del DataFrame
    return summary_df.select(
        "day",
        "merchant_id",
        merchants["merchant_name"].alias("merchant_name"),
        merchants["merchant_category"].alias("merchant_category"),
        "num_transactions",
        "approved_cop",
        "chargeback_cop",
        "approval_rate_pct",
        "year",
        "month"
    )

# ── TODO 2: fraud_weekly_report ──────────────────────────────────────────────
# Una fila por SEMANA, PATRÓN de fraude y SEGMENTO de cliente:
#   week_start              lunes de la semana del evento (date)
#   pattern, segment
#   num_events              número de eventos de fraude
#   avg_risk_score          risk score promedio
#   total_cop               monto total de las transacciones involucradas
# Solo cuentan los eventos cuya transacción está en silver.transactions.

#def fraud_weekly_report() -> DataFrame:
#    raise NotImplementedError("TODO 2: fraud_weekly_report")

def fraud_weekly_report() -> DataFrame:
    # 1. Unimos 'fraud' con 'txn' usando 'transaction_id' (solo eventos con transacción válida)
    df_fraud_txn = fraud.join(txn, on="transaction_id", how="inner")
    
    # 2. Unimos el resultado con 'customers' usando 'customer_id' para obtener la columna 'segment'
    df_complete = df_fraud_txn.join(customers, on="customer_id", how="inner")
    
    # 3. Calculamos el lunes de la semana usando 'event_ts'
    df_with_weeks = df_complete.withColumn(
        "week_start", 
        F.date_trunc("week", F.col("event_ts")).cast(DateType())
    )
    
    # 4. Agrupamos por semana, patrón de fraude y segmento de cliente
    report_df = df_with_weeks.groupBy(
        "week_start",
        "pattern",
        "segment"
    ).agg(
        F.count("*").alias("num_events"),
        F.avg("risk_score").alias("avg_risk_score"),
        F.sum("amount_cop").alias("total_cop")
    )
    
    return report_df

# ── TODO 3: hourly_volume ────────────────────────────────────────────────────
# Una fila por HORA del día (0-23) con las transacciones APROBADAS:
#   hour, num_transactions, total_cop, avg_cop (2 decimales)

#def hourly_volume() -> DataFrame:
#    raise NotImplementedError("TODO 3: hourly_volume")

def hourly_volume() -> DataFrame:

    # 1. Creamos un DataFrame base que contiene obligatoriamente las 24 horas (0-23)
    hours_df = spark.createDataFrame([(h,) for h in range(24)], ["hour"])
    
    # 2. Filtramos solo por transacciones APROBADAS y extraemos la hora (en minúsculas 'approved')
    df_approved = txn.filter(F.col("status") == "approved") \
                     .withColumn("hour", F.hour(F.col("event_ts")))
    
    # 3. Agrupamos las transacciones por hora del día
    tx_counts = df_approved.groupBy("hour").agg(
        F.count("*").alias("num_transactions"),
        F.sum("amount_cop").alias("total_cop"),
        F.round(F.avg("amount_cop"), 2).alias("avg_cop")
    )
    
    # 4. Hacemos un LEFT JOIN desde las 24 horas hacia nuestros conteos reales
    #    y reemplazamos los valores nulos (None/null) por 0 para las horas vacías
    result_df = hours_df.join(tx_counts, on="hour", how="left").na.fill({
        "num_transactions": 0,
        "total_cop": 0,
        "avg_cop": 0.00
    })
    
    return result_df

# ── Ejecución y validación (dadas) ───────────────────────────────────────────

def save(df: DataFrame, name: str, partition_by: list[str] | None = None) -> DataFrame:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(table_path(BASE, "gold", name))
    return read_table(spark, table_path(BASE, "gold", name))


daily = save(daily_merchant_summary(), "daily_merchant_summary", ["year", "month"])
weekly = save(fraud_weekly_report(), "fraud_weekly_report")
hourly = save(hourly_volume(), "hourly_volume")

total = lambda df, col: df.agg(F.sum(col)).first()[0]  # noqa: E731
approved = txn.filter("status = 'approved'")

check(daily.count() == daily.select("day", "merchant_id").distinct().count(), "daily: una fila por día y comercio")
check(total(daily, "num_transactions") == txn.count(), "daily: cuadra el número de transacciones con silver")
check(total(daily, "approved_cop") == total(approved, "amount_cop"), "daily: cuadra el monto aprobado")
check(total(daily, "chargeback_cop") == (total(txn.filter("status = 'chargeback'"), "amount_cop") or 0),
      "daily: cuadra el monto en contracargos")
check(daily.filter((F.length("year") != 4) | (F.length("month") != 2)).count() == 0, "daily: particiones bien formadas")
check(weekly.count() == weekly.select("week_start", "pattern", "segment").distinct().count(),
      "weekly: una fila por semana, patrón y segmento")
check(total(weekly, "num_events") == fraud.join(txn, "transaction_id").count(), "weekly: cuadra el número de eventos")
check(weekly.filter(F.dayofweek("week_start") != 2).count() == 0, "weekly: las semanas empiezan en lunes")
check(hourly.count() == 24, "hourly: 24 horas")
check(total(hourly, "num_transactions") == approved.count(), "hourly: cuadra con las aprobadas de silver")

print("\nGold listo")
