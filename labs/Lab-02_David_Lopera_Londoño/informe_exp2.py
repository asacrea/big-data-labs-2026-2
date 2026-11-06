"""Experimento 2: precisión de las alertas a 20 y a 50 eventos/s (lakehouse local)."""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("exp2")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "America/Bogota")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
base = "lakehouse-local/lakehouse"
CUT = F.lit("2026-11-04 16:00:00").cast("timestamp")

def corrida(col):
    return F.when(F.col(col) < CUT, "20 ev/s").otherwise("50 ev/s")

def resumen(df, titulo):
    print(f"\n=== {titulo} ===")
    (df.groupBy("corrida")
     .agg(F.count("*").alias("alertas"), F.sum("hit").alias("aciertos"),
          F.round(100 * F.sum("hit") / F.count("*"), 2).alias("precision_pct"),
          F.min("window_start").alias("desde"), F.max("window_start").alias("hasta"))
     .orderBy("corrida").show(truncate=False))

events = spark.read.format("delta").load(f"{base}/bronze/transaction_events").dropDuplicates(["event_id"])
pays = events.filter("event_type = 'payment'")

alerts = spark.read.format("delta").load(f"{base}/gold/fraud_alerts").alias("a")
emitted = (alerts.join(pays.alias("e"), (F.col("e.customer_id") == F.col("a.customer_id"))
                       & (F.col("e.event_time") >= F.col("a.window_start"))
                       & (F.col("e.event_time") < F.col("a.window_end")), "left")
           .groupBy("a.window_start", "a.customer_id")
           .agg(F.max(F.col("e.is_fraud").cast("int")).alias("hit"))
           .withColumn("corrida", corrida("window_start")))
resumen(emitted, "A) Alertas emitidas por el streaming (gold.fraud_alerts)")

rule = (pays.groupBy(F.window("event_time", "30 seconds", "10 seconds"), "customer_id")
        .agg(F.count("*").alias("n"), F.max(F.col("is_fraud").cast("int")).alias("hit"))
        .filter("n >= 6")
        .select(F.col("window.start").alias("window_start"), "customer_id", "hit")
        .withColumn("corrida", corrida("window_start")))
resumen(rule, "B) Regla aplicada en batch sobre todos los pagos de bronze")

print("=== Pagos y clientes distintos por corrida ===")
(pays.withColumn("corrida", corrida("event_time")).groupBy("corrida")
 .agg(F.count("*").alias("pagos"), F.countDistinct("customer_id").alias("clientes"),
      F.sum(F.col("is_fraud").cast("int")).alias("pagos_fraude"))
 .orderBy("corrida").show(truncate=False))
