"""Datos para el informe: cuarentena por motivo e historial de silver.transactions (local)."""
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

spark = (SparkSession.builder.appName("informe")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
base = "lakehouse-local/lakehouse/silver"

print("\n=== Cuarentena por lote y motivo ===")
(spark.read.format("delta").load(f"{base}/transactions_quarantine")
 .groupBy("ingest_date", "dq_reason").count().orderBy("ingest_date", "dq_reason").show(truncate=False))

print("=== Historial de silver.transactions ===")
h = DeltaTable.forPath(spark, f"{base}/transactions").history()
m = F.col("operationMetrics")
(h.select("version", "operation",
          m["numSourceRows"].alias("src"), m["numTargetRowsInserted"].alias("ins"),
          m["numTargetRowsUpdated"].alias("upd"), m["numTargetRowsCopied"].alias("copied"),
          m["numTargetFilesAdded"].alias("files_add"), m["numTargetFilesRemoved"].alias("files_rm"))
 .orderBy("version").show(200, truncate=False))
print(f"Versiones totales: {h.count()}")