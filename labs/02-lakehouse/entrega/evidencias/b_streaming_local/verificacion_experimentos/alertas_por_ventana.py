from pyspark.sql import SparkSession, functions as F
spark=(SparkSession.builder.config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
 .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.sql.session.timeZone","UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
L="/workspace/lakehouse-local/lakehouse/"
al=spark.read.format("delta").load(L+"gold/fraud_alerts"); br=spark.read.format("delta").load(L+"bronze/transaction_events").filter("event_type='payment'")
hit=al.alias("a").join(br.alias("e"),(F.col("a.customer_id")==F.col("e.customer_id"))&(F.col("e.event_time")>=F.col("a.window_start"))&(F.col("e.event_time")<F.col("a.window_end")),"left").groupBy("a.customer_id","a.window_start").agg(F.max(F.col("e.is_fraud").cast("int")).alias("h"))
def rango(n,i,f):
    x=hit.filter((F.col("window_start")>=i)&(F.col("window_start")<f)); c=x.count(); h=x.agg(F.sum("h")).first()[0] or 0
    print(f"rango {n}: alertas={c} aciertos={h}")
print("rango total:",hit.count())
ev=br.withColumn("min",F.date_format("arrival_ts","HH:mm")).groupBy("min").count().orderBy("min").collect()
print("rango llegadas por minuto (UTC):"," ".join(f"{r['min']}={r['count']}" for r in ev))
win=hit.withColumn("min",F.date_format("window_start","HH:mm")).groupBy("min").agg(F.count("*").alias("n"),F.sum("h").alias("h")).orderBy("min").collect()
print("rango alertas por minuto de ventana:"," ".join(f"{r['min']}={r['n']}/{r['h']}" for r in win))
rango("eventos a 20/s (ventanas 19:46-19:50)","2026-11-02 19:46:00","2026-11-02 19:50:00")
rango("eventos a 50/s (ventanas 19:52-19:56)","2026-11-02 19:52:00","2026-11-02 19:56:00")
