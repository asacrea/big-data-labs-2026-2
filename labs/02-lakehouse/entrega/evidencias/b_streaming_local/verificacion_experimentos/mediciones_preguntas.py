"""Mediciones locales para las preguntas del informe. Solo lee lakehouse-local; las pruebas de escritura van en una copia en /tmp."""
import json, re
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
L = "/workspace/lakehouse-local/lakehouse/"
rd = lambda p: spark.read.format("delta").load(L + p)
txt = open("/workspace/labs/02-lakehouse/entrega/evidencias/b_streaming_local/verificacion_experimentos/latencia_corrida.txt", encoding="utf-8").read()

print("\n### Q2 cuarentena por motivo y lote")
q = rd("silver/transactions_quarantine")
for r in q.groupBy("ingest_date", "dq_reason").count().orderBy("ingest_date", "dq_reason").collect():
    print("Q2", r["ingest_date"], r["dq_reason"], r["count"])
b = rd("bronze/transactions")
for r in b.groupBy("ingest_date").agg(F.count("*").alias("filas"), F.countDistinct("transaction_id").alias("distintas")).orderBy("ingest_date").collect():
    print("Q2 bronze", r["ingest_date"], "filas", r["filas"], "distintas", r["distintas"], "duplicadas", r["filas"] - r["distintas"])
print("Q1 tipos bronze:", [(n, t) for n, t in b.dtypes if n in ("amount_cop", "is_fraud", "timestamp")])

print("\n### Q13 historial de silver.transactions")
h = DeltaTable.forPath(spark, L + "silver/transactions").history().orderBy("version").collect()
print("Q13 versiones", len(h), "operaciones", {o: sum(1 for x in h if x["operation"] == o) for o in {x["operation"] for x in h}})
m = [x for x in h if x["operation"] == "MERGE"]
def met(x, k): return int(x["operationMetrics"].get(k, 0) or 0)
for x in m[:6] + m[-6:]:
    print("Q13 v", x["version"], x["timestamp"], "fuente", met(x, "numSourceRows"), "insert", met(x, "numTargetRowsInserted"), "update", met(x, "numTargetRowsUpdated"), "copiadas", met(x, "numTargetRowsCopied"), "archivos+", met(x, "numTargetFilesAdded"), "archivos-", met(x, "numTargetFilesRemoved"))
strm = [x for x in m if met(x, "numSourceRows") and x["version"] >= 5]
tot = sum(met(x, "numTargetRowsCopied") for x in strm); src = sum(met(x, "numSourceRows") for x in strm)
print("Q13 merges de stream", len(strm), "filas fuente", src, "copiadas total", tot, "promedio copiadas por merge", tot // max(len(strm), 1), "max", max(met(x, "numTargetRowsCopied") for x in strm))
print("Q13 merges sin cambios", sum(1 for x in strm if met(x, "numTargetRowsInserted") + met(x, "numTargetRowsUpdated") == 0))
files = spark.read.format("delta").load(L + "silver/transactions").select(F.input_file_name().alias("f")).distinct().count()
print("Q13 archivos parquet activos", files)

print("\n### Q11 contracargos con un evento posterior de la misma transacción")
ev = rd("bronze/transaction_events")
cb = ev.filter("event_type='chargeback'").select("transaction_id", F.col("arrival_ts").alias("cb_llega"), F.col("event_time").alias("cb_t"))
otros = ev.filter("status <> 'chargeback'").select("transaction_id", "arrival_ts", "event_time")
j = cb.join(otros, "transaction_id")
print("Q11 contracargos", cb.count(), "| de pagos del stream", cb.filter("transaction_id like 'TXNS%'").select("transaction_id").distinct().count())
print("Q11 evento no-contracargo que llega después del contracargo", j.filter("arrival_ts > cb_llega").count())
print("Q11 de esos, 10 s o más después (otro micro-lote)", j.filter(F.col("arrival_ts").cast("double") - F.col("cb_llega").cast("double") >= 10).select("transaction_id").distinct().count())
print("Q11 silver: contracargos en silver", rd("silver/transactions").filter("status='chargeback'").count())

print("\n### Q10 umbral de alerta (pagos sin repetidos, ventanas de 30 s cada 10 s)")
pagos = ev.filter("event_type='payment'").dropDuplicates(["event_id"])
for nom, ini, fin in [("20/s", "2026-11-02 19:47:00", "2026-11-02 19:49:10"), ("50/s", "2026-11-02 19:52:50", "2026-11-02 19:55:00")]:
    p = pagos.filter((F.col("event_time") >= ini) & (F.col("event_time") < fin))
    w = p.groupBy(F.window("event_time", "30 seconds", "10 seconds"), "customer_id").agg(F.count("*").alias("n"), F.max(F.col("is_fraud").cast("int")).alias("h"))
    fraude = p.filter("is_fraud").select("customer_id").distinct().count()
    for k in (6, 7, 8, 9):
        x = w.filter(F.col("n") >= k); a = x.count(); hh = x.agg(F.sum("h")).first()[0] or 0
        det = x.filter("h=1").select("customer_id").distinct().count()
        print(f"Q10 {nom} umbral>={k}: alertas={a} aciertos={hh} precision={100*hh/max(a,1):.1f}% clientes con fraude detectados={det}/{fraude}")
    w15 = p.groupBy(F.window("event_time", "15 seconds", "5 seconds"), "customer_id").agg(F.count("*").alias("n"), F.max(F.col("is_fraud").cast("int")).alias("h"))
    x = w15.filter(F.col("n") >= 6); a = x.count(); hh = x.agg(F.sum("h")).first()[0] or 0; det = x.filter("h=1").select("customer_id").distinct().count()
    print(f"Q10 {nom} ventana 15 s cada 5 s, umbral>=6: alertas={a} aciertos={hh} precision={100*hh/max(a,1):.1f}% clientes con fraude detectados={det}/{fraude}")

print("\n### Q9 latencia de alertas")
ini = re.search(r"productor arranca UTC (\S+ \S+)", txt).group(1); fin = re.search(r"productor termina UTC (\S+ \S+)", txt).group(1)
print("Q9 productor", ini, "->", fin)
A = L + "gold/fraud_alerts"
adds = {}
for f in spark.sparkContext.wholeTextFiles(A + "/_delta_log/*.json").collect():
    for l in f[1].splitlines():
        d = json.loads(l)
        if "add" in d: adds[d["add"]["path"].split("/")[-1]] = d["add"]["modificationTime"]
tm = spark.createDataFrame([(k, v / 1000.0) for k, v in adds.items()], ["f", "escrita"])
al = rd("gold/fraud_alerts").withColumn("f", F.element_at(F.split(F.input_file_name(), "/"), -1)).join(tm, "f").filter((F.col("window_start") >= ini) & (F.col("window_start") < fin))
pay = pagos.select("customer_id", F.col("event_time").alias("t"), "is_fraud")
x = al.alias("a").join(pay.alias("p"), (F.col("a.customer_id") == F.col("p.customer_id")) & (F.col("p.t") >= F.col("a.window_start")) & (F.col("p.t") < F.col("a.window_end")))
y = x.groupBy("a.customer_id", "a.window_start", "a.window_end", "a.escrita").agg(F.max("p.t").alias("ultimo"), F.max(F.col("p.is_fraud").cast("int")).alias("h")) \
    .withColumn("lat", F.col("escrita") - F.col("ultimo").cast("double")).withColumn("trasfin", F.col("escrita") - F.col("window_end").cast("double"))
print("Q9 alertas de la corrida", y.count(), "aciertos", y.agg(F.sum("h")).first()[0])
fin_ts = spark.sql(f"select cast(timestamp'{fin}' as double) t").first()["t"]
for nom, c in [("todas", F.lit(True)), ("con fraude", F.col("h") == 1), ("escritas con el productor activo", F.col("escrita") <= fin_ts), ("con fraude y productor activo", (F.col("h") == 1) & (F.col("escrita") <= fin_ts))]:
    s = y.filter(c)
    r = s.agg(F.count("*").alias("n"), F.min("lat").alias("min"), F.expr("percentile_approx(lat,0.5)").alias("med"), F.max("lat").alias("max"), F.expr("percentile_approx(trasfin,0.5)").alias("fin")).first()
    if r["n"]:
        print(f"Q9 {nom}: n={r['n']} ultimo pago -> alerta min={r['min']:.0f}s mediana={r['med']:.0f}s max={r['max']:.0f}s | cierre de ventana -> alerta mediana={r['fin']:.0f}s")

print("\n### Q3 MERGE con y sin la condición, sobre una copia")
src = rd("silver/transactions").filter("ingest_date='2026-09-30' and transaction_id not like 'TXNS%'")
print("Q3 filas fuente (lote 2 ya integrado)", src.count())
for nombre, cond in [("con condición", "t.status <> s.status"), ("sin condición", None)]:
    path = "/tmp/copia_silver_" + nombre[:3]
    rd("silver/transactions").write.format("delta").mode("overwrite").save(path)
    dt = DeltaTable.forPath(spark, path)
    b4 = spark.read.format("delta").load(path).select("transaction_id", F.col("status").alias("old_status"))
    mm = dt.alias("t").merge(src.alias("s"), "t.transaction_id = s.transaction_id")
    upd = {"status": "s.status", "is_fraud": "s.is_fraud", "silver_updated_at": "s.silver_updated_at"}
    mm = mm.whenMatchedUpdate(condition=cond, set=upd) if cond else mm.whenMatchedUpdate(set=upd)
    mm.whenNotMatchedInsertAll().execute()
    hh = dt.history(1).first()
    af = spark.read.format("delta").load(path).select("transaction_id", F.col("status").alias("new_status"))
    cambios = b4.join(af, "transaction_id").filter("old_status <> new_status").count()
    print(f"Q3 {nombre}: reportadas como actualizadas={met(hh,'numTargetRowsUpdated')} | cambios reales de status={cambios}")
