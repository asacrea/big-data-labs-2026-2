import json, glob, os, re
from pyspark.sql import SparkSession, functions as F
spark=(SparkSession.builder.config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
 .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.sql.session.timeZone","UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
B="/workspace/lakehouse-local/lakehouse/bronze/transaction_events"; CK="/workspace/lakehouse-local/checkpoints/metrics/offsets/"
def wm(k): return int(json.loads(open(CK+str(k)).read().splitlines()[1])["batchWatermarkMs"])
# archivos del snapshot versión 6 en el orden del DeltaSource: modificationTime, path
adds=[]
for v in range(0,7):
    for l in open(f"{B}/_delta_log/{v:020d}.json"):
        j=json.loads(l)
        if "add" in j: adds.append((j["add"]["modificationTime"],j["add"]["path"],v))
adds.sort()
print("archivos en versiones 0-6:",len(adds))
lote={}
for i,(m,p,v) in enumerate(adds): lote[p]=min(i//6,3)
df=spark.read.format("delta").load(B).withColumn("f",F.element_at(F.split(F.input_file_name(),"/"),-1))
m=spark.createDataFrame([(k.split("/")[-1],v) for k,v in lote.items()],["f","lote"])
d=df.join(m,"f").withColumn("t",F.col("event_time").cast("double")*1000)
print("filas de la primera corrida:",d.count())
for row in d.groupBy("lote").agg(F.count("*").alias("n"),F.min("t").alias("min"),F.max("t").alias("max")).orderBy("lote").collect():
    k=row["lote"]; 
    t_ev=wm(k)            # watermark vigente del lote
    t_prev=wm(k-1) if k>0 else 0  # watermark de filtrado de tardíos (lote anterior)
    x=d.filter(F.col("lote")==k)
    print(f"lote {k}: n={row['n']} simulado {(row['max']-row['min'])/1000:.0f}s | wm lote={t_ev} tardias<wm_lote={x.filter(F.col('t')<t_ev).count()} | wm anterior={t_prev} tardias<wm_anterior={x.filter(F.col('t')<t_prev).count()}")
# orden real de llegada y retraso
a=spark.read.format("delta").load(B).withColumn("lag",F.col("arrival_ts").cast("double")-F.col("event_time").cast("double"))
for nom,ini,fin in [("en vivo 20/s","2026-11-02 19:47:00","2026-11-02 19:49:20"),("en vivo 50/s","2026-11-02 19:52:50","2026-11-02 19:55:10")]:
    v=a.filter((F.col("arrival_ts")>=ini)&(F.col("arrival_ts")<fin))
    print(nom,"filas",v.count(),"| retraso>30s:",v.filter("lag>30").count(),"| retraso>120s:",v.filter("lag>120").count(),"| retraso>130s:",v.filter("lag>130").count(),"| pagos retraso>120s:",v.filter("lag>120 and event_type='payment'").count())
