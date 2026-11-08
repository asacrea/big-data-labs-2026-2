import json, boto3
p = boto3.client("pricing", region_name="us-east-1")
LOC = "US East (N. Virginia)"
def prods(svc, **attrs):
    f = [{"Type": "TERM_MATCH", "Field": "location", "Value": LOC}] + [{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in attrs.items()]
    for page in p.get_paginator("get_products").paginate(ServiceCode=svc, Filters=f):
        for s in page["PriceList"]:
            yield json.loads(s)
def show(j):
    a = j["product"]["attributes"]
    for t in j["terms"].get("OnDemand", {}).values():
        for d in t["priceDimensions"].values():
            usd = float(d["pricePerUnit"].get("USD", 0))
            if usd:
                print(f"{usd:<12g} USD/{d['unit']:<14} {a.get('usagetype','')[:42]:<42} | {d['description'][:80]}")
print("== Kinesis")
for j in prods("AmazonKinesis"):
    u = j["product"]["attributes"].get("usagetype", "")
    if u in ("ShardHour", "PutRequestPayloadUnits", "PutRequestPayloadUnit") or "Shard" in u or "PutRequest" in u:
        show(j)
print("== Glue")
for j in prods("AWSGlue"):
    u = j["product"]["attributes"].get("usagetype", "")
    if "ETL-DPU-Hour" in u or "Streaming" in u or "DPU" in u and "Flex" not in u and "Interactive" not in u and "DataBrew" not in u:
        show(j)
print("== Athena")
for j in prods("AmazonAthena"):
    if "DataScanned" in j["product"]["attributes"].get("usagetype", ""):
        show(j)
print("== S3")
for j in prods("AmazonS3", storageClass="General Purpose", volumeType="Standard"):
    u = j["product"]["attributes"].get("usagetype", "")
    if u == "TimedStorage-ByteHrs":
        show(j)
