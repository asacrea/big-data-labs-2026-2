"""
Corre sql/athena.sql en Athena (workgroup paytrack-lakehouse-wg) y guarda cada resultado con sus MB escaneados.

Primero el DDL dado (registra las tablas Delta) y luego los TODO 1 a 6, en orden. Si una consulta
falla, termina con error (el script que lo llama desmonta la infraestructura).

Uso: python labs/02-lakehouse/entrega/aws/athena_cli.py --bucket <bucket> --out <archivo.md>
"""

import argparse
import re
import time
from pathlib import Path

import boto3

SQL = Path(__file__).parents[2] / "sql" / "athena.sql"
WORKGROUP, DATABASE = "paytrack-lakehouse-wg", "paytrack_lakehouse"
athena = boto3.client("athena")


def run(query: str) -> tuple[list[list[str]], float]:
    """Ejecuta una consulta y devuelve (filas con encabezado, MB escaneados)."""
    qid = athena.start_query_execution(QueryString=query, WorkGroup=WORKGROUP,
                                       QueryExecutionContext={"Database": DATABASE})["QueryExecutionId"]
    while True:
        ex = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise SystemExit(f"✗ Athena {state}: {ex['Status'].get('StateChangeReason', '')}\n{query[:300]}")
    mb = ex["Statistics"].get("DataScannedInBytes", 0) / 1e6
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue", "") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    return rows, mb


def markdown(rows: list[list[str]]) -> str:
    if not rows:
        return "_(sin filas)_"
    head, *body = rows
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    lines += ["| " + " | ".join(r) + " |" for r in body] or ["| " + " | ".join("" for _ in head) + " |"]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    sql = SQL.read_text(encoding="utf-8").replace("<BUCKET>", args.bucket)
    for ddl in re.findall(r"CREATE EXTERNAL TABLE.*?;", sql, flags=re.S):
        run(ddl.rstrip(";"))
    print(f"  ✓ {len(re.findall('CREATE EXTERNAL TABLE', sql))} tablas Delta registradas")

    out = [f"# Resultados de Athena (workgroup `{WORKGROUP}`, {time.strftime('%Y-%m-%d %H:%M')})", ""]
    parts = re.split(r"-- ── (TODO \d[^─]*)─+\n", sql)[1:]
    for name, body in zip(parts[::2], parts[1::2]):
        query = "\n".join(l for l in body.splitlines() if not l.startswith("--")).strip().rstrip(";")
        rows, mb = run(query)
        print(f"  ✓ {name.strip()}: {len(rows) - 1} filas · {mb:.2f} MB escaneados")
        out += [f"## {name.strip()} · {mb:.2f} MB escaneados", "", markdown(rows), ""]
    args.out.write_text("\n".join(out), encoding="utf-8")
    print(f"  → {args.out}")


if __name__ == "__main__":
    main()
