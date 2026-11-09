"""
Corre labs/02-lakehouse/sql/athena.sql en Athena, sentencia por sentencia (apoyo para el informe).

Para cada sentencia imprime el estado, los MB escaneados, el tiempo del motor y, en las
consultas, las filas del resultado. El DDL usa IF NOT EXISTS, así que se puede repetir.

Uso (desde la raíz del repositorio, con las credenciales de .envs/aws.env cargadas):
    uv run python data/analisis/athena_consultas.py | tee data/analisis/athena_resultados.txt
"""

from __future__ import annotations

import re
import time
from pathlib import Path

import boto3

SQL = Path("labs/02-lakehouse/sql/athena.sql")
WORKGROUP = "paytrack-lakehouse-wg"
DATABASE = "paytrack_lakehouse"
MAX_FILAS = 30

# Consultas de apoyo (no son del laboratorio): de dónde salen los contracargos que hay en silver
EXTRA = [
    ("Extra: silver por origen y status", """
SELECT CASE WHEN transaction_id LIKE 'TXNS%' THEN 'streaming' ELSE 'batch' END AS origen,
       status, COUNT(*) AS transacciones
FROM paytrack_lakehouse.silver_transactions
GROUP BY 1, 2
ORDER BY 1, 2"""),
    ("Extra: contracargos de transacciones batch por mes", """
SELECT year, month, COUNT(*) AS contracargos, SUM(amount_cop) AS monto_cop
FROM paytrack_lakehouse.silver_transactions
WHERE transaction_id NOT LIKE 'TXNS%' AND status = 'chargeback'
GROUP BY 1, 2
ORDER BY 1, 2"""),
]


def sentencias(texto: str) -> list[tuple[str, str]]:
    """Parte el archivo en sentencias (un ';' al final de la línea las cierra) y les pone etiqueta."""
    salida, todo = [], None
    for bloque in re.split(r";[ \t]*\n", texto):
        if m := re.search(r"TODO (\d)", bloque):
            todo = m.group(1)
        sql = "\n".join(re.sub(r"--.*$", "", linea).rstrip() for linea in bloque.splitlines()).strip()
        if not sql:
            continue
        if sql.upper().startswith("CREATE"):
            etiqueta = "DDL " + re.search(r"paytrack_lakehouse\.(\w+)", sql).group(1)
        elif "por corrida" in bloque:
            etiqueta = f"TODO {todo} por corrida"
        else:
            etiqueta = f"TODO {todo}"
        salida.append((etiqueta, sql))
    return salida


def correr(athena, sql: str) -> dict:
    qid = athena.start_query_execution(
        QueryString=sql, WorkGroup=WORKGROUP, QueryExecutionContext={"Database": DATABASE}
    )["QueryExecutionId"]
    while True:
        ejecucion = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if ejecucion["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return ejecucion
        time.sleep(1)


def tabla(athena, qid: str) -> str:
    filas = athena.get_query_results(QueryExecutionId=qid, MaxResults=MAX_FILAS + 1)["ResultSet"]["Rows"]
    valores = [[c.get("VarCharValue", "") for c in f["Data"]] for f in filas]
    if not valores:
        return "  (sin filas)"
    anchos = [max(len(v[i]) for v in valores) for i in range(len(valores[0]))]
    lineas = ["  " + " | ".join(v.ljust(a) for v, a in zip(fila, anchos)) for fila in valores]
    lineas.insert(1, "  " + "-+-".join("-" * a for a in anchos))
    return "\n".join(lineas)


if __name__ == "__main__":
    athena = boto3.client("athena")
    for etiqueta, sql in sentencias(SQL.read_text()) + EXTRA:
        e = correr(athena, sql)
        estado = e["Status"]["State"]
        stats = e.get("Statistics", {})
        escaneados = stats.get("DataScannedInBytes", 0)
        ms = stats.get("EngineExecutionTimeInMillis", 0)
        print(f"\n━━━ {etiqueta}: {estado} · {escaneados:,} bytes ({escaneados / 1e6:.3f} MB) escaneados · {ms} ms")
        if estado != "SUCCEEDED":
            print("  " + e["Status"].get("StateChangeReason", ""))
        elif not etiqueta.startswith("DDL"):
            print(tabla(athena, e["QueryExecutionId"]))
