"""
Carga los datos generados (JSON) al esquema OLTP de PostgreSQL.
Uso: python labs/01-modelado/load_oltp.py [--data-dir data/]
"""

import argparse
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch
from rich.console import Console
from rich.progress import track

console = Console()

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/paytrack")


def load_oltp(data_dir: Path) -> None:
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # 1. Crear esquema (idempotente: borra y recrea oltp en cada ejecución)
    cur.execute("DROP SCHEMA IF EXISTS oltp CASCADE")
    with open(Path(__file__).parent / "sql/oltp_schema.sql") as f:
        cur.execute(f.read())
    conn.commit()
    console.print("[green]✓ Esquema OLTP creado[/green]")

    # 2. Cargar catálogos
    merchants = json.loads((data_dir / "merchants.json").read_text())
    execute_batch(cur,
        "INSERT INTO oltp.merchants VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        [(m["id"], m["name"], m["category"], m["city"]) for m in merchants],
    )

    payment_methods = ["credit_card", "debit_card", "nequi", "daviplata", "pse"]
    execute_batch(cur,
        "INSERT INTO oltp.payment_methods(method_name) VALUES (%s) ON CONFLICT DO NOTHING",
        [(m,) for m in payment_methods],
    )
    conn.commit()

    # Mapa method_name → id
    cur.execute("SELECT method_id, method_name FROM oltp.payment_methods")
    method_map = {name: mid for mid, name in cur.fetchall()}

    # 3. Cargar clientes
    customers = json.loads((data_dir / "customers.json").read_text())
    execute_batch(cur,
        """INSERT INTO oltp.customers VALUES (%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT DO NOTHING""",
        [(c["customer_id"], c["name"], c["email"], c["city"],
          c["segment"], c["monthly_income_cop"], c["created_at"])
         for c in track(customers, description="Cargando clientes...")],
        page_size=500,
    )
    conn.commit()
    console.print(f"[green]✓ {len(customers)} clientes cargados[/green]")

    # 4. Cargar transacciones
    transactions = json.loads((data_dir / "transactions.json").read_text())
    execute_batch(cur,
        """INSERT INTO oltp.transactions
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        [(t["transaction_id"], t["customer_id"], t["merchant_id"],
          method_map[t["payment_method"]], t["amount_cop"],
          t["status"], t["is_fraud"], t["timestamp"])
         for t in track(transactions, description="Cargando transacciones...")],
        page_size=1000,
    )
    conn.commit()
    console.print(f"[green]✓ {len(transactions)} transacciones cargadas[/green]")

    # 5. Cargar eventos de fraude
    fraud_events = json.loads((data_dir / "fraud_events.json").read_text())
    execute_batch(cur,
        """INSERT INTO oltp.fraud_events VALUES (%s,%s,%s,%s,%s)
           ON CONFLICT DO NOTHING""",
        [(e["event_id"], e["transaction_id"], e["pattern"],
          e["risk_score"], e["detected_at"])
         for e in fraud_events],
    )
    conn.commit()
    console.print(f"[green]✓ {len(fraud_events)} eventos de fraude cargados[/green]")

    cur.close()
    conn.close()
    console.print("\n[bold green]¡Carga OLTP completada![/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data", type=Path)
    args = parser.parse_args()
    load_oltp(args.data_dir)
