"""
Transforma los datos del esquema OLTP (oltp.*) al esquema estrella (dw.*).

El proceso es ELT: los datos ya están en PostgreSQL, así que cada paso es un
INSERT ... SELECT que lee de oltp.* y escribe en dw.*.

Uso: python labs/01-modelado/load_star.py

Archivos utilizados:
  1. sql/star_schema.sql: diseño dimensional.
  2. SQL_DIM_* y SQL_FACT: carga inicial completa.
Al final se ejecuta verify(), que valida tu carga. No lo modifiques.
"""

import os
from pathlib import Path

import psycopg2
from rich.console import Console

console = Console()

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/paytrack")
SQL_DIR = Path(__file__).parent / "sql"


# ── Dimensiones de calendario (dadas) ────────────────────────────────────────
# dim_date cubre todo el rango de fechas de las transacciones.
# Convención: day_of_week usa EXTRACT(DOW) de PostgreSQL → 0 = domingo, 6 = sábado.

SQL_DIM_DATE = """
INSERT INTO dw.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT          AS date_key,
    d::DATE                              AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT    AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT    AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT    AS month,
    TO_CHAR(d, 'TMMonth')                AS month_name,
    EXTRACT(WEEK    FROM d)::SMALLINT    AS week_of_year,
    EXTRACT(DAY     FROM d)::SMALLINT    AS day_of_month,
    EXTRACT(DOW     FROM d)::SMALLINT    AS day_of_week,
    TO_CHAR(d, 'TMDay')                  AS day_name,
    EXTRACT(DOW FROM d) IN (0, 6)        AS is_weekend,
    FALSE                                AS is_holiday_co
FROM generate_series(
    (SELECT MIN(created_at)::DATE FROM oltp.transactions),
    (SELECT MAX(created_at)::DATE FROM oltp.transactions),
    INTERVAL '1 day'
) AS d;
"""

SQL_DIM_TIME = """
INSERT INTO dw.dim_time
SELECT
    h * 100 + m AS time_key,  -- HHMM
    h           AS hour,
    m           AS minute,
    CASE
        WHEN h BETWEEN  6 AND 11 THEN 'morning'
        WHEN h BETWEEN 12 AND 17 THEN 'afternoon'
        WHEN h BETWEEN 18 AND 22 THEN 'evening'
        ELSE 'night'
    END         AS period_of_day
FROM generate_series(0, 23) AS h, generate_series(0, 59) AS m;
"""


# ── 1: dim_customer ─────────────────────────────────────────────────────
# Carga un registro por cliente desde oltp.customers.
# - La surrogate key (customer_key) la genera la base de datos: no la insertes.
# - Es una dimensión SCD Tipo 2. En esta carga inicial cada cliente tiene una
#   sola versión: ¿qué valores deben tener valid_from, valid_to e is_current?
SQL_DIM_CUSTOMER = """
INSERT INTO dw.dim_customer
    (customer_id, city, segment, valid_from, valid_to, is_current)
SELECT customer_id, city, segment, '-infinity'::TIMESTAMP, NULL, TRUE
FROM oltp.customers;
"""
# Snapshot inicial: no existe historia de cambios en la fuente. La primera
# versión cubre los hechos disponibles; -infinity no inventa una fecha de cambio.


# ── 2: dim_merchant ─────────────────────────────────────────────────────
# Un registro por comercio desde oltp.merchants.
SQL_DIM_MERCHANT = """
INSERT INTO dw.dim_merchant (merchant_id, name, category, city)
SELECT merchant_id, name, category, city FROM oltp.merchants;
"""


# ── 3: dim_payment_method ───────────────────────────────────────────────
# Un registro por método de pago desde oltp.payment_methods.
# Enriquece la dimensión con method_type, un atributo que NO existe en el OLTP:
#   credit_card, debit_card → 'card'
#   nequi, daviplata        → 'digital_wallet'
#   pse                     → 'bank_transfer'
SQL_DIM_PAYMENT_METHOD = """
INSERT INTO dw.dim_payment_method (method_name, method_type)
SELECT method_name,
       CASE
           WHEN method_name IN ('credit_card', 'debit_card') THEN 'card'
           WHEN method_name IN ('nequi', 'daviplata') THEN 'digital_wallet'
           WHEN method_name = 'pse' THEN 'bank_transfer'
           ELSE 'other'
       END
FROM oltp.payment_methods;
"""


# ── 4: fact_transactions ────────────────────────────────────────────────
# Grano: una fila por transacción de oltp.transactions.
# Para cada transacción debes:
# - Traducir cada natural key (customer_id, merchant_id, method_id) a la
#   surrogate key de su dimensión (JOIN contra la dimensión).
#   Ojo con dim_customer: es SCD2, la transacción debe apuntar a la versión
#   del cliente vigente en la fecha de la transacción.
# - Calcular date_key (YYYYMMDD) y time_key (HHMM) a partir de created_at.
# - Convertir status a una medida booleana is_approved.
# - Traer el risk_score de oltp.fraud_events cuando exista (no todas las
#   transacciones tienen evento de fraude: ¿qué tipo de JOIN necesitas?).
SQL_FACT = """
INSERT INTO dw.fact_transactions
    (transaction_id, date_key, time_key, customer_key, merchant_key,
     payment_method_key, amount_cop, is_fraud, is_approved, fraud_risk_score)
SELECT t.transaction_id, TO_CHAR(t.created_at, 'YYYYMMDD')::INT,
       EXTRACT(HOUR FROM t.created_at)::INT * 100 + EXTRACT(MINUTE FROM t.created_at)::INT,
       c.customer_key, m.merchant_key, p.payment_method_key,
       t.amount_cop, t.is_fraud, t.status = 'approved', e.fraud_risk_score
FROM oltp.transactions t
JOIN dw.dim_customer c ON c.customer_id = t.customer_id
    AND t.created_at >= c.valid_from
    AND (c.valid_to IS NULL OR t.created_at < c.valid_to)
JOIN dw.dim_merchant m ON m.merchant_id = t.merchant_id
JOIN oltp.payment_methods pm ON pm.method_id = t.method_id
JOIN dw.dim_payment_method p ON p.method_name = pm.method_name
LEFT JOIN (
    SELECT transaction_id, MAX(risk_score) AS fraud_risk_score
    FROM oltp.fraud_events
    GROUP BY transaction_id
) e ON e.transaction_id = t.transaction_id;
"""


# ── Ejecución (dado) ─────────────────────────────────────────────────────────

STEPS = [
    ("dim_date", SQL_DIM_DATE),
    ("dim_time", SQL_DIM_TIME),
    ("dim_customer", SQL_DIM_CUSTOMER),
    ("dim_merchant", SQL_DIM_MERCHANT),
    ("dim_payment_method", SQL_DIM_PAYMENT_METHOD),
    ("fact_transactions", SQL_FACT),
]


def scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def verify(cur) -> None:
    """Validaciones de la carga. Si alguna falla, revisa tu SQL."""
    checks = [
        (
            "dim_customer tiene un registro vigente por cliente",
            "SELECT COUNT(*) FROM oltp.customers",
            "SELECT COUNT(*) FROM dw.dim_customer WHERE is_current",
        ),
        (
            "dim_merchant tiene todos los comercios",
            "SELECT COUNT(*) FROM oltp.merchants",
            "SELECT COUNT(*) FROM dw.dim_merchant",
        ),
        (
            "dim_payment_method tiene todos los métodos",
            "SELECT COUNT(*) FROM oltp.payment_methods",
            "SELECT COUNT(*) FROM dw.dim_payment_method WHERE method_type IS NOT NULL",
        ),
        (
            "fact_transactions tiene una fila por transacción",
            "SELECT COUNT(*) FROM oltp.transactions",
            "SELECT COUNT(*) FROM dw.fact_transactions",
        ),
        (
            "el monto total cuadra",
            "SELECT SUM(amount_cop) FROM oltp.transactions",
            "SELECT SUM(amount_cop) FROM dw.fact_transactions",
        ),
        (
            "el número de fraudes cuadra",
            "SELECT COUNT(*) FROM oltp.transactions WHERE is_fraud",
            "SELECT COUNT(*) FROM dw.fact_transactions WHERE is_fraud",
        ),
        (
            "el número de transacciones aprobadas cuadra",
            "SELECT COUNT(*) FROM oltp.transactions WHERE status = 'approved'",
            "SELECT COUNT(*) FROM dw.fact_transactions WHERE is_approved",
        ),
        (
            "toda transacción tiene una fecha y hora válidas en las dimensiones",
            "SELECT COUNT(*) FROM dw.fact_transactions",
            """SELECT COUNT(*) FROM dw.fact_transactions f
               JOIN dw.dim_date d ON f.date_key = d.date_key
               JOIN dw.dim_time t ON f.time_key = t.time_key""",
        ),
        (
            "toda transacción tiene cliente, comercio y método en las dimensiones",
            "SELECT COUNT(*) FROM dw.fact_transactions",
            """SELECT COUNT(*) FROM dw.fact_transactions f
               JOIN dw.dim_customer       c ON f.customer_key       = c.customer_key
               JOIN dw.dim_merchant       m ON f.merchant_key       = m.merchant_key
               JOIN dw.dim_payment_method p ON f.payment_method_key = p.payment_method_key""",
        ),
    ]

    if not scalar(cur, "SELECT COUNT(*) FROM oltp.transactions"):
        raise SystemExit("oltp.transactions está vacía: ejecuta primero load_oltp.py")

    failed = 0
    for name, expected_sql, actual_sql in checks:
        expected, actual = scalar(cur, expected_sql), scalar(cur, actual_sql)
        if expected == actual:
            console.print(f"  [green]✓[/green] {name}")
        else:
            failed += 1
            console.print(f"  [red]✗ {name}: esperado {expected}, obtenido {actual}[/red]")

    if failed:
        raise SystemExit(f"\n{failed} validaciones fallaron.")
    console.print("\n[bold green]¡Esquema estrella cargado y validado![/bold green]")


def load_star() -> None:
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # El script es idempotente: borra y recrea el esquema dw en cada ejecución
    cur.execute("DROP SCHEMA IF EXISTS dw CASCADE")
    cur.execute((SQL_DIR / "star_schema.sql").read_text())
    conn.commit()
    console.print("[green]✓ Esquema estrella creado[/green]")

    for table, sql in STEPS:
        if "TODO" in sql:
            raise SystemExit(f"Falta completar el SQL de {table} (busca el TODO).")
        cur.execute(sql)
        console.print(f"[green]✓ {table}: {cur.rowcount} filas[/green]")
    conn.commit()

    console.print("\n[bold]Validando la carga...[/bold]")
    verify(cur)

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_star()
