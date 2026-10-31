"""
Autoevaluación: compara los resultados de las consultas OLTP (oltp_problems.sql)
con tus consultas sobre el esquema estrella (olap_queries.sql).

Uso: python labs/01-modelado/check_queries.py

Ejecuta cada consulta sin el EXPLAIN ANALYZE y verifica que ambas versiones
devuelvan exactamente las mismas filas. No modifiques este archivo.
"""

import os
import re
from decimal import Decimal
from pathlib import Path

import psycopg2
from rich.console import Console

console = Console()

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/paytrack")
SQL_DIR = Path(__file__).parent / "sql"


def read_queries(path: Path) -> list[str]:
    """Divide el archivo en sentencias y quita comentarios y EXPLAIN ANALYZE."""
    text = re.sub(r"--[^\n]*", "", path.read_text())
    statements = [s.strip() for s in text.split(";")]
    statements = statements[:3]  # Q1, Q2, Q3
    return [re.sub(r"^\s*EXPLAIN\s+ANALYZE\s+", "", s, flags=re.IGNORECASE) for s in statements]


def normalize(rows: list[tuple]) -> list[tuple]:
    """Redondea los números para comparar sin importar el tipo (INT, NUMERIC...)."""
    def norm(v):
        if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
            return round(float(v), 2)
        return v
    return sorted((tuple(norm(v) for v in row) for row in rows), key=repr)


def main() -> None:
    conn = psycopg2.connect(DB_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor()

    oltp = read_queries(SQL_DIR / "oltp_problems.sql")
    olap = read_queries(SQL_DIR / "olap_queries.sql")

    passed = 0
    for i, (q_oltp, q_olap) in enumerate(zip(oltp, olap), start=1):
        if not q_olap:
            console.print(f"  [yellow]… Q{i}: sin implementar[/yellow]")
            continue
        if re.search(r"\boltp\.", q_olap, re.IGNORECASE):
            console.print(f"  [red]✗ Q{i}: usa tablas oltp.* (solo se permiten dw.*)[/red]")
            continue
        try:
            cur.execute(q_oltp)
            cols_oltp, rows_oltp = [d.name for d in cur.description], cur.fetchall()
            cur.execute(q_olap)
            cols_olap, rows_olap = [d.name for d in cur.description], cur.fetchall()
        except psycopg2.Error as e:
            conn.rollback()
            console.print(f"  [red]✗ Q{i}: error al ejecutar → {e.pgerror or e}[/red]")
            continue

        if not rows_oltp:
            console.print(f"  [yellow]… Q{i}: la versión OLTP no devuelve filas; ¿cargaste los datos?[/yellow]")
        elif cols_oltp != cols_olap:
            console.print(f"  [red]✗ Q{i}: columnas distintas\n      OLTP: {cols_oltp}\n      OLAP: {cols_olap}[/red]")
        elif normalize(rows_oltp) != normalize(rows_olap):
            console.print(
                f"  [red]✗ Q{i}: los resultados no coinciden "
                f"({len(rows_oltp)} filas OLTP vs {len(rows_olap)} filas OLAP)[/red]"
            )
        else:
            passed += 1
            console.print(f"  [green]✓ Q{i}: {len(rows_olap)} filas idénticas[/green]")

    console.print(f"\n[bold]{passed}/3 consultas correctas[/bold]")
    cur.close()
    conn.close()
    if passed < 3:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
