"""
Parte D · verificación estricta de exactitud OLTP vs OLAP (archivo nuestro, no del profesor).

check_queries.py ya valida el paso 2 del README; para comparar, redondea los
números a 2 decimales y ordena las filas. Este script complementa esa
validación con una comparación más estricta sobre las mismas consultas
(lee oltp_problems.sql y olap_queries.sql igual que check_queries.py):
  * nombres y orden de columnas,
  * mismo número de filas y mismo ORDER BY (fila i contra fila i),
  * cada valor exactamente igual, sin redondear (Decimal contra Decimal),
  * sumas de control: COUNT y SUM(total_cop) del resultado completo.
Muestra los dos resultados lado a lado para verlos con los propios ojos.

Uso: python labs/01-modelado/verify_exactitud.py
"""

import os
import re
from decimal import Decimal
from pathlib import Path

import psycopg2
from rich.console import Console
from rich.table import Table

console = Console(width=int(os.getenv("ANCHO_EVIDENCIA", "140")))  # ancho fijo: la evidencia sale completa

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/paytrack")
SQL_DIR = Path(__file__).parent / "sql"
SHOW_ROWS = 7  # filas que se muestran lado a lado por consulta (Q1 completa)


def read_queries(path: Path) -> list[str]:
    """Mismo criterio que check_queries.py: quita comentarios y EXPLAIN ANALYZE."""
    text = re.sub(r"--[^\n]*", "", path.read_text())
    statements = [s.strip() for s in text.split(";")][:3]
    return [re.sub(r"^\s*EXPLAIN\s+ANALYZE\s+", "", s, flags=re.IGNORECASE) for s in statements]


def exact(v):
    """Valor exacto: los enteros pasan a Decimal para comparar 1 con 1.0 sin redondear nada."""
    if isinstance(v, int) and not isinstance(v, bool):
        return Decimal(v)
    return v


def main() -> None:
    conn = psycopg2.connect(DB_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor()

    titles = re.findall(r"^-- (Q[123]:.*)$", (SQL_DIR / "oltp_problems.sql").read_text(), re.MULTILINE)
    oltp = read_queries(SQL_DIR / "oltp_problems.sql")
    olap = read_queries(SQL_DIR / "olap_queries.sql")

    passed = 0
    for i, (q_oltp, q_olap) in enumerate(zip(oltp, olap), start=1):
        cur.execute(q_oltp)
        cols_oltp, rows_oltp = [d.name for d in cur.description], cur.fetchall()
        cur.execute(q_olap)
        cols_olap, rows_olap = [d.name for d in cur.description], cur.fetchall()

        console.rule(f"[bold]{titles[i - 1]}")
        same_cols = cols_oltp == cols_olap
        same_len = len(rows_oltp) == len(rows_olap)
        diffs = [
            (r, c)
            for r, (ro, ra) in enumerate(zip(rows_oltp, rows_olap))
            for c, (vo, va) in enumerate(zip(ro, ra))
            if exact(vo) != exact(va)
        ]

        # Una línea por celda: tabla angosta, cada valor completo a cualquier ancho de consola
        table = Table(show_header=True, header_style="bold")
        table.add_column("fila", justify="right")
        table.add_column("columna")
        table.add_column("OLTP", justify="right", style="cyan", no_wrap=True)
        table.add_column("OLAP", justify="right", style="magenta", no_wrap=True)
        table.add_column("igual")
        for r, (ro, ra) in enumerate(zip(rows_oltp[:SHOW_ROWS], rows_olap[:SHOW_ROWS]), start=1):
            for c, (vo, va) in enumerate(zip(ro, ra)):
                ok = exact(vo) == exact(va)
                table.add_row(str(r) if c == 0 else "", cols_oltp[c], str(vo), str(va),
                              "[green]✓[/green]" if ok else "[red]✗[/red]",
                              end_section=c == len(ro) - 1)
        console.print(table)
        if len(rows_oltp) > SHOW_ROWS:
            console.print(f"  … {len(rows_oltp) - SHOW_ROWS} filas más, comparadas igual pero sin mostrarlas")

        def control(rows, cols):
            sums = {}
            for name in ("num_transactions", "total_txn", "fraud_txn", "total_cop"):
                if name in cols:
                    sums[name] = sum(row[cols.index(name)] for row in rows)
            return sums

        ctl_oltp, ctl_olap = control(rows_oltp, cols_oltp), control(rows_olap, cols_olap)
        checks = [
            ("columnas (nombre y orden)", same_cols, f"{cols_oltp}"),
            ("número de filas", same_len, f"OLTP {len(rows_oltp)} · OLAP {len(rows_olap)}"),
            ("valores exactos, fila a fila, sin redondeo", same_len and not diffs,
             f"{len(rows_oltp) * len(cols_oltp) - len(diffs)}/{len(rows_oltp) * len(cols_oltp)} celdas iguales"),
        ] + [
            (f"suma de control SUM({k})", ctl_oltp[k] == ctl_olap.get(k), f"OLTP {ctl_oltp[k]} · OLAP {ctl_olap.get(k)}")
            for k in ctl_oltp
        ]
        for name, ok, detail in checks:
            mark = "[green]✓[/green]" if ok else "[red]✗[/red]"
            console.print(f"  {mark} {name}: {detail}")
        if all(ok for _, ok, _ in checks):
            passed += 1

    console.print(f"\n[bold]{passed}/3 consultas con resultados exactamente iguales[/bold]")
    cur.close()
    conn.close()
    if passed < 3:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
