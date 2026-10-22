"""
Parte D · pasos 3 y 4 del README (archivo nuestro, no del profesor).

Paso 3: ejecuta oltp_problems.sql y olap_queries.sql con su EXPLAIN ANALYZE y
        compara tiempo total, tipos de scan y joins, y filas procesadas.
Paso 4: experimenta con los índices de la tabla de hechos (TODO 5 de
        star_schema.sql): mide el plan con y sin cada índice, y muestra por qué
        el optimizador a veces prefiere un sequential scan.

Los índices se quitan dentro de una transacción (BEGIN; DROP INDEX …; ROLLBACK),
así que el esquema queda igual al terminar.

Uso: python labs/01-modelado/parte_d_planes.py <carpeta_de_evidencias> [repeticiones]
"""

import os
import re
import statistics
import sys
from pathlib import Path

import psycopg2
from rich.console import Console
from rich.table import Table

console = Console(record=True, width=int(os.getenv("ANCHO_EVIDENCIA", "140")))  # ancho fijo: la evidencia sale completa

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/paytrack")
SQL_DIR = Path(__file__).parent / "sql"
OUT = Path(sys.argv[1])
RUNS = int(sys.argv[2]) if len(sys.argv) > 2 else 5

NODE = re.compile(r"^\s*(?:->\s+)?([A-Z][A-Za-z ]+?)(?: using (\S+))?(?: on (\S+)(?: (\w+))?)?\s+\(cost=[\d.]+\.\.([\d.]+) rows=\d+ width=\d+\) \(actual time=[\d.]+\.\.[\d.]+ rows=(\d+) loops=(\d+)\)")
REMOVED = re.compile(r"Rows Removed by (?:Filter|Index Recheck): (\d+)")


def statements(path: Path) -> list[tuple[str, str]]:
    """(título, sentencia con su EXPLAIN ANALYZE) de cada consulta Q1–Q3 del archivo."""
    text = path.read_text()
    titles = re.findall(r"^-- (Q[123]:.*)$", text, re.MULTILINE)
    body = [s.strip() for s in re.sub(r"--[^\n]*", "", text).split(";") if s.strip()][:3]
    return list(zip(titles, body))


def explain(cur, sql: str) -> list[str]:
    cur.execute(sql)
    return [r[0] for r in cur.fetchall()]


def exec_ms(plan: list[str]) -> float:
    return float(re.search(r"Execution Time: ([\d.]+)", "\n".join(plan)).group(1))


def measure(cur, sql: str, runs: int = RUNS) -> tuple[float, list[str]]:
    """Mediana del Execution Time en `runs` ejecuciones (caché caliente) y el plan de la última."""
    explain(cur, sql)  # calentamiento: los datos quedan en shared_buffers
    times, plan = [], []
    for _ in range(runs):
        plan = explain(cur, sql)
        times.append(exec_ms(plan))
    return statistics.median(times), plan


def nodes(plan: list[str]) -> list[dict]:
    """Nodos del plan con tipo, tabla, índice, costo total estimado y filas reales."""
    out = []
    for line in plan:
        m = NODE.match(line)
        if m:
            kind, index, rel, _alias, cost, rows, loops = m.groups()
            if kind.strip() == "Bitmap Index Scan":  # "Bitmap Index Scan on <índice>"
                index = rel
            out.append({"kind": kind.strip(), "index": index, "rel": rel, "cost": float(cost),
                        "rows": int(rows) * int(loops), "loops": int(loops), "removed": 0})
        elif out and (r := REMOVED.search(line)):
            out[-1]["removed"] += int(r.group(1)) * out[-1]["loops"]
    return out


def scans(ns):  return [n for n in ns if "Scan" in n["kind"] and n["rel"]]
def joins(ns):  return [n for n in ns if n["kind"] in ("Hash Join", "Merge Join", "Nested Loop")]


def scan_label(n: dict) -> str:
    if n["kind"] == "Bitmap Index Scan":
        return f"  └ Bitmap Index Scan ({n['index']})"
    table = n["rel"].split(".")[-1]
    return f"{n['kind']} {table}" + (f" ({n['index']})" if n["index"] else "")


def fact_scan(ns: list[dict]) -> str:
    """Cómo se lee la tabla de hechos (o transactions en OLTP)."""
    for n in ns:
        if n["rel"] and n["rel"].split(".")[-1] in ("fact_transactions", "transactions") and n["kind"] != "Bitmap Index Scan":
            idx = next((m["index"] for m in ns if m["kind"] == "Bitmap Index Scan"), None) if n["kind"] == "Bitmap Heap Scan" else n["index"]
            return n["kind"] + (f" ({idx})" if idx else "")
    return "?"


def save(name: str, lines: list[str]) -> None:
    (OUT / name).write_text("\n".join(lines) + "\n", encoding="utf-8")


def export(name: str) -> None:
    (OUT / name).write_text(console.export_text(clear=True), encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dw.fact_transactions")
    n_fact = cur.fetchone()[0]
    conn.commit()

    oltp = statements(SQL_DIR / "oltp_problems.sql")
    olap = statements(SQL_DIR / "olap_queries.sql")

    # ── Paso 3 ────────────────────────────────────────────────────────────────
    console.rule(f"[bold]Paso 3 · EXPLAIN ANALYZE OLTP vs OLAP ({n_fact:,} transacciones, mediana de {RUNS} ejecuciones)")
    summary = Table(show_header=True, header_style="bold", show_lines=True)
    for col in ("Consulta", "Modelo", "Tiempo total", "Scans", "Joins", "Filas leídas de tablas"):
        summary.add_column(col)
    speedups = []
    for i, ((title, q_oltp), (_, q_olap)) in enumerate(zip(oltp, olap), start=1):
        results = {}
        for model, sql in (("OLTP", q_oltp), ("OLAP", q_olap)):
            ms, plan = measure(cur, sql)
            save(f"paso3_q{i}_{model.lower()}_explain_analyze.txt", [f"-- {title} · {model}", sql + ";", ""] + plan)
            ns = nodes(plan)
            read = sum(n["rows"] + n["removed"] for n in scans(ns) if n["kind"] != "Bitmap Index Scan")
            results[model] = ms
            summary.add_row(
                f"Q{i}" if model == "OLTP" else "", model, f"{ms:,.3f} ms",
                "\n".join(scan_label(n) for n in scans(ns)),
                "\n".join(n["kind"] for n in joins(ns)) or "—",
                f"{read:,}",
            )
        speedups.append((title, results["OLTP"], results["OLAP"]))
    conn.rollback()
    console.print(summary)
    for title, t_oltp, t_olap in speedups:
        ratio = t_oltp / t_olap
        console.print(f"  {title[:2]}: OLTP {t_oltp:,.3f} ms → OLAP {t_olap:,.3f} ms  "
                      f"([bold]{'%.2f× más rápido' % ratio if ratio >= 1 else '%.2f× más lento' % (1 / ratio)}[/bold])")
    console.print(f"  Planes completos en {OUT}/paso3_q*_explain_analyze.txt")
    export("paso3_resumen.txt")

    # ── Paso 4 ────────────────────────────────────────────────────────────────
    cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'dw' "
                "AND tablename = 'fact_transactions' AND indexname LIKE 'ix_fact_%' ORDER BY indexname")
    indexes = [r[0] for r in cur.fetchall()]
    conn.rollback()

    console.rule(f"[bold]Paso 4 · índices de la tabla de hechos: con y sin cada uno (mediana de {RUNS})")
    configs = [("con los 5 índices", [])] + [(f"sin {ix}", [ix]) for ix in indexes] + [("sin ningún índice", indexes)]
    table = Table(show_header=True, header_style="bold", show_lines=True)
    table.add_column("Configuración")
    for i in range(1, 4):
        table.add_column(f"Q{i}: tiempo", justify="right")
        table.add_column(f"Q{i}: cómo lee fact")
    detail = []
    for label, dropped in configs:
        cur.execute("BEGIN")
        for ix in dropped:
            cur.execute(f"DROP INDEX dw.{ix}")
        row = [label]
        for i, (title, q) in enumerate(olap, start=1):
            ms, plan = measure(cur, q)
            row += [f"{ms:,.3f} ms", fact_scan(nodes(plan))]
            detail += [f"===== {label} · {title}", *plan, ""]
        conn.rollback()  # devuelve los índices
        table.add_row(*row)
    console.print(table)
    save("paso4_planes_por_configuracion.txt", detail)

    # Usos reales de cada índice con los 5 presentes
    used = {ix: [] for ix in indexes}
    for i, (_, q) in enumerate(olap, start=1):
        for n in nodes(explain(cur, q)):
            if n["index"] in used:
                used[n["index"]].append(f"Q{i}")
    conn.rollback()
    console.print("\n[bold]¿Qué índice usa el optimizador en cada consulta?[/bold]")
    for ix, qs in used.items():
        mark = "[green]✓[/green]" if qs else "[yellow]–[/yellow]"
        console.print(f"  {mark} {ix}: {', '.join(qs) if qs else 'ninguna consulta lo usa'}")

    # Selectividad: qué fracción de la tabla de hechos necesita cada consulta
    console.print("\n[bold]Selectividad: fracción de fact_transactions que necesita cada consulta[/bold]")
    sel = [
        ("Q1", "SELECT COUNT(*) FROM dw.fact_transactions f JOIN dw.dim_date d ON f.date_key = d.date_key "
               "WHERE d.full_date >= CURRENT_DATE - 30"),
        ("Q2", "SELECT COUNT(*) FROM dw.fact_transactions"),
        ("Q3", "SELECT COUNT(*) FROM dw.fact_transactions f JOIN dw.dim_date d ON f.date_key = d.date_key "
               "WHERE d.full_date >= CURRENT_DATE - 7"),
    ]
    for q, sql in sel:
        cur.execute(sql)
        k = cur.fetchone()[0]
        console.print(f"  {q}: {k:,} de {n_fact:,} filas = {100 * k / n_fact:.1f} %")
    conn.rollback()

    # Por qué a veces gana el sequential scan: se prohíbe y se compara
    console.print("\n[bold]¿Por qué el optimizador a veces prefiere un sequential scan?[/bold] "
                  "Se prohíbe con SET enable_seqscan = off y se compara:")
    why = Table(show_header=True, header_style="bold", show_lines=True)
    for col in ("Consulta", "Plan", "Cómo lee fact", "Costo estimado", "Tiempo real"):
        why.add_column(col, justify="right" if col in ("Costo estimado", "Tiempo real") else "left")
    why_plans = []
    for i, (title, q) in enumerate(olap, start=1):
        for label, setting in (("elegido", "on"), ("forzado sin seq scan", "off")):
            cur.execute("BEGIN")
            cur.execute(f"SET LOCAL enable_seqscan = {setting}")
            ms, plan = measure(cur, q)
            conn.rollback()
            ns = nodes(plan)
            why.add_row(f"Q{i}" if setting == "on" else "", label, fact_scan(ns), f"{ns[0]['cost']:,.0f}", f"{ms:,.3f} ms")
            why_plans += [f"===== {title} · enable_seqscan = {setting}", *plan, ""]
    console.print(why)
    save("paso4_seqscan_forzado.txt", why_plans)
    export("paso4_indices.txt")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
