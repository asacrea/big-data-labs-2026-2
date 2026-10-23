"""Evidencia reproducible: ejecutar desde la raíz, con DATABASE_URL de una BD del lab.

Recrea oltp y dw mediante los cargadores originales. --output guarda evidencia.
"""

import argparse
import csv
import importlib.util
import io
import json
import os
import random
import statistics
from datetime import datetime
from pathlib import Path

import psycopg2
from faker import Faker
from rich.console import Console

HERE = Path(__file__).parent
ROOT = HERE.parents[1]
DATE_INDEX = (
    "CREATE INDEX idx_fact_approved_date ON dw.fact_transactions(date_key) "
    "INCLUDE (merchant_key,time_key,amount_cop) WHERE is_approved"
)
CUSTOMER_INDEX = "CREATE INDEX idx_fact_customer_test ON dw.fact_transactions(customer_key)"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def capture(mod, function, out, label):
    stream = io.StringIO()
    mod.console = Console(file=stream, record=True, width=112, force_terminal=True)
    mod.console.print(f"[bold]{label}[/bold]\n")
    function()
    mod.console.save_html(str(out.with_suffix(".html")), clear=False)
    out.with_suffix(".txt").write_text(mod.console.export_text(), encoding="utf-8")


def nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from nodes(child)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-date", default="2026-07-24")
    parser.add_argument("--seed", type=int, default=20261022)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    gen = load_module("generate_data", ROOT / "labs/shared/generate_data.py")
    oltp = load_module("load_oltp", HERE / "load_oltp.py")
    star = load_module("load_star", HERE / "load_star.py")
    check = load_module("check_queries", HERE / "check_queries.py")
    random.seed(args.seed)
    Faker.seed(args.seed)
    customers = gen.generate_customers(500)
    transactions, events = gen.generate_transactions(
        customers, 500000, start_date=datetime.fromisoformat(args.start_date), days=90
    )
    # Fecha de alta fija anterior a los pagos; elimina la dependencia del reloj de Faker.
    for customer in customers:
        customer["created_at"] = "2025-01-01T00:00:00"
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT version(), CURRENT_DATE::text, current_setting('TimeZone')")
    version, asof, timezone = cur.fetchone()
    # Mismo huso para todos los comandos y las dos escalas.
    metadata = dict(
        version=version,
        current_date=asof,
        timezone=timezone,
        seed=args.seed,
        start_date=args.start_date,
        days=90,
        repetitions=7,
        warmups=2,
        sizes=[50000, 500000],
    )
    cur.execute(
        "SELECT name, setting, unit FROM pg_settings WHERE name IN "
        "('shared_buffers','work_mem','max_parallel_workers_per_gather',"
        "'random_page_cost','effective_cache_size','jit')"
    )
    metadata["settings"] = cur.fetchall()
    (out / "environment.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    ol = check.read_queries(HERE / "sql/oltp_problems.sql")
    dw = check.read_queries(HERE / "sql/olap_queries.sql")
    summary = []
    for size in [50000, 500000]:
        print(f"Preparando {size:,} transacciones...", flush=True)
        data = ROOT / f"data/lab1_{size}"
        data.mkdir(parents=True, exist_ok=True)
        selected = transactions[:size]
        ids = {t["transaction_id"] for t in selected}
        for name, rows in [
            ("customers", customers),
            ("merchants", gen.MERCHANTS),
            ("transactions", selected),
            ("fraud_events", [e for e in events if e["transaction_id"] in ids]),
        ]:
            (data / f"{name}.json").write_text(
                json.dumps(rows, ensure_ascii=False), encoding="utf-8"
            )
        target = out / str(size)
        target.mkdir(exist_ok=True)
        capture(
            oltp,
            lambda: oltp.load_oltp(data),
            target / "load_oltp",
            f"load_oltp.py | {size:,} transacciones",
        )
        capture(
            star, star.load_star, target / "load_star", f"load_star.py | {size:,} transacciones"
        )
        capture(
            check,
            check.main,
            target / "check_queries",
            f"check_queries.py | {size:,} transacciones",
        )
        for i, (a, b) in enumerate(zip(ol, dw), 1):
            cur.execute(a)
            cols_a = [d.name for d in cur.description]
            rows_a = cur.fetchall()
            cur.execute(b)
            assert cols_a == [d.name for d in cur.description]
            assert sorted(rows_a) == sorted(cur.fetchall()), f"Q{i}: diferencia exacta"
        (target / "exact_equality.txt").write_text(
            "3/3 consultas: mismas columnas y valores exactos (Decimal, sin redondeo).\n",
            encoding="utf-8",
        )
        cur.execute("VACUUM (ANALYZE)")
        cur.execute("""SELECT COUNT(*), SUM(amount_cop), COUNT(*) FILTER (WHERE is_fraud),
            COUNT(*) FILTER (WHERE status='approved'), MIN(created_at), MAX(created_at),
            COUNT(*) FILTER (WHERE status='approved' AND created_at >= CURRENT_DATE-30),
            COUNT(*) FILTER (WHERE status='approved' AND created_at >= CURRENT_DATE-7)
            FROM oltp.transactions""")
        stats = dict(
            zip(
                [
                    "transactions",
                    "total_cop",
                    "fraud",
                    "approved",
                    "min_date",
                    "max_date",
                    "q1_rows",
                    "q3_rows",
                ],
                cur.fetchone(),
            )
        )
        (target / "totals.json").write_text(
            json.dumps(stats, default=str, indent=2), encoding="utf-8"
        )
        configs = [
            ("oltp", ol, None),
            ("sin_indices", dw, []),
            ("fecha", dw, [DATE_INDEX]),
            ("cliente", dw, [CUSTOMER_INDEX]),
            ("ambos", dw, [DATE_INDEX, CUSTOMER_INDEX]),
        ]
        # Alternar el orden de consultas en cada repetición; no se fuerza ningún plan.
        for label, sqls, indexes in configs:
            if indexes is not None:
                cur.execute("DROP INDEX IF EXISTS dw.idx_fact_approved_date")
                cur.execute("DROP INDEX IF EXISTS dw.idx_fact_customer_test")
                for statement in indexes:
                    cur.execute(statement)
            print(f"  {size}: {label}", flush=True)
            samples = {i: [] for i in range(3)}
            for repeat in range(9):
                order = list(range(3))
                random.Random(repeat).shuffle(order)
                for i in order:
                    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sqls[i])
                    plan = cur.fetchone()[0][0]
                    if repeat >= 2:
                        samples[i].append(plan)
            for i, plans in samples.items():
                stem = target / f"{label}_q{i + 1}"
                stem.with_suffix(".json").write_text(json.dumps(plans, indent=2), encoding="utf-8")
                times = [p["Execution Time"] for p in plans]
                median = statistics.median(times)
                representative = min(plans, key=lambda p: abs(p["Execution Time"] - median))
                cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sqls[i])
                stem.with_suffix(".txt").write_text(
                    "\n".join(r[0] for r in cur.fetchall()) + "\n", encoding="utf-8"
                )
                plan_nodes = list(nodes(representative["Plan"]))
                scans = [n for n in plan_nodes if "Scan" in n["Node Type"]]
                summary.append(
                    dict(
                        size=size,
                        variant=label,
                        query=f"Q{i + 1}",
                        median_ms=median,
                        min_ms=min(times),
                        max_ms=max(times),
                        shared_hit=representative["Plan"].get("Shared Hit Blocks", 0),
                        shared_read=representative["Plan"].get("Shared Read Blocks", 0),
                        scans="; ".join(
                            ("Parallel " if n.get("Parallel Aware") else "")
                            + n["Node Type"]
                            + " "
                            + n.get("Relation Name", "")
                            for n in scans
                        ),
                        joins="; ".join(
                            n["Node Type"]
                            for n in plan_nodes
                            if "Join" in n["Node Type"] or n["Node Type"] == "Nested Loop"
                        ),
                        scan_output_rows_approx=sum(
                            n.get("Actual Rows", 0) * n.get("Actual Loops", 0)
                            for n in scans
                            if n.get("Relation Name") in ("transactions", "fact_transactions")
                        ),
                    )
                )
        cur.execute("DROP INDEX IF EXISTS dw.idx_fact_customer_test")
        cur.execute("DROP INDEX IF EXISTS dw.idx_fact_approved_date")
        cur.execute(DATE_INDEX)
        cur.execute(
            "SELECT schemaname, tablename, indexname, "
            "pg_relation_size((schemaname||'.'||indexname)::regclass) "
            "FROM pg_indexes WHERE schemaname IN ('oltp','dw') ORDER BY 1,2,3"
        )
        (target / "indexes.json").write_text(json.dumps(cur.fetchall(), indent=2), encoding="utf-8")
        with (out / "benchmark.csv").open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(summary[0]))
            writer.writeheader()
            writer.writerows(summary)
    conn.close()
    print("Validaciones y mediciones completadas.", flush=True)


if __name__ == "__main__":
    main()
