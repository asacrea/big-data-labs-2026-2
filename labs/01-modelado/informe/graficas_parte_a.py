"""
Figuras de la sección 1 del informe (Parte A), a partir de las evidencias de EXPLAIN ANALYZE.

  Figura 1: tiempo de ejecución con 50.000 y 500.000 transacciones, y proyección a un año.
  Figura 2: en qué se va el tiempo de cada consulta (tiempo exclusivo por tipo de operación, 50.000).

Uso: python labs/01-modelado/informe/graficas_parte_a.py
"""

import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).parent
EVID = HERE.parent / "evidencias"
OUT = HERE / "img"

# Paleta de referencia (dataviz), validada: categórica en orden fijo, tinta y superficie
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
QUERIES = ["Q1 · monto por categoría\n(30 días)", "Q2 · fraude por segmento", "Q3 · volumen por día y hora\n(7 días)"]

# Proyección a un año de 500K transacciones diarias (sección 1): Q1 y Q2 leen la tabla
# completa (×365 sobre 500.000); Q3 lee una ventana de 7 días (~90 veces la de 500.000).
YEAR_FACTOR = [365, 365, 90]

NODE = re.compile(r"^(\s*)(?:->\s+)?([A-Z][A-Za-z ]+?)(?: on \S+(?: \w+)?)?\s+\(cost=.*actual time=([\d.]+)\.\.([\d.]+) rows=\d+ loops=(\d+)\)")
GROUP = {  # tipo de operación de cada nodo del plan
    "Seq Scan": "Lectura de la tabla", "Bitmap Heap Scan": "Lectura de la tabla", "Bitmap Index Scan": "Lectura de la tabla",
    "Parallel Seq Scan": "Lectura de la tabla",
    "Hash Join": "Join", "Hash": "Join",
    "HashAggregate": "Agregación", "GroupAggregate": "Agregación",
    "Sort": "Ordenamiento",
}
GROUPS = ["Lectura de la tabla", "Join", "Agregación", "Ordenamiento"]


def exec_time(path: Path) -> float:
    return float(re.search(r"Execution Time: ([\d.]+)", path.read_text(encoding="utf-8")).group(1))


def exclusive_by_group(path: Path) -> dict[str, float]:
    """Tiempo exclusivo de cada nodo = su tiempo final menos el de sus hijos directos."""
    nodes = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if m := NODE.match(line):
            indent, kind, _, end, loops = m.groups()
            nodes.append([len(indent), kind.strip(), float(end) * int(loops)])
    totals = dict.fromkeys(GROUPS, 0.0)
    for i, (ind, kind, end) in enumerate(nodes):
        child_ind = next((n[0] for n in nodes[i + 1:] if n[0] > ind), None)
        children = 0.0
        for n in nodes[i + 1:]:
            if n[0] <= ind:
                break
            if n[0] == child_ind:
                children += n[2]
        totals[GROUP[kind]] += max(end - children, 0.0)
    return totals


def style(ax):
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=8.5, length=0)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def figura_1():
    t50 = [exec_time(EVID / "parte_a_50000" / f"paso4_q{q}_explain_analyze.txt") for q in (1, 2, 3)]
    t500 = [exec_time(EVID / "parte_a_500000" / f"paso4_q{q}_explain_analyze.txt") for q in (1, 2, 3)]
    year = [t * f / 1000 for t, f in zip(t500, YEAR_FACTOR)]

    fig, (a, b) = plt.subplots(1, 2, figsize=(7.2, 2.25), gridspec_kw={"width_ratios": [1.5, 1]})
    x = range(3)
    w = 0.36
    for off, vals, color, label in ((-w / 2, t50, SERIES[0], "50.000 transacciones"), (w / 2, t500, SERIES[1], "500.000 transacciones")):
        bars = a.bar([i + off for i in x], vals, w * 0.94, color=color, label=label, edgecolor="white", linewidth=1)
        a.bar_label(bars, labels=[f"{v:.1f}".replace(".", ",") for v in vals], fontsize=8, color=INK, padding=2)
    a.set_xticks(list(x), [q.split(" · ")[0] for q in QUERIES])
    a.set_ylabel("ms", color=INK2, fontsize=8.5)
    a.set_title("Medido (EXPLAIN ANALYZE)", fontsize=9.5, color=INK, loc="left")
    a.legend(frameon=False, fontsize=8, loc="upper left")
    style(a)

    bars = b.bar(list(x), year, 0.5, color=SERIES[2], edgecolor="white", linewidth=1)
    b.bar_label(bars, labels=[f"≥ {v:.1f} s".replace(".", ",") for v in year], fontsize=8, color=INK, padding=2)
    b.set_xticks(list(x), [q.split(" · ")[0] for q in QUERIES])
    b.set_ylabel("s", color=INK2, fontsize=8.5)
    b.set_title("Proyección a un año (500K por día)", fontsize=9.5, color=INK, loc="left")
    b.set_ylim(0, max(year) * 1.2)
    style(b)
    fig.tight_layout()
    fig.savefig(OUT / "parte_a_tiempos.png", dpi=200)
    return t50, t500, year


def figura_2():
    data = [exclusive_by_group(EVID / "parte_a_50000" / f"paso4_q{q}_explain_analyze.txt") for q in (1, 2, 3)]
    fig, ax = plt.subplots(figsize=(7.2, 1.75))
    for qi, totals in enumerate(data):
        total = sum(totals.values())
        left = 0.0
        for gi, g in enumerate(GROUPS):
            share = 100 * totals[g] / total
            ax.barh(qi, share, left=left, height=0.62, color=SERIES[gi], edgecolor="white", linewidth=2,
                    label=g if qi == 0 else None)
            if share >= 8:
                ax.text(left + share / 2, qi, f"{share:.0f} %", ha="center", va="center", fontsize=8.5, color=INK)
            left += share
    ax.set_yticks(range(3), [q.replace("\n", " ") for q in QUERIES], fontsize=8.5, color=INK)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("% del tiempo de ejecución (tiempo exclusivo por operación, 50.000 transacciones)", fontsize=8, color=INK2)
    ax.legend(frameon=False, fontsize=8, ncol=4, loc="lower center", bbox_to_anchor=(0.4, 1.0))
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(length=0, labelsize=8)
    fig.tight_layout()
    fig.savefig(OUT / "parte_a_operaciones.png", dpi=200)
    return data


if __name__ == "__main__":
    print(figura_1())
    for d in figura_2():
        t = sum(d.values())
        print({k: f"{v:.3f} ms ({100 * v / t:.0f} %)" for k, v in d.items()})
