"""
Diagrama del esquema estrella de PayTrack (entregable del README: tablas, llaves, cardinalidades y grano).

Toma las tablas y los colores del diagrama del equipo y los tipos de sql/star_schema.sql; las dimensiones
van en el orden de las FK de la tabla de hechos para que las relaciones no se crucen.
Genera, desde los mismos datos:
  esquema_estrella.drawio  editable en https://app.diagrams.net/
  esquema_estrella.svg     vista previa (se convierte a PNG con un navegador)

Uso: python labs/01-modelado/diagrama/generar_diagrama.py
"""

from html import escape
from pathlib import Path

OUT = Path(__file__).parent
ROW_H, HEAD_H = 26, 34
PK, FK, NK = "#24434f", "#1f3fbf", "#1a7f37"

# (columna, tipo, rol) — rol: pk, fk, nk o "" ; el orden es el de star_schema.sql
TABLES = {
    "dim_date": dict(x=40, y=400, w=300, head="#b3d795", cols=[
        ("date_key", "INT", "pk"), ("full_date", "DATE", "nk"), ("year", "SMALLINT", ""),
        ("quarter", "SMALLINT", ""), ("month", "SMALLINT", ""), ("month_name", "VARCHAR(20)", ""),
        ("week_of_year", "SMALLINT", ""), ("day_of_month", "SMALLINT", ""), ("day_of_week", "SMALLINT", ""),
        ("day_name", "VARCHAR(20)", ""), ("is_weekend", "BOOLEAN", ""), ("is_holiday_co", "BOOLEAN", ""),
    ]),
    "dim_customer": dict(x=40, y=80, w=300, head="#fdf0b0", note="SCD Tipo 2", cols=[
        ("customer_key", "INT", "pk"), ("customer_id", "VARCHAR(10)", "nk"), ("city", "VARCHAR(100)", ""),
        ("segment", "VARCHAR(50)", ""), ("income_band", "VARCHAR(20)", ""), ("valid_from", "DATE", ""),
        ("valid_to", "DATE", ""), ("is_current", "BOOLEAN", ""),
    ]),
    "fact_transactions": dict(x=470, y=230, w=340, head="#d4aa7d", cols=[
        ("transaction_id", "VARCHAR(20)", "pk"), ("customer_key", "INT", "fk"), ("merchant_key", "INT", "fk"),
        ("payment_method_key", "INT", "fk"), ("date_key", "INT", "fk"), ("time_key", "INT", "fk"),
        ("amount_cop", "DECIMAL(18,2)", ""), ("is_approved", "BOOLEAN", ""), ("is_fraud", "BOOLEAN", ""),
        ("fraud_risk_score", "DECIMAL(5,4)", ""),
    ]),
    "dim_time": dict(x=940, y=500, w=300, head="#c5a5c8", cols=[
        ("time_key", "INT", "pk"), ("hour", "SMALLINT", ""), ("minute", "SMALLINT", ""),
        ("period_of_day", "VARCHAR(20)", ""),
    ]),
    "dim_merchant": dict(x=940, y=80, w=300, head="#e9b48a", cols=[
        ("merchant_key", "INT", "pk"), ("merchant_id", "VARCHAR(50)", "nk"), ("name", "VARCHAR(200)", ""),
        ("category", "VARCHAR(100)", ""), ("city", "VARCHAR(100)", ""),
    ]),
    "dim_payment_method": dict(x=940, y=310, w=300, head="#8db9a8", cols=[
        ("payment_method_key", "INT", "pk"), ("method_name", "VARCHAR(100)", "nk"), ("method_type", "VARCHAR(50)", ""),
    ]),
}
# FK de la tabla de hechos → PK de la dimensión (cardinalidad 1:N)
LINKS = [("customer_key", "dim_customer"), ("merchant_key", "dim_merchant"),
         ("payment_method_key", "dim_payment_method"), ("date_key", "dim_date"), ("time_key", "dim_time")]
TITLE = "Esquema estrella PayTrack"
GRAIN = "Grano: una fila por transacción (transaction_id = dimensión degenerada)"
LEGEND = "PK llave primaria (surrogate) · FK llave foránea · NK llave natural del OLTP · ┼──< relación 1 : N"


def label(col, role):
    return f"{col} ({role.upper()})" if role else col


def row_y(t, i):
    return t["y"] + HEAD_H + i * ROW_H


# ---------- draw.io ----------
def drawio():
    cells, eid = [], [100]
    ids = {}

    def nid():
        eid[0] += 1
        return f"c{eid[0]}"

    cells.append(f'<mxCell id="title" value="{escape(TITLE)}" style="text;html=1;fontSize=24;fontStyle=1;align=left;verticalAlign=middle;" vertex="1" parent="1"><mxGeometry x="40" y="20" width="500" height="40" as="geometry"/></mxCell>')
    for name, t in TABLES.items():
        h = HEAD_H + ROW_H * len(t["cols"])
        tid = nid()
        cells.append(
            f'<mxCell id="{tid}" value="{name}" style="shape=table;startSize={HEAD_H};container=1;collapsible=0;childLayout=tableLayout;'
            f'fixedRows=1;rowLines=0;fontStyle=1;fontSize=14;align=center;fillColor={t["head"]};strokeColor=#555555;resizeLast=1;html=1;" '
            f'vertex="1" parent="1"><mxGeometry x="{t["x"]}" y="{t["y"]}" width="{t["w"]}" height="{h}" as="geometry"/></mxCell>')
        for i, (col, typ, role) in enumerate(t["cols"]):
            rid = nid()
            ids[(name, col)] = rid
            color = {"pk": PK, "fk": FK, "nk": NK}.get(role, "#1f2328")
            bold = 1 if role in ("pk", "fk") else 0
            cells.append(
                f'<mxCell id="{rid}" value="" style="shape=tableRow;horizontal=0;startSize=0;swimlaneHead=0;swimlaneBody=0;fillColor=#ffffff;'
                f'collapsible=0;dropTarget=0;points=[[0,0.5],[1,0.5]];portConstraint=eastwest;strokeColor=inherit;top=0;left=0;right=0;bottom=0;html=1;" '
                f'vertex="1" parent="{tid}"><mxGeometry y="{HEAD_H + i * ROW_H}" width="{t["w"]}" height="{ROW_H}" as="geometry"/></mxCell>')
            cells.append(
                f'<mxCell id="{nid()}" value="{escape(label(col, role))}" style="shape=partialRectangle;connectable=0;fillColor=none;top=0;left=0;bottom=0;right=0;'
                f'align=left;spacingLeft=10;fontSize=13;fontStyle={bold};fontColor={color};overflow=hidden;html=1;" vertex="1" parent="{rid}">'
                f'<mxGeometry width="{t["w"] - 120}" height="{ROW_H}" as="geometry"><mxRectangle width="{t["w"] - 120}" height="{ROW_H}" as="alternateBounds"/></mxGeometry></mxCell>')
            cells.append(
                f'<mxCell id="{nid()}" value="{escape(typ)}" style="shape=partialRectangle;connectable=0;fillColor=none;top=0;left=0;bottom=0;right=0;'
                f'align=right;spacingRight=10;fontSize=11;fontColor=#6e6e6e;fontFamily=Courier New;overflow=hidden;html=1;" vertex="1" parent="{rid}">'
                f'<mxGeometry x="{t["w"] - 120}" width="120" height="{ROW_H}" as="geometry"><mxRectangle width="120" height="{ROW_H}" as="alternateBounds"/></mxGeometry></mxCell>')
        if t.get("note"):
            cells.append(f'<mxCell id="{nid()}" value="{t["note"]}" style="text;html=1;fontSize=12;fontStyle=2;fontColor=#6e6e6e;align=left;" vertex="1" parent="1">'
                         f'<mxGeometry x="{t["x"]}" y="{t["y"] + h + 2}" width="200" height="22" as="geometry"/></mxCell>')
    for fk, dim in LINKS:
        pk = TABLES[dim]["cols"][0][0]
        cells.append(
            f'<mxCell id="{nid()}" value="" style="edgeStyle=entityRelationEdgeStyle;fontSize=12;html=1;endArrow=ERmandOne;startArrow=ERmany;'
            f'endFill=0;startFill=0;strokeWidth=2;strokeColor=#3b7d3b;rounded=1;" edge="1" parent="1" source="{ids[("fact_transactions", fk)]}" target="{ids[(dim, pk)]}">'
            f'<mxGeometry relative="1" as="geometry"/></mxCell>')
    f = TABLES["fact_transactions"]
    gy = f["y"] + HEAD_H + ROW_H * len(f["cols"]) + 22
    cells.append(f'<mxCell id="grain" value="{escape(GRAIN)}" style="shape=note;size=14;whiteSpace=wrap;html=1;fillColor=#e3f8e3;strokeColor=#3b7d3b;fontColor=#2e6b2e;fontStyle=1;fontSize=13;" vertex="1" parent="1">'
                 f'<mxGeometry x="{f["x"]}" y="{gy}" width="{f["w"]}" height="56" as="geometry"/></mxCell>')
    cells.append(f'<mxCell id="legend" value="{escape(LEGEND)}" style="text;html=1;fontSize=12;fontColor=#555555;align=left;" vertex="1" parent="1">'
                 f'<mxGeometry x="40" y="800" width="900" height="24" as="geometry"/></mxCell>')
    xml = ('<mxfile host="app.diagrams.net"><diagram id="estrella" name="Esquema estrella">'
           '<mxGraphModel dx="1300" dy="850" grid="1" gridSize="10" page="1" pageWidth="1280" pageHeight="840" math="0" shadow="0">'
           '<root><mxCell id="0"/><mxCell id="1" parent="0"/>' + "".join(cells) + "</root></mxGraphModel></diagram></mxfile>")
    (OUT / "esquema_estrella.drawio").write_text(xml, encoding="utf-8")


# ---------- vista previa SVG ----------
def svg():
    W, H = 1280, 840
    p = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="Helvetica, Arial, sans-serif">',
         f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
         f'<text x="40" y="50" font-size="24" font-weight="bold" fill="#111">{escape(TITLE)}</text>']
    f = TABLES["fact_transactions"]
    for fk, dim in LINKS:  # líneas debajo de las tablas
        i = [c[0] for c in f["cols"]].index(fk)
        y1 = row_y(f, i) + ROW_H / 2
        d = TABLES[dim]
        y2 = row_y(d, 0) + ROW_H / 2
        if d["x"] > f["x"]:
            x1, x2 = f["x"] + f["w"], d["x"]
            s = 1
        else:
            x1, x2 = f["x"], d["x"] + d["w"]
            s = -1
        mx = (x1 + x2) / 2
        p.append(f'<path d="M{x1},{y1} L{x1 + s * 22},{y1} C{mx},{y1} {mx},{y2} {x2 - s * 22},{y2} L{x2},{y2}" fill="none" stroke="#3b7d3b" stroke-width="2.2"/>')
        # muchos (pata de gallo) del lado de la tabla de hechos
        p.append(f'<path d="M{x1 + s * 14},{y1} L{x1},{y1 - 7} M{x1 + s * 14},{y1} L{x1},{y1 + 7}" stroke="#3b7d3b" stroke-width="2"/>')
        # uno obligatorio (dos barras) del lado de la dimensión
        for off in (8, 14):
            p.append(f'<line x1="{x2 - s * off}" y1="{y2 - 7}" x2="{x2 - s * off}" y2="{y2 + 7}" stroke="#3b7d3b" stroke-width="2"/>')
    for name, t in TABLES.items():
        h = HEAD_H + ROW_H * len(t["cols"])
        p.append(f'<rect x="{t["x"]}" y="{t["y"]}" width="{t["w"]}" height="{h}" fill="#fff" stroke="#555" stroke-width="1.2"/>')
        p.append(f'<rect x="{t["x"]}" y="{t["y"]}" width="{t["w"]}" height="{HEAD_H}" fill="{t["head"]}" stroke="#555" stroke-width="1.2"/>')
        p.append(f'<text x="{t["x"] + t["w"] / 2}" y="{t["y"] + 22}" font-size="15" font-weight="bold" text-anchor="middle" fill="#111">{name}</text>')
        for i, (col, typ, role) in enumerate(t["cols"]):
            y = row_y(t, i) + 18
            color = {"pk": PK, "fk": FK, "nk": NK}.get(role, "#1f2328")
            weight = "bold" if role in ("pk", "fk") else "normal"
            p.append(f'<text x="{t["x"] + 12}" y="{y}" font-size="13.5" font-weight="{weight}" fill="{color}">{escape(label(col, role))}</text>')
            p.append(f'<text x="{t["x"] + t["w"] - 12}" y="{y}" font-size="11.5" text-anchor="end" fill="#6e6e6e" font-family="Courier New, monospace">{escape(typ)}</text>')
        if t.get("note"):
            p.append(f'<text x="{t["x"]}" y="{t["y"] + h + 18}" font-size="12.5" font-style="italic" fill="#6e6e6e">{t["note"]}</text>')
    gy = f["y"] + HEAD_H + ROW_H * len(f["cols"]) + 22
    p.append(f'<path d="M{f["x"]},{gy} h{f["w"] - 14} l14,14 v42 h-{f["w"]} z" fill="#e3f8e3" stroke="#3b7d3b"/>')
    words, line1 = GRAIN.split(" (")
    p.append(f'<text x="{f["x"] + f["w"] / 2}" y="{gy + 24}" font-size="13.5" font-weight="bold" text-anchor="middle" fill="#2e6b2e">{escape(words)}</text>')
    p.append(f'<text x="{f["x"] + f["w"] / 2}" y="{gy + 44}" font-size="12" text-anchor="middle" fill="#2e6b2e">({escape(line1)}</text>')
    p.append(f'<text x="40" y="818" font-size="12.5" fill="#555">{escape(LEGEND)}</text>')
    p.append("</svg>")
    (OUT / "esquema_estrella.svg").write_text("\n".join(p), encoding="utf-8")


if __name__ == "__main__":
    drawio()
    svg()
    print("esquema_estrella.drawio y esquema_estrella.svg en", OUT)
