"""
Diagrama de arquitectura del Lab 2 (entregable del README): zonas, tablas por capa, jobs,
consultas de streaming y servicios de AWS.

Genera, desde los mismos datos:
  arquitectura_lakehouse.drawio  editable en https://app.diagrams.net/
  arquitectura_lakehouse.svg     vista previa (se convierte a PNG con un navegador)

Uso: python labs/02-lakehouse/entrega/diagrama/generar_diagrama.py
"""

from html import escape
from pathlib import Path

OUT = Path(__file__).parent
W, H = 1600, 880
INK, MUTED, LINE = "#1f2328", "#57606a", "#3d444d"
S3, RAW, BRONZE, SILVER, GOLD = "#eef7ee", "#e7e7e7", "#f0d9c0", "#dfe5ec", "#fbedb3"
GLUE, KINESIS, ATHENA, SRC, LOGS = "#e6dcf5", "#fbdcc4", "#d3e6fa", "#ffffff", "#f3e3ec"

# id: (x, y, ancho, alto, texto, relleno, tamaño, negrita, estilo de borde)
B = {}


def box(i, x, y, w, h, text, fill="#ffffff", size=12, bold=False, dash=False, top=False, bottom=False):
    B[i] = dict(x=x, y=y, w=w, h=h, text=text, fill=fill, size=size, bold=bold, dash=dash, top=top, bottom=bottom)


# ── Contenedores ──
box("s3", 250, 80, 1075, 490, "Amazon S3 · bucket paytrack-lakehouse-<STUDENT_ID>-<cuenta>", S3, 14, True, top=True)
box("z_raw", 265, 115, 120, 400, "raw/", RAW, 13, True, top=True)
box("z_bronze", 520, 115, 160, 400, "lakehouse/bronze/", BRONZE, 13, True, top=True)
box("z_silver", 815, 115, 170, 400, "lakehouse/silver/", SILVER, 13, True, top=True)
box("z_gold", 1120, 115, 192, 400, "lakehouse/gold/", GOLD, 13, True, top=True)
box("stream", 515, 600, 795, 190, "AWS Glue Streaming · job paytrack-stream · Spark Structured Streaming, micro-lotes cada 10 s", GLUE, 13, True, True, bottom=True)

# ── Tablas ──
def tablas(prefijo, x, w, nombres, fill):
    for n, nombre in enumerate(nombres):
        box(f"{prefijo}_{nombre}", x, 155 + n * 36, w, 30, nombre, "#ffffff", 11)

tablas("raw", 275, 100, ["transactions", "fraud_events", "customers", "merchants"], RAW)
tablas("br", 530, 140, ["transactions", "fraud_events", "customers", "merchants"], BRONZE)
tablas("si", 825, 150, ["customers", "merchants", "fraud_events", "transactions_quarantine"], SILVER)
tablas("go", 1130, 170, ["daily_merchant_summary", "fraud_weekly_report", "hourly_volume", "status_changes"], GOLD)
box("note_raw", 272, 320, 106, 90, "ingest_date=\nYYYY-MM-DD\n\nIA a 30 días\nGlacier IR a 90\nexpira a 10 años", RAW, 10)
box("br_events", 530, 465, 140, 30, "transaction_events", "#ffffff", 11, True)
box("si_transactions", 825, 465, 150, 30, "transactions", "#ffffff", 11, True)
box("note_si", 825, 425, 150, 34, "misma tabla para\nbatch y streaming", SILVER, 10)
box("go_alerts", 1130, 430, 170, 30, "fraud_alerts", "#ffffff", 11, True)
box("go_metrics", 1130, 470, 170, 30, "merchant_minute_metrics", "#ffffff", 11, True)
box("checkpoints", 265, 527, 160, 30, "checkpoints/ · por consulta", "#ffffff", 10)
box("athena_res", 435, 527, 155, 30, "athena-results/ · 7 días", "#ffffff", 10)

# ── Fuentes y entrada ──
box("lotes", 30, 150, 185, 70, "Lotes diarios\ndata/batchN/*.json", SRC, 12)
box("ingest", 30, 260, 185, 56, "ingest.py\nsube el lote a S3", SRC, 12, True)
box("producer", 30, 650, 185, 100, "producer.py\npagos, contracargos,\nreenvíos, eventos tardíos\ny ráfagas de fraude", SRC, 12)
box("kinesis", 262, 650, 215, 100, "Amazon Kinesis\npaytrack-transactions (2 shards)\n\nEn local: Kafka\npaytrack.transactions", KINESIS, 11)

# ── Jobs batch de Glue ──
box("j_bronze", 410, 225, 85, 60, "AWS Glue\npaytrack-\nbronze", GLUE, 10.5, True)
box("j_silver", 705, 225, 85, 60, "AWS Glue\npaytrack-\nsilver", GLUE, 10.5, True)
box("j_gold", 1010, 160, 85, 60, "AWS Glue\npaytrack-\ngold", GLUE, 10.5, True)
box("j_audit", 1010, 250, 85, 60, "AWS Glue\npaytrack-\naudit", GLUE, 10.5, True)

# ── Consultas de streaming ──
box("q_bronze", 530, 650, 150, 110, "consulta bronze\n\nappend\ncheckpoint", "#ffffff", 11)
box("q_silver", 815, 650, 170, 110, "consulta silver\n\nforeachBatch + MERGE\nsin watermark", "#ffffff", 11)
box("q_metrics", 1010, 650, 140, 110, "consulta metrics\n\nventanas de 1 min\nwatermark de 2 min", "#ffffff", 11)
box("q_alerts", 1160, 650, 150, 110, "consulta alerts\n\nventanas de 30 s cada 10 s\n6 o más pagos", "#ffffff", 10.5)

# ── Consumo y soporte ──
box("catalog", 1370, 130, 190, 80, "AWS Glue Data Catalog\npaytrack_lakehouse", GLUE, 12, True)
box("athena", 1370, 260, 190, 110, "Amazon Athena\npaytrack-lakehouse-wg\n\nsql/athena.sql\n6 consultas", ATHENA, 12, True)
box("logs", 1370, 650, 190, 70, "Amazon CloudWatch Logs\n/aws-glue/jobs/*", LOGS, 11.5)

# ── Flechas: (origen, destino, puntos, texto opcional) ──
E = [
    ("lotes", "ingest", [(122, 220), (122, 260)]),
    ("ingest", "z_raw", [(215, 288), (265, 288)]),
    ("z_raw", "j_bronze", [(385, 255), (410, 255)]),
    ("j_bronze", "z_bronze", [(495, 255), (520, 255)]),
    ("z_bronze", "j_silver", [(680, 255), (705, 255)]),
    ("j_silver", "z_silver", [(790, 255), (815, 255)]),
    ("j_silver", "si_transactions", [(747, 285), (747, 480), (825, 480)]),
    ("z_silver", "j_gold", [(985, 190), (1010, 190)]),
    ("j_gold", "z_gold", [(1095, 190), (1120, 190)]),
    ("si_transactions", "j_audit", [(975, 480), (1052, 480), (1052, 310)]),
    ("j_audit", "go_status_changes", [(1095, 278), (1130, 278)]),
    ("producer", "kinesis", [(215, 700), (262, 700)]),
    ("kinesis", "q_bronze", [(477, 700), (530, 700)]),
    ("q_bronze", "br_events", [(605, 650), (605, 495)]),
    ("br_events", "q_silver", [(670, 480), (697, 480), (697, 625), (850, 625), (850, 650)]),
    ("br_events", "q_metrics", [(670, 480), (697, 480), (697, 625), (1050, 625), (1050, 650)]),
    ("br_events", "q_alerts", [(670, 480), (697, 480), (697, 625), (1200, 625), (1200, 650)]),
    ("q_silver", "si_transactions", [(950, 650), (950, 495)]),
    ("q_metrics", "go_metrics", [(1140, 650), (1140, 500)]),
    ("q_alerts", "go_alerts", [(1305, 650), (1305, 445), (1300, 445)]),
    ("s3", "athena", [(1325, 315), (1370, 315)]),
    ("catalog", "athena", [(1465, 210), (1465, 260)]),
    ("stream", "logs", [(1310, 685), (1370, 685)]),
]

TITLE = "Arquitectura del lakehouse de PayTrack: batch y streaming"
SUB = "Parte A · Batch: arriba, de izquierda a derecha. Parte B · Streaming: abajo, entra por bronze y escribe en las mismas tablas Delta."
NOTE = ("Infraestructura: infra/serverless.yml (Serverless Framework + CloudFormation). "
        "Las consultas de streaming guardan su progreso en checkpoints/.")


def drawio():
    c = []
    c.append(f'<mxCell id="t" value="{escape(TITLE)}" style="text;html=1;fontSize=22;fontStyle=1;align=left;" vertex="1" parent="1"><mxGeometry x="30" y="15" width="1000" height="32" as="geometry"/></mxCell>')
    c.append(f'<mxCell id="st" value="{escape(SUB)}" style="text;html=1;fontSize=13;fontColor={MUTED};align=left;" vertex="1" parent="1"><mxGeometry x="30" y="47" width="1300" height="24" as="geometry"/></mxCell>')
    c.append(f'<mxCell id="n" value="{escape(NOTE)}" style="text;html=1;fontSize=12;fontColor={MUTED};align=left;" vertex="1" parent="1"><mxGeometry x="30" y="815" width="1300" height="24" as="geometry"/></mxCell>')
    for i, b in B.items():
        v = escape(b["text"]).replace(chr(10), "&#xa;")
        st = (f'rounded=1;arcSize=6;whiteSpace=wrap;html=0;fillColor={b["fill"]};strokeColor={LINE};fontColor={INK};'
              f'fontSize={b["size"]};fontStyle={1 if b["bold"] else 0};')
        if b["dash"]: st += "dashed=1;"
        if b["top"]: st += "verticalAlign=top;spacingTop=5;"
        if b["bottom"]: st += "verticalAlign=bottom;spacingBottom=6;"
        c.append(f'<mxCell id="{i}" value="{v}" style="{st}" vertex="1" parent="1"><mxGeometry x="{b["x"]}" y="{b["y"]}" width="{b["w"]}" height="{b["h"]}" as="geometry"/></mxCell>')
    for n, (a, z, pts) in enumerate(E):
        ba, bz = B[a], B[z]
        (x0, y0), (x1, y1) = pts[0], pts[-1]
        st = (f'edgeStyle=none;rounded=0;html=1;strokeColor={LINE};strokeWidth=1.5;endArrow=block;endFill=1;'
              f'exitX={(x0 - ba["x"]) / ba["w"]:.4f};exitY={(y0 - ba["y"]) / ba["h"]:.4f};exitDx=0;exitDy=0;'
              f'entryX={(x1 - bz["x"]) / bz["w"]:.4f};entryY={(y1 - bz["y"]) / bz["h"]:.4f};entryDx=0;entryDy=0;')
        mid = "".join(f'<mxPoint x="{x}" y="{y}"/>' for x, y in pts[1:-1])
        arr = f'<Array as="points">{mid}</Array>' if mid else ""
        c.append(f'<mxCell id="e{n}" value="" style="{st}" edge="1" parent="1" source="{a}" target="{z}"><mxGeometry relative="1" as="geometry">{arr}</mxGeometry></mxCell>')
    xml = ('<mxfile host="app.diagrams.net"><diagram id="lakehouse" name="Arquitectura">'
           f'<mxGraphModel dx="1400" dy="900" grid="1" gridSize="10" page="1" pageWidth="{W}" pageHeight="{H}" math="0" shadow="0">'
           '<root><mxCell id="0"/><mxCell id="1" parent="0"/>' + "".join(c) + "</root></mxGraphModel></diagram></mxfile>")
    (OUT / "arquitectura_lakehouse.drawio").write_text(xml, encoding="utf-8")


def svg():
    p = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="Helvetica, Arial, sans-serif">',
         f'<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L10 5L0 10z" fill="{LINE}"/></marker></defs>',
         f'<rect width="{W}" height="{H}" fill="#fff"/>',
         f'<text x="30" y="38" font-size="22" font-weight="bold" fill="{INK}">{escape(TITLE)}</text>',
         f'<text x="30" y="64" font-size="13" fill="{MUTED}">{escape(SUB)}</text>',
         f'<text x="30" y="832" font-size="12" fill="{MUTED}">{escape(NOTE)}</text>']
    for b in B.values():
        d = ' stroke-dasharray="6 4"' if b["dash"] else ""
        p.append(f'<rect x="{b["x"]}" y="{b["y"]}" width="{b["w"]}" height="{b["h"]}" rx="5" fill="{b["fill"]}" stroke="{LINE}"{d}/>')
        lines = b["text"].split(chr(10))
        lh = b["size"] * 1.25
        y = b["y"] + 18 if b["top"] else b["y"] + b["h"] - 10 if b["bottom"] else b["y"] + b["h"] / 2 - lh * (len(lines) - 1) / 2 + b["size"] * 0.36
        for i, l in enumerate(lines):
            w = "bold" if b["bold"] or (i == 0 and b["fill"] == "#ffffff" and len(lines) > 1) else "normal"
            p.append(f'<text x="{b["x"] + b["w"] / 2}" y="{y + i * lh}" font-size="{b["size"]}" font-weight="{w}" text-anchor="middle" fill="{INK}">{escape(l)}</text>')
    for a, z, pts in E:
        q = " ".join(f"{x},{y}" for x, y in pts)
        p.append(f'<polyline points="{q}" fill="none" stroke="{LINE}" stroke-width="1.5" marker-end="url(#a)"/>')
    p.append("</svg>")
    (OUT / "arquitectura_lakehouse.svg").write_text(chr(10).join(p), encoding="utf-8")


if __name__ == "__main__":
    drawio()
    svg()
    print("arquitectura_lakehouse.drawio y .svg en", OUT)
