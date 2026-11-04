# Genera el diagrama (TikZ + .drawio) desde una sola especificación.
import html, sys, os
OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)

COL = {  # tikz fill, tikz draw, drawio fill, drawio stroke
    "aws":   ("FFF2CC", "D6B656"),
    "delta": ("DAE8FC", "6C8EBF"),
    "job":   ("D5E8D4", "82B366"),
    "local": ("F5F5F5", "666666"),
    "query": ("E1D5E7", "9673A6"),
    "frame": ("FFFFFF", "888888"),
}
# id: (kind, x, y, w, h, texto)
N = {
 "t_title":  ("label", 0.3, 0.1, 22, 0.7, "**PayTrack · Lakehouse end-to-end en AWS (Lab 2)** — batch + streaming sobre las mismas tablas Delta"),
 "f_s3":     ("frame", 5.0, 1.0, 18.6, 15.8, "Amazon S3 · bucket paytrack-lakehouse-<id>-<cuenta> (Delta Lake)"),
 "t_a":      ("label", 5.4, 1.55, 6, 0.4, "**PARTE A · BATCH** (Glue 5.0 · 4 jobs)"),
 "t_b":      ("label", 5.4, 9.15, 8, 0.4, "**PARTE B · STREAMING** (Glue Streaming · 1 job)"),
 "files":    ("local", 0.3, 2.2, 4.2, 1.6, "data/batchN/*.json\nlote 1: 2026-09-29\nlote 2: 2026-09-30"),
 "ingest":   ("local", 0.3, 4.6, 4.2, 1.0, "ingest.py\nJSON Lines + ingest_date"),
 "raw":      ("delta", 5.4, 2.1, 3.8, 3.1, "raw/<dataset>/\ningest_date=YYYY-MM-DD/\ntransactions · fraud_events\ncustomers · merchants\nciclo de vida: IA 30 d,\nGlacier IR 90 d, sin borrar"),
 "bronze":   ("delta", 10.0, 2.1, 4.0, 3.1, "bronze/\ntransactions · fraud_events\ncustomers · merchants\nDelta, todo como texto\n+ ingest_date/ingested_at\nreplaceWhere (idempotente)"),
 "silver":   ("delta", 14.8, 2.1, 4.0, 3.1, "silver/\ntransactions (MERGE)\ntransactions_quarantine\ncustomers (PII enmascarada)\nmerchants · fraud_events"),
 "gold":     ("delta", 19.6, 2.1, 3.6, 3.1, "gold/\ndaily_merchant_summary\nfraud_weekly_report\nhourly_volume"),
 "audit":    ("job",   14.8, 6.1, 4.0, 1.2, "job audit\ntime travel (versionAsOf)"),
 "changes":  ("delta", 19.6, 6.1, 3.6, 1.2, "gold/status_changes"),
 "producer": ("local", 0.3, 9.7, 4.2, 1.8, "producer.py\npagos · contracargos\nreenvíos · tardíos · ráfagas\nllave = customer_id"),
 "broker":   ("aws",   0.3, 12.1, 4.2, 1.9, "Broker de eventos\nAWS: Kinesis paytrack-transactions\n(2 shards, put_records + reintentos)\nlocal: Kafka paytrack.transactions\n(3 particiones, acks=all)"),
 "f_job":    ("frame", 5.3, 9.7, 12.2, 6.0, "Glue Streaming job · stream.py (Spark Structured Streaming)"),
 "q1":       ("query", 5.6, 12.1, 3.3, 1.3, "1 · bronze\nbroker → append"),
 "events":   ("delta", 9.4, 12.1, 3.3, 1.3, "bronze/\ntransaction_events"),
 "q2":       ("query", 13.3, 10.6, 3.9, 1.2, "2 · silver\nforeachBatch + MERGE\nsin watermark"),
 "q3":       ("query", 13.3, 12.1, 3.9, 1.2, "3 · metrics\nventana 1 min\nwatermark 2 min"),
 "q4":       ("query", 13.3, 13.6, 3.9, 1.2, "4 · alerts\nventanas 30 s / 10 s\n≥ 6 pagos por cliente"),
 "o_silver": ("delta", 18.0, 10.6, 4.8, 1.2, "silver/transactions\n(la misma tabla del batch)"),
 "o_metric": ("delta", 18.0, 12.1, 4.8, 1.2, "gold/merchant_minute_metrics"),
 "o_alert":  ("delta", 18.0, 13.6, 4.8, 1.2, "gold/fraud_alerts"),
 "ckpt":     ("delta", 5.6, 14.2, 7.1, 1.1, "checkpoints/<consulta> (bronze, silver, metrics, alerts)"),
 "catalog":  ("aws",   5.4, 18.2, 6.2, 1.5, "AWS Glue Data Catalog\nbase paytrack_lakehouse\n8 tablas Delta"),
 "athena":   ("aws",   13.4, 18.2, 6.6, 1.5, "Amazon Athena\nworkgroup paytrack-lakehouse-wg\n6 consultas (athena.sql)"),
 "dev":      ("local", 0.3, 14.6, 4.2, 2.0, "Desarrollo local (gratis)\nrun_local.sh: Docker +\nSpark 3.5 + Delta 3.2\nlakehouse-local/"),
 "legend":   ("label", 0.3, 17.2, 4.6, 1.6, "Naranja: servicio AWS\nAzul: datos Delta / S3\nVerde: job · Violeta: query stream\nGris: código y entorno local"),
}
# (origen, destino, etiqueta)
E = [
 ("files","ingest",""), ("ingest","raw","S3"), ("raw","bronze","Glue: bronze"),
 ("bronze","silver","Glue: silver"), ("silver","gold","Glue: gold"),
 ("silver","audit","versiones"), ("audit","changes",""),
 ("producer","broker","sink"), ("broker","q1",""), ("q1","events",""),
 ("events","q2",""), ("events","q3",""), ("events","q4",""),
 ("q2","o_silver",""), ("q3","o_metric",""), ("q4","o_alert",""),
 ("f_s3","catalog","_delta_log"), ("catalog","athena",""),
]

def center(i):
    k,x,y,w,h,_ = N[i]; return x+w/2, y+h/2
def anchor(a, b):
    """punto de salida de a hacia b (lados e/w/n/s)"""
    ka,xa,ya,wa,ha,_ = N[a]; kb,xb,yb,wb,hb,_ = N[b]
    cxa,cya = center(a); cxb,cyb = center(b)
    if xb >= xa+wa-0.01 and not (yb >= ya+ha-0.01 or yb+hb <= ya+0.01 and False):
        pass
    # decide por posición relativa
    if xb >= xa+wa-0.05:   # b a la derecha
        return ("e", xa+wa, cya), ("w", xb, cyb)
    if xb+wb <= xa+0.05:   # b a la izquierda
        return ("w", xa, cya), ("e", xb+wb, cyb)
    if yb >= ya+ha-0.05:   # b abajo
        return ("s", cxa, ya+ha), ("n", cxb, yb)
    return ("n", cxa, ya), ("s", cxb, yb+hb)

def path(a, b):
    (sa,x1,y1),(sb,x2,y2) = anchor(a,b)
    if sa in "ew":
        if abs(y1-y2) < 0.05: pts=[(x1,y1),(x2,y2)]
        else:
            mx=(x1+x2)/2; pts=[(x1,y1),(mx,y1),(mx,y2),(x2,y2)]
    else:
        if abs(x1-x2) < 0.05: pts=[(x1,y1),(x2,y2)]
        else:
            my=(y1+y2)/2; pts=[(x1,y1),(x1,my),(x2,my),(x2,y2)]
    return pts

# ---------- TikZ ----------
def esc(t):
    for a,b in [("\\","\\textbackslash{}"),("_","\\_"),("<","\\textless{}"),(">","\\textgreater{}"),("&","\\&"),("%","\\%"),("#","\\#"),("≥","$\\geq$"),("·","$\\cdot$"),("→","$\\rightarrow$")]:
        t=t.replace(a,b)
    return t
lines=[r"\documentclass[tikz,border=4pt]{standalone}",
r"\usepackage[utf8]{inputenc}\usepackage[T1]{fontenc}\usepackage{helvet}\renewcommand{\familydefault}{\sfdefault}",
r"\usetikzlibrary{arrows.meta}"]
for k,(f,d) in COL.items():
    lines.append(rf"\definecolor{{f{k}}}{{HTML}}{{{f}}}\definecolor{{d{k}}}{{HTML}}{{{d}}}")
lines.append(r"\begin{document}\begin{tikzpicture}[x=1cm,y=-1cm,font=\fontsize{6.8}{8}\selectfont]")
for i,(k,x,y,w,h,t) in N.items():
    if k=="label":
        bold = t.startswith("**")
        t=t.replace("**","")
        txt = esc(t).replace("\n",r"\\")
        if bold: txt=r"\textbf{"+txt+"}"
        fs = r"\fontsize{9}{10}\selectfont" if i=="t_title" else ""
        lines.append(rf"\node[anchor=north west,align=left,text width={w}cm,inner sep=0] ({i}) at ({x},{y}) {{{fs}{txt}}};")
    elif k=="frame":
        lines.append(rf"\draw[dashed,thick,draw=dframe,rounded corners=3pt] ({x},{y}) rectangle ({x+w},{y+h}) coordinate({i}n);")
        lines.append(rf"\node[anchor=north west,inner sep=2pt,font=\fontsize{{7}}{{8}}\selectfont\bfseries,text=black!70] at ({x+0.1},{y+0.05}) {{{esc(t)}}};")
        lines.append(rf"\coordinate ({i}) at ({x+w/2},{y+h/2});")
    else:
        txt=esc(t).split("\n"); txt=r"\textbf{"+txt[0]+r"}\\"+r"\\".join(txt[1:]) if len(txt)>1 else r"\textbf{"+txt[0]+"}"
        lines.append(rf"\node[draw=d{k},fill=f{k},rounded corners=3pt,line width=0.7pt,minimum width={w}cm,minimum height={h}cm,align=center,inner sep=2pt] ({i}) at ({x+w/2},{y+h/2}) {{{txt}}};")
for a,b,l in E:
    pts=path(a,b)
    coords=" -- ".join(f"({x:.2f},{y:.2f})" for x,y in pts)
    lines.append(rf"\draw[-{{Stealth[length=4pt]}},thick,draw=black!70] {coords};")
    if l:
        x1,y1=pts[0]; x2,y2=pts[1]
        lines.append(rf"\node[font=\fontsize{{6}}{{7}}\selectfont\itshape,fill=white,inner sep=1pt] at ({(x1+x2)/2:.2f},{(y1+y2)/2:.2f}) {{{esc(l)}}};")
lines.append(r"\end{tikzpicture}\end{document}")
open(f"{OUT}/arquitectura.tex","w").write("\n".join(lines))

# ---------- draw.io ----------
U=40
cells=['<mxCell id="0"/>','<mxCell id="1" parent="0"/>']
def val(t): return html.escape(t.replace("**","")).replace("\n","&lt;br&gt;")
for i,(k,x,y,w,h,t) in N.items():
    if k=="label":
        st="text;html=1;align=left;verticalAlign=top;whiteSpace=wrap;fontSize=%d;%s"%(14 if i=="t_title" else 11,"fontStyle=1;" if t.startswith("**") else "")
    elif k=="frame":
        st="rounded=1;whiteSpace=wrap;html=1;dashed=1;fillColor=none;strokeColor=#888888;verticalAlign=top;align=left;spacingLeft=6;fontStyle=1;fontSize=11;arcSize=2;"
    else:
        f,d=COL[k]; st=f"rounded=1;whiteSpace=wrap;html=1;fillColor=#{f};strokeColor=#{d};fontSize=10;arcSize=8;"
    cells.append(f'<mxCell id="{i}" value="{val(t)}" style="{st}" vertex="1" parent="1"><mxGeometry x="{x*U:.0f}" y="{y*U:.0f}" width="{w*U:.0f}" height="{h*U:.0f}" as="geometry"/></mxCell>')
for n,(a,b,l) in enumerate(E):
    cells.append(f'<mxCell id="e{n}" value="{html.escape(l)}" style="endArrow=block;html=1;rounded=0;edgeStyle=orthogonalEdgeStyle;strokeWidth=2;fontSize=9;fontStyle=2;" edge="1" parent="1" source="{a}" target="{b}"><mxGeometry relative="1" as="geometry"/></mxCell>')
xml='<mxfile host="app.diagrams.net"><diagram name="Arquitectura Lab 2" id="lab2"><mxGraphModel dx="1400" dy="900" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1654" pageHeight="1169" math="0" shadow="0"><root>'+"".join(cells)+'</root></mxGraphModel></diagram></mxfile>'
open(f"{OUT}/arquitectura.drawio","w").write(xml)
print("ok")
