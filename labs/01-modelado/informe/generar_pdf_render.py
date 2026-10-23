# Convierte informe_lab1.md en HTML con el estilo del PDF (carta, 3 páginas + anexos).
# Uso: python generar_pdf_render.py informe_lab1.md informe.html
#      chrome --headless --no-pdf-header-footer --print-to-pdf=informe_lab1.pdf informe.html
import sys, io, markdown
src = io.open(sys.argv[1], encoding='utf-8').read()
body = markdown.markdown(src, extensions=['tables','md_in_html'])
css = """@page{size:letter;margin:1.3cm 1.5cm} body{font-family:Calibri,Arial,sans-serif;font-size:9.5pt;line-height:1.18} h2{break-before:page} h2:first-of-type{break-before:auto} hr{display:none}
h2{font-size:13pt;margin:0 0 4pt} h3{font-size:11pt;margin:8pt 0 2pt} p{margin:3pt 0}
table{border-collapse:collapse;font-size:8.3pt;margin:3pt 0} ul{margin:2pt 0;padding-left:16pt} li{margin:0} td,th{border:1px solid #999;padding:1px 4px} img{display:block;margin:4pt auto;max-width:72%} img[src*=load_star]{max-width:45%} img[src*=parte_a]{max-width:100%;margin:2pt auto} .dicc table{width:100%;font-size:8.5pt;table-layout:fixed} .dicc td,.dicc th{overflow-wrap:anywhere} .dicc th:nth-child(1){width:14%} .dicc th:nth-child(2){width:15%} .dicc th:nth-child(3){width:10%} .dicc th:nth-child(4){width:12%} .dicc th:nth-child(5){width:5%} .dicc th:nth-child(6){width:15%} .dicc th:nth-child(7){width:29%}"""
io.open(sys.argv[2],'w',encoding='utf-8').write(f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>{body}</body></html>")
