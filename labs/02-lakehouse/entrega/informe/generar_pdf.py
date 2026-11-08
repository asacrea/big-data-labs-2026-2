# Convierte informe_lab2.md en HTML con el estilo del PDF.
# Uso: python generar_pdf.py informe_lab2.md informe.html
#      chrome --headless --no-pdf-header-footer --print-to-pdf=informe_lab2.pdf informe.html
import io, sys
import markdown

src = io.open(sys.argv[1], encoding="utf-8").read()
body = markdown.markdown(src, extensions=["tables", "md_in_html"])
css = """@page{size:letter;margin:1.3cm 1.5cm}
body{font-family:Calibri,Arial,sans-serif;font-size:9.5pt;line-height:1.17}
h1{font-size:15pt;margin:0 0 3pt} h2{font-size:12pt;margin:8pt 0 3pt} h3{font-size:10.5pt;margin:6pt 0 2pt}
p{margin:3pt 0} ul{margin:2pt 0;padding-left:15pt} li{margin:0}
table{border-collapse:collapse;font-size:8.3pt;margin:3pt 0} td,th{border:1px solid #999;padding:1px 4px}
img{display:block;margin:3pt 0;max-width:72%;break-inside:avoid} img[src*=arquitectura]{max-width:100%}"""
io.open(sys.argv[2], "w", encoding="utf-8").write(
    f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>{body}</body></html>")
