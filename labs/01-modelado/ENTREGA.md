# Lab 1 · Entrega

**Equipo:** Luis Alberto Ortiz Quiroga · Santiago Vélez Casallas
**Caso:** PayTrack Colombia · Enunciado: [`README.md`](README.md) (del profesor)

Este archivo es el índice de nuestra entrega: dónde está cada entregable que pide el enunciado.

## Video de sustentación

🎥 **[Sustentación Lab 01 · Ortiz Vélez](https://eafit-my.sharepoint.com/:v:/g/personal/savelezc_eafit_edu_co/IQCXXWVeFjCrRb6mRro8x-NgAbV9jRMLqfSdeQPp8i9HJhc)** (OneDrive EAFIT): sustentación en video del laboratorio por los dos integrantes. Entregable adicional.

## Entregables del enunciado

| | Entregable | Dónde |
|---|---|---|
| ✅ | `sql/star_schema.sql`, `load_star.py` y `sql/olap_queries.sql` | [`sql/star_schema.sql`](sql/star_schema.sql), [`load_star.py`](load_star.py), [`sql/olap_queries.sql`](sql/olap_queries.sql) |
| ✅ | Captura de `load_star.py` con todas las validaciones en verde | [`informe/img/load_star_validaciones_9de9.png`](informe/img/load_star_validaciones_9de9.png) (Anexo C del informe) |
| ✅ | Captura de `check_queries.py` con 3/3 consultas correctas | [`evidencias/revision_parte_b/6_check_queries_3de3.png`](evidencias/revision_parte_b/6_check_queries_3de3.png) |
| ✅ | Diagrama del esquema estrella en draw.io: tablas, llaves, cardinalidades y grano | [`diagrama/esquema_estrella.drawio`](diagrama/esquema_estrella.drawio) (vista previa: [`.png`](diagrama/esquema_estrella.png)) |
| ✅ | Informe en PDF (máx. 3 páginas): Partes A, B y D | [`informe/informe_lab1.pdf`](informe/informe_lab1.pdf): 3 páginas más los anexos A (diagrama), B (diccionario de datos) y C (carga y validaciones) |
| ➕ | Video de sustentación | [Enlace](https://eafit-my.sharepoint.com/:v:/g/personal/savelezc_eafit_edu_co/IQCXXWVeFjCrRb6mRro8x-NgAbV9jRMLqfSdeQPp8i9HJhc) (arriba) |

## Evidencias y procedimiento

- [`PARTE_A.md`](PARTE_A.md) y [`PARTE_D.md`](PARTE_D.md): cada paso del enunciado, en orden, con su evidencia.
- [`evidencias/`](evidencias/): salidas de `EXPLAIN ANALYZE` y validaciones con 50.000 y 500.000 transacciones (`parte_a_*`, `parte_d_*`) y capturas de la revisión de la Parte B (`revision_parte_b/`).
- Para reproducir: `bash labs/01-modelado/run_parte_a.sh` y `bash labs/01-modelado/run_parte_d.sh` (con `500000` como argumento para el volumen grande).

## Autoevaluación

`load_star.py` 9/9 ✓ · `check_queries.py` 3/3 ✓. No se modificaron los archivos dados ni los scripts de validación.
