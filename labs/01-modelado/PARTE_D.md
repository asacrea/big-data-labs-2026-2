# Parte D — Comparación de rendimiento

Ejecución de la Parte D del [README del lab](README.md), **en el mismo orden y con los mismos comandos**.
Bajo cada paso, en cita, va el texto del enunciado tal cual; debajo, la evidencia de que se ejecutó.

**Estado:** ✅ pasos 1 a 4 con 50.000 y con 500.000 transacciones (22-oct-2026). Respuestas en el [informe](informe/informe_lab1.md), sección 3.

Para repetir los pasos 1 a 4 de una vez y regenerar todas las evidencias:

```bash
bash labs/01-modelado/run_parte_d.sh            # 50.000 transacciones
bash labs/01-modelado/run_parte_d.sh 500000     # 💡 del README
```

Los tiempos son la **mediana de 5 ejecuciones** con caché caliente, después de un `ANALYZE;` (igual que en la Parte A).

---

## 1. Completa `sql/olap_queries.sql`

> Completa `sql/olap_queries.sql`: las mismas 3 preguntas de negocio, usando solo tablas `dw.*`.

**Evidencia:** [`paso1_olap_queries_solo_dw.txt`](evidencias/parte_d_50000/paso1_olap_queries_solo_dw.txt).
Las 3 consultas usan solo `dw.fact_transactions`, `dw.dim_date`, `dw.dim_time`, `dw.dim_customer` y `dw.dim_merchant`: hay 0 referencias a `oltp.*` y 0 `EXTRACT()` sobre la tabla de hechos. Las 3 conservan su `EXPLAIN ANALYZE`.

## 2. Verifica que tus resultados sean idénticos a los del OLTP

> ```
> uv run python labs/01-modelado/check_queries.py
> ```

**Evidencia:** [`paso2_check_queries.txt`](evidencias/parte_d_50000/paso2_check_queries.txt): `3/3 consultas correctas` (Q1 7 filas, Q2 3 filas, Q3 120 filas idénticas).

**Verificación estricta (anexo, nuestra):** [`paso2_anexo_exactitud.txt`](evidencias/parte_d_50000/paso2_anexo_exactitud.txt), generada con [`verify_exactitud.py`](verify_exactitud.py).
`check_queries.py` redondea a 2 decimales y ordena antes de comparar. Esta verificación es más exigente: compara fila *i* contra fila *i*, con el mismo `ORDER BY`, y cada valor exacto, sin redondeo. Además suma los totales del resultado completo de cada consulta.

| Consulta | Columnas (nombre y orden) | Filas OLTP · OLAP | Celdas exactamente iguales | Sumas de control |
|---|---|---|---|---|
| Q1 | ✓ | 7 · 7 | **28/28** (incluido `avg_cop` con 12 decimales) | `num_transactions` 15.510 = 15.510 · `total_cop` 1.161.280.244,15 = 1.161.280.244,15 |
| Q2 | ✓ | 3 · 3 | **12/12** | `total_txn` 50.000 = 50.000 · `fraud_txn` 200 = 200 |
| Q3 | ✓ | 120 · 120 | **480/480** | `num_transactions` 2.627 = 2.627 · `total_cop` 206.426.297,39 = 206.426.297,39 |

## 3. Ejecuta ambas versiones con `EXPLAIN ANALYZE` y compara

> Ejecuta ambas versiones con `EXPLAIN ANALYZE` y compara: tiempo total, tipos de scan y joins, filas procesadas.

**Evidencia:** [`paso3_resumen.txt`](evidencias/parte_d_50000/paso3_resumen.txt) y un plan completo por consulta y modelo (`paso3_q{1,2,3}_{oltp,olap}_explain_analyze.txt`).

| Consulta | Modelo | Tiempo total | Scans | Joins | Filas leídas de tablas |
|---|---|---|---|---|---|
| Q1 | OLTP | 8,96 ms | Bitmap Heap Scan `transactions` (`idx_txn_created_at`) · Seq Scan `merchants` | 1 Hash Join | 15.540 |
| | OLAP | 13,56 ms | Seq Scan `fact_transactions` · `dim_date` · `dim_merchant` | 2 Hash Join | 50.099 |
| Q2 | OLTP | 18,19 ms | Seq Scan `transactions` · `customers` | 1 Hash Join | 50.500 |
| | OLAP | 17,98 ms | Seq Scan `fact_transactions` · `dim_customer` | 1 Hash Join | 50.500 |
| Q3 | OLTP | 5,82 ms | Bitmap Heap Scan `transactions` (`idx_txn_created_at`) | — | 2.630 |
| | OLAP | 9,66 ms | Seq Scan `fact_transactions` · `dim_date` · `dim_time` | 2 Hash Join | 51.531 |

Con 50K filas el modelo estrella **no es más rápido**. Q2 empata (1,01×), y Q1 (1,51×) y Q3 (1,66×) son más lentas.
- **Q1 y Q3:** el OLTP filtra la fecha *directamente* sobre `transactions.created_at`, que tiene índice, y lee solo las filas del rango. En la estrella la fecha está en `dim_date`, así que el filtro se aplica en el Hash Join y la tabla de hechos se lee completa.
- **Q2:** recorre el 100 % de las filas en ambos modelos, con la misma forma de plan.

## 4. Experimenta con los índices de la tabla de hechos

> Experimenta con los índices de la tabla de hechos (TODO 5 de `star_schema.sql`): mide el plan con y sin cada índice. ¿Todos ayudan? ¿Por qué el optimizador a veces prefiere un sequential scan?

**Evidencia:** [`paso4_indices.txt`](evidencias/parte_d_50000/paso4_indices.txt), con los planes completos en [`paso4_planes_por_configuracion.txt`](evidencias/parte_d_50000/paso4_planes_por_configuracion.txt) y [`paso4_seqscan_forzado.txt`](evidencias/parte_d_50000/paso4_seqscan_forzado.txt).
Cada configuración quita índices dentro de una transacción (`BEGIN; DROP INDEX …; ROLLBACK`), así que `star_schema.sql` y la base quedan intactos.

**Con y sin cada índice.** Se probaron 7 configuraciones: con los 5, sin cada uno por separado y sin ninguno. En las 7 el plan es el mismo, `Seq Scan` sobre `fact_transactions`, y los tiempos solo varían por ruido (Q1 12–17 ms, Q2 16–23 ms, Q3 8–11 ms, sin un patrón por índice). Con los 5 presentes, **ninguna de las 3 consultas usa ninguno**.

**¿Todos ayudan?** No, para estas consultas ninguno ayuda:

| Consulta | Fracción de `fact_transactions` que necesita | Por qué no se usa el índice |
|---|---|---|
| Q1 | 31,1 % (30 de 91 días) | Leer un tercio de la tabla por índice cuesta más que leerla entera en orden. |
| Q2 | 100 % | Agrupa todas las transacciones: no hay nada que filtrar. |
| Q3 | 5,3 % (7 días) | Es selectiva, pero el filtro está en `dim_date.full_date` y no en `date_key`, y el optimizador estima mal cuántas filas de hechos pasan el join. |

`ix_fact_merchant`, `ix_fact_payment_method` e `ix_fact_time` solo intervienen en joins que el optimizador resuelve con Hash Join: lee la tabla completa una vez y no busca fila por fila.

**¿Por qué el optimizador a veces prefiere un sequential scan?** Se prohibió con `SET enable_seqscan = off` para obligarlo a usar los índices:

| Consulta | Plan | Cómo lee `fact_transactions` | Costo estimado | Tiempo real |
|---|---|---|---|---|
| Q1 | elegido | Seq Scan | 1.334 | 15,28 ms |
| | forzado | Bitmap Heap Scan (`ix_fact_date`) | 2.591 | 15,30 ms |
| Q2 | elegido | Seq Scan | 1.540 | 16,26 ms |
| | forzado | Index Scan (`ix_fact_customer`) | 3.632 | **31,46 ms** (×1,9) |
| Q3 | elegido | Seq Scan | 1.237 | 8,58 ms |
| | forzado | Bitmap Heap Scan (`ix_fact_date`) | 1.613 | **3,62 ms** (2,4× más rápido) |

El optimizador elige por **costo estimado**. Un sequential scan lee páginas contiguas (`seq_page_cost = 1`), y un índice suma la lectura del índice más un acceso aleatorio a la tabla por cada fila (`random_page_cost = 4`).
- **Cuando se necesita mucho de la tabla (Q1, Q2), el seq scan gana.** En Q2 forzar el índice duplica el tiempo, y en Q1 no mejora nada.
- **Q3 muestra el límite del modelo de costos.** Con solo 5,3 % de las filas el índice sí es más rápido (3,6 ms contra 8,6 ms), pero el optimizador no lo ve: el filtro llega desde la dimensión y `random_page_cost = 4` supone un disco giratorio, no un SSD con los datos en caché. Por eso no lo elige.

---

## Anexos (complementos; no reemplazan ningún paso)

### A. Archivos nuestros que usa la Parte D

| Archivo | Para qué |
|---|---|
| [`run_parte_d.sh`](run_parte_d.sh) | Ejecuta los pasos 1 a 4 en orden y guarda las evidencias en `evidencias/parte_d_<N>/`. |
| [`verify_exactitud.py`](verify_exactitud.py) | Paso 2, verificación estricta celda por celda, sin redondeo, con sumas de control. |
| [`parte_d_planes.py`](parte_d_planes.py) | Pasos 3 y 4: mediana de 5 `EXPLAIN ANALYZE`, resumen de scans, joins y filas, experimento de índices y `enable_seqscan = off`. |

Ninguno modifica archivos del profesor ni el esquema: el paso 4 revierte cada `DROP INDEX`.

### B. 💡 Prueba con 500.000 transacciones

> 💡 Con 50K filas las diferencias pueden ser pequeñas. Prueba también con `--transactions 500000` y compara cómo crecen los tiempos en cada modelo.

Se repitieron los pasos 1 a 4 con 500.000 transacciones: evidencias en [`evidencias/parte_d_500000/`](evidencias/parte_d_500000/).
La verificación sigue exacta: `check_queries.py` 3/3 y [`paso2_anexo_exactitud.txt`](evidencias/parte_d_500000/paso2_anexo_exactitud.txt) con 28/28, 12/12 y 384/384 celdas iguales. Q3 tiene 96 filas porque los datos de 500K se generan con otra ventana de fechas.

| Consulta | OLTP 50K | OLAP 50K | OLAP/OLTP 50K | OLTP 500K | OLAP 500K | OLAP/OLTP 500K |
|---|---|---|---|---|---|---|
| Q1 | 8,96 ms | 13,56 ms | 1,51× | 50,67 ms | 53,83 ms | **1,06×** |
| Q2 | 18,19 ms | 17,98 ms | 0,99× | 73,23 ms | 74,18 ms | **1,01×** |
| Q3 | 5,82 ms | 9,66 ms | 1,66× | 31,17 ms | 37,46 ms | **1,20×** |

Con 10 veces más datos la desventaja de la estrella se reduce, porque los joins extra son con dimensiones pequeñas y cuestan casi lo mismo. Con 500K, Q1 en el OLTP abandona el índice de `created_at` y pasa a *Parallel Seq Scan*, igual que la estrella. Los índices de la tabla de hechos siguen sin usarse. Forzarlos (`enable_seqscan = off`) lleva Q1 de 55,7 a 179,0 ms y Q2 de 69,6 a 164,7 ms, y solo mejora Q3, de 36,3 a 25,1 ms ([`paso4_indices.txt`](evidencias/parte_d_500000/paso4_indices.txt)).

```bash
bash labs/01-modelado/run_parte_d.sh 500000   # luego volver a 50K: bash labs/01-modelado/run_parte_d.sh
```
