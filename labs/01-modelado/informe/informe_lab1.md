# Lab 1 · Modelado OLTP → OLAP — PayTrack Colombia

**SI7006 Almacenamiento y Procesamiento de Grandes Datos · EAFIT 2026-2**
**Equipo:** Santiago Vélez Casallas · _[nombre del compañero]_
**Repositorio:** https://github.com/santiagovelcasallas/big-data-labs-2026-2

> Borrador en Markdown. La versión final se exporta a PDF (**máximo 3 páginas**): sección 1 ≈ 1 página,
> sección 2 ≈ 1 página y sección 3 ≈ 1 página. Las evidencias completas están en `labs/01-modelado/evidencias/`.

---

## 1. Diagnóstico del OLTP (Parte A)

**Entorno.** PostgreSQL 16 (Docker) con el esquema `oltp` en 3FN: `transactions` referencia a `customers`,
`merchants` y `payment_methods`, y `fraud_events` referencia a `transactions`. Se cargaron 50.000
transacciones de 90 días (569 páginas de 8 kB, unos 4,5 MB) y, para la prueba de escala, 500.000
(5.676 páginas, unos 44 MB). La carga se verificó: los conteos cuadran con lo generado y no hay huérfanos ni
eventos de fraude duplicados. Los tiempos son de caché caliente, después de `ANALYZE`.

| Consulta | 50K | 500K | Operaciones que dominan el plan |
|---|---|---|---|
| Q1 · monto por categoría, 30 días | 9,1 ms | 56,5 ms | Lectura del heap de un tercio de la tabla + Hash Join con `merchants` |
| Q2 · tasa de fraude por segmento | 18,6 ms | 78,4 ms | Seq Scan de toda la tabla + Hash Join con `customers` |
| Q3 · volumen por día y hora, 7 días | 6,7 ms | 51,7 ms | `EXTRACT()` por fila + Sort por expresión + GroupAggregate |

**Q1.** El índice `idx_txn_created_at` encuentra las 16.626 transacciones del último mes, pero el
Bitmap Heap Scan visita **las 569 páginas** de la tabla (`Heap Blocks: exact=569`). Las filas de un mes están
dispersas, así que el índice reduce filas pero no páginas. Además `status` no está en el índice y cada fila se
trae del heap solo para descartar las rechazadas. Luego viene un Hash Join con `merchants` para obtener
`category`. Con 500K el optimizador abandona el índice y usa un **Parallel Seq Scan** con 2 workers: leer un
tercio de una tabla dispersa por índice ya no compensa. *Por qué en 3FN:* `category` depende de
`merchant_id`, no de la transacción, así que vive en otra tabla. Agrupar por categoría exige un join por cada
transacción del periodo.

**Q2.** La pregunta no filtra, así que ningún índice sirve y el Seq Scan es la decisión correcta. El costo lo
domina el **join**: leer las 50.000 filas toma 3,1 ms, pero el Hash Join con `customers` lleva el acumulado a
11,9 ms. Cruzar cuesta casi 3 veces lo que leer, para producir apenas 3 grupos. *Por qué en 3FN:* `segment`
es atributo del cliente y obliga a cruzar **toda** la historia de transacciones. Además el OLTP sobrescribe el
segmento: si un cliente pasó de `mass` a `premium`, sus fraudes de antes se atribuyen al segmento actual. La
respuesta es históricamente incorrecta, y ese es el argumento para la dimensión cliente SCD Tipo 2 (sección 2).

**Q3.** No hay joins, pero cada fila del periodo exige calcular `EXTRACT(dow)` y `EXTRACT(hour)`, y luego
ordenar por esas expresiones (277 kB con 50K y 3,2 MB con 500K). Otra vez se visitan todas las páginas. El
optimizador no tiene estadísticas de las expresiones: estima 3.877 grupos cuando salen 168 (37.868 contra 168
con 500K), y por eso elige Sort + GroupAggregate. *Por qué en 3FN:* día de la semana y hora se derivan de
`created_at`. Guardarlos sería redundancia, así que la normalización no los almacena y se recalculan en cada
consulta. En el modelo dimensional, `dim_date` y `dim_time` los traen precalculados y con estadísticas propias.

**Escala: 500K transacciones diarias durante un año.** Son unos 182,5 millones de filas, 365 veces el volumen
de 500K: unos 2 millones de páginas, cerca de **16 GB** solo en el heap de `transactions`, mucho más que la
memoria disponible. Con 10 veces más datos los tiempos crecieron entre 4,2 y 7,8 veces (el paralelismo
amortigua el crecimiento). Al extrapolar linealmente desde 500K y con todo en memoria, Q2 pasaría de 78 ms a
unos **28 s** y Q1 a unos 20 s, porque las dos recorren la tabla completa. Ese es un piso optimista: a ese
tamaño la lectura pasa a ser de disco. Q2 crece con toda la historia en cada ejecución, aunque la pregunta
no cambie. Q1 y Q3 dependen del tamaño de la ventana (unos 15 M y 3,5 M filas), pero mientras las filas de
una ventana estén dispersas el acceso por índice termina tocando gran parte de los 16 GB. Más allá del
tiempo, estas lecturas compiten por CPU, memoria e I/O con la escritura de los pagos. Por eso conviene
separar el análisis en un modelo dimensional propio.

*Nota de método:* el generador sintético inserta las transacciones sin orden temporal. En producción llegarían
en orden cronológico y las ventanas recientes quedarían más agrupadas físicamente, lo que favorece a Q1 y Q3,
pero no a Q2.

**Conclusión.** El 3FN optimiza escribir una transacción sin redundancia. A cambio, cada pregunta analítica
debe **reconstruir** el contexto: joins para los atributos descriptivos (Q1, Q2), funciones para los atributos
de calendario (Q3) y lecturas amplias sin agregados previos. Además pierde la historia de los atributos que
cambian (Q2).

---

## 2. Diseño del esquema estrella (Parte B)

_Pendiente: proceso de negocio y grano, dimensiones y atributos (qué datos del OLTP no llegan al DW),
medidas y su aditividad, surrogate keys, SCD Tipo 2 en `dim_customer`. Diagrama en draw.io._

## 3. Comparación de rendimiento OLTP vs OLAP (Parte D)

_Pendiente: planes de Q1–Q3 sobre `dw.*` frente a los de la sección 1, y el experimento de índices
de la tabla de hechos (con y sin cada índice; cuándo el optimizador prefiere un sequential scan)._
