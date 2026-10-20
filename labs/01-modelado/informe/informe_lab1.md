# Lab 1 · Modelado OLTP → OLAP

**SI7006 Almacenamiento y Procesamiento de Grandes Datos · Maestría en Ingeniería de Datos · EAFIT 2026-2**
**Caso:** PayTrack Colombia
**Equipo:** Santiago Vélez Casallas · _[nombre del compañero]_
**Repositorio:** https://github.com/santiagovelcasallas/big-data-labs-2026-2

> Borrador en Markdown; se entrega en PDF. Enunciado: *"Informe en PDF (máx. 3 páginas): diagnóstico del OLTP
> (Parte A), decisiones de diseño (Parte B) y comparación de rendimiento con planes de ejecución (Parte D)"*.
> Las evidencias citadas están en `labs/01-modelado/evidencias/`.

---

## 1. Diagnóstico del OLTP (Parte A)

**Montaje.** Se siguieron los pasos 1 a 4 del enunciado: PostgreSQL 16 y pgAdmin con Docker, 50.000 transacciones
generadas con `generate_data.py` y cargadas con `load_oltp.py` al esquema `oltp` (3FN), y las tres consultas de
`sql/oltp_problems.sql` ejecutadas con `EXPLAIN ANALYZE`. La tabla `oltp.transactions` ocupa 569 páginas de 8 kB.
Para la tercera pregunta se repitió el montaje con 500.000 transacciones (5.680 páginas), como sugiere el
enunciado en la Parte D. Antes de medir se ejecutó `ANALYZE`.

| Consulta | 50.000 | 500.000 | Crecimiento (10× datos) |
|---|---|---|---|
| Q1: Monto total por categoría de comercio (últimos 30 días) | 9,586 ms | 52,036 ms | 5,4× |
| Q2: Tasa de fraude por segmento de cliente | 17,562 ms | 80,863 ms | 4,6× |
| Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días) | 4,864 ms | 50,865 ms | 10,5× |

La tercera pregunta se responde con el mismo volumen de referencia para las tres consultas: 500K transacciones
diarias durante un año son unos **182,5 millones de filas**. Eso es 365 veces la prueba de 500.000: unos
2 millones de páginas, cerca de **16 GB** solo en el heap de `oltp.transactions`.

### Q1: Monto total por categoría de comercio (últimos 30 días)

**¿Qué operaciones dominan el plan (scans, joins, sorts)?**
El **acceso a `transactions`** y el **Hash Join** con `merchants`. El Bitmap Index Scan sobre `idx_txn_created_at`
encuentra 16.646 filas del último mes, pero el Bitmap Heap Scan visita **las 569 páginas** de la tabla
(`Heap Blocks: exact=569`) y descarta 22 filas por `status`. Llega a 4,2 ms de los 9,6 totales. El Hash Join con
los 8 comercios lleva el acumulado a 6,6 ms, y el HashAggregate (7 grupos) y el Sort final completan el tiempo.
Con 500.000 el optimizador **cambia de plan**: abandona el índice y usa un *Parallel Seq Scan* con 2 workers
(52,0 ms), porque leer un tercio de la tabla por índice ya no compensa.

**¿Por qué el diseño en 3FN obliga a hacer ese trabajo?**
`category` depende de `merchant_id`, no de la transacción. Por eso en 3FN vive en `merchants`, y agrupar por
categoría exige un join por cada una de las 16.624 transacciones del periodo. Además, en 3FN la fecha es solo un
`TIMESTAMP` dentro de la fila transaccional; no hay un atributo de periodo que permita llegar directo a "los
últimos 30 días" sin recorrer las páginas donde están dispersas esas filas.

**¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?**
La ventana de 30 días tendría unos 15 millones de filas. Con 500.000 filas el optimizador ya prefiere leer la tabla
completa, así que con un año leería cerca de 16 GB en cada ejecución. Al extrapolar linealmente desde la medición
de 500.000 (52 ms × 365), el tiempo sería de unos **19 s como mínimo**. Es un piso: supone todo en memoria, y a
ese tamaño la lectura sería de disco.

### Q2: Tasa de fraude por segmento de cliente

**¿Qué operaciones dominan el plan (scans, joins, sorts)?**
El **Hash Join**. La consulta no tiene filtro, así que el Seq Scan de las 50.000 filas es la decisión correcta y
cuesta 3,1 ms. Unir esas filas con los 500 clientes lleva el acumulado a 11,7 ms: el join cuesta unas **2,8 veces
lo que la lectura**. El HashAggregate llega a 17,5 ms para producir **3 filas**. Es la consulta más costosa. Con
500.000 el plan es el mismo pero en paralelo (*Parallel Seq Scan* + Hash Join con 2 workers): 80,9 ms.

**¿Por qué el diseño en 3FN obliga a hacer ese trabajo?**
`segment` es un atributo del cliente y en 3FN vive en `customers`. Calcular el fraude por segmento obliga a hacer
el join de **todas** las transacciones de la historia contra la tabla de clientes. Además, el 3FN guarda solo el
segmento **actual**. Si un cliente cambió de segmento, sus transacciones anteriores se atribuyen al segmento nuevo,
y la tasa de fraude por segmento queda históricamente incorrecta.

**¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?**
Es la consulta que peor escala: recorre y une la tabla completa en cada ejecución, y su costo crece todos los días
aunque la pregunta no cambie. Al extrapolar linealmente desde la medición de 500.000 (80,9 ms × 365), serían unos
**30 s como mínimo** sobre 16 GB, y más si la lectura es de disco. El paralelismo amortiguó el crecimiento entre
50.000 y 500.000 (4,6× con 10× datos), pero está limitado al número de workers.

### Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días)

**¿Qué operaciones dominan el plan (scans, joins, sorts)?**
El **Sort** por expresiones calculadas y la lectura del heap. El Bitmap Index Scan encuentra 3.735 filas de la última
semana, pero de nuevo se visitan **las 569 páginas** (`exact=569`). Después se calculan `EXTRACT(DOW)` y
`EXTRACT(HOUR)` para cada fila y se ordena por ese resultado (Sort de 270 kB). Luego el GroupAggregate produce
168 grupos. El optimizador **estimó 3.776 grupos para 168 reales**, y con 500.000 estimó 38.397 para 168. No tiene
estadísticas sobre esas expresiones. Con 500.000 el tiempo creció 10,5× (50,9 ms), proporcional a los datos, porque
este plan no se paralelizó, y el Sort subió a 3.172 kB.

**¿Por qué el diseño en 3FN obliga a hacer ese trabajo?**
El día de la semana y la hora se derivan de `created_at`. Guardarlos sería redundancia, así que la normalización
no los almacena y **se recalculan para cada fila en cada consulta**. Agrupar por una expresión calculada impide usar
índices y deja al optimizador sin estadísticas para estimar.

**¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?**
La ventana de 7 días tendría unos 3,5 millones de filas, unas 90 veces las 38.647 de la medición de 500.000.
Por extrapolación lineal serían unos **4,6 s como mínimo**. Además, el Sort necesitaría del orden de 285 MB
(3.172 kB × 90). Eso supera ampliamente el `work_mem` por defecto de PostgreSQL (4 MB; con 500.000 ya usó 3,1 MB),
así que pasaría de *quicksort* en memoria a ordenamiento externo en disco. Y como las filas de la semana están
dispersas por la tabla, el acceso por índice tocaría gran parte de los 16 GB.

### Conclusión de la Parte A

El 3FN está diseñado para registrar cada pago sin redundancia. Por eso cada pregunta analítica debe
**reconstruir** el contexto en cada ejecución: joins para traer atributos descriptivos (Q1, Q2), funciones para
derivar atributos de calendario (Q3) y lecturas amplias sin agregados previos. Además pierde la historia de los
atributos que cambian (Q2). Con el volumen de un año, todas pasan de milisegundos a segundos, y Q2 a decenas de
segundos. Esas lecturas compiten por CPU, memoria e I/O con la escritura de los pagos en la misma base.

*Nota de método:* las extrapolaciones son lineales a partir de la medición con 500.000 filas; son cotas inferiores
con datos en memoria. El generador sintético inserta las transacciones sin orden temporal; en producción
llegarían en orden cronológico, lo que favorecería a Q1 y Q3, pero no a Q2.

---

## 2. Decisiones de diseño (Parte B)

_Pendiente. Se responderán, en este orden, los puntos del enunciado:_

- _**Proceso de negocio y grano** de la tabla de hechos._
- _**Dimensiones** y los atributos de cada una. ¿Qué datos del OLTP *no* deben llegar al data warehouse?_
- _**Medidas** de la tabla de hechos y si son aditivas, semiaditivas o no aditivas._
- _**Por qué surrogate keys** en lugar de las llaves del OLTP._
- _**Por qué SCD Tipo 2** para la dimensión cliente: ¿qué atributo puede cambiar y qué análisis se daña si se sobrescribe?_

_Incluye el diagrama del esquema estrella hecho en draw.io: tablas, llaves, cardinalidades y grano de la tabla de hechos._

## 3. Comparación de rendimiento con planes de ejecución (Parte D)

_Pendiente. Se responderán, en este orden, los puntos del enunciado:_

- _Ejecuta ambas versiones con `EXPLAIN ANALYZE` y compara: tiempo total, tipos de scan y joins, filas procesadas._
- _Experimenta con los índices de la tabla de hechos (TODO 5 de `star_schema.sql`): mide el plan con y sin cada índice. ¿Todos ayudan? ¿Por qué el optimizador a veces prefiere un *sequential scan*?_
