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

Se siguieron los pasos 1 a 4 del enunciado: PostgreSQL 16 en Docker, 50.000 transacciones cargadas al esquema
`oltp` (3FN, 569 páginas de 8 kB) y `EXPLAIN ANALYZE` de las tres consultas de `sql/oltp_problems.sql`, después
de `ANALYZE`. Como sugiere el enunciado, también se midió con 500.000 transacciones (5.680 páginas). La
proyección usa *500K transacciones diarias durante un año*: unos 182,5 M de filas, cerca de 16 GB de heap
(365 veces la prueba de 500.000). Las proyecciones son lineales y son cotas inferiores, con datos en memoria.

| Consulta | 50.000 | 500.000 | Año (proyección) |
|---|---|---|---|
| Q1: Monto total por categoría de comercio (últimos 30 días) | 9,6 ms | 52,0 ms | ≥ 19 s |
| Q2: Tasa de fraude por segmento de cliente | 17,6 ms | 80,9 ms | ≥ 30 s |
| Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días) | 4,9 ms | 50,9 ms | ≥ 4,6 s |

### Q1: Monto total por categoría de comercio (últimos 30 días)

**¿Qué operaciones dominan el plan (scans, joins, sorts)?** El Bitmap Heap Scan sobre `transactions` (4,2 de 9,6 ms):
el índice de `created_at` halla 16.646 filas, pero se visitan las 569 páginas (`exact=569`). Le sigue el Hash Join
con `merchants`. Con 500.000 el optimizador cambia a *Parallel Seq Scan*.  
**¿Por qué el diseño en 3FN obliga a hacer ese trabajo?** `category` depende de `merchant_id` y vive en `merchants`:
agrupar por categoría exige un join por transacción. La fecha es un `TIMESTAMP` sin atributos de periodo.  
**¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?** La ventana tendría unos 15 M de
filas. Como con 500.000 ya se lee la tabla completa, serían unos 16 GB por ejecución: ≥ 19 s (52 ms × 365).

### Q2: Tasa de fraude por segmento de cliente

**¿Qué operaciones dominan el plan (scans, joins, sorts)?** El Hash Join: el Seq Scan de las 50.000 filas cuesta 3,1 ms
y el join con `customers` lleva el acumulado a 11,7 ms (2,8 veces la lectura), para producir 3 grupos.  
**¿Por qué el diseño en 3FN obliga a hacer ese trabajo?** `segment` vive en `customers`, así que hay que unir toda la
historia de transacciones. Además el 3FN guarda solo el segmento actual: si un cliente cambia de segmento, sus
fraudes anteriores se atribuyen al nuevo.  
**¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?** Es la que peor escala: recorre la
tabla completa y crece cada día aunque la pregunta no cambie. Serían ≥ 30 s (80,9 ms × 365). El paralelismo
(4,6× con 10× datos) está limitado por el número de workers.

### Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días)

**¿Qué operaciones dominan el plan (scans, joins, sorts)?** El Sort por `EXTRACT(DOW)` y `EXTRACT(HOUR)` (270 kB) y la
lectura de las 569 páginas para 3.735 filas. El optimizador estima 3.776 grupos para 168 reales. Con 500.000 crece
10,5×: el plan no se paraleliza.  
**¿Por qué el diseño en 3FN obliga a hacer ese trabajo?** Día y hora se derivan de `created_at`; guardarlos sería
redundante. Se recalculan por fila en cada consulta, sin índices ni estadísticas posibles.  
**¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?** La ventana tendría unos 3,5 M de
filas (90 veces la de 500.000): ≥ 4,6 s. El Sort pediría unos 285 MB, mucho más que el `work_mem` por defecto
(4 MB), así que pasaría a ordenamiento en disco.

**Conclusión.** El 3FN optimiza registrar pagos sin redundancia. A cambio, cada consulta analítica reconstruye el
contexto: joins (Q1, Q2), atributos de calendario calculados (Q3) y lecturas amplias. Además se pierde la
historia de los atributos que cambian (Q2), y esas lecturas compiten con la escritura transaccional.

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
