# B.3 · Experimentos: datos medidos en local (2026-11-02)

Solo los números. El análisis (por qué) va en el informe, preguntas 8, 10 y 12.

## Experimento 1 · Watermark

Filas de `metrics` descartadas por watermark (2 minutos), según el reporte de cada corrida:

| Corrida | Eventos | Descartadas por watermark (`metrics`) | Evidencia |
|---|---|---|---|
| availableNow, productor `--fast` (10 min simulados, 20/s) | 12.288 | **6** | `stream_availablenow_1.txt` |
| En vivo, productor a 20 eventos/s durante 120 s | 2.487 | **51** (46 en vivo + 5 al apagar) | `stream_en_vivo_180s.txt` |
| En vivo, productor a 50 eventos/s durante 120 s | 6.195 | **100** (97 en vivo + 3 al apagar) | `experimento2_stream_en_vivo_rate50.txt` |

El productor envía la misma proporción de eventos tardíos en todos los casos (`LATE_RATE` = 3 %, con 30 a 360 s de retraso).

## Experimento 2 · Carga

Alertas contadas por las ventanas de cada corrida, no por la diferencia del total de la tabla: las ventanas que quedan
abiertas al final de una corrida se cierran en la siguiente, así que esa diferencia mezcla corridas.

| Tasa del productor | Eventos | Alertas | Aciertos | Precisión | Evidencia |
|---|---|---|---|---|---|
| 20 eventos/s | 2.487 | **47** | 38 | 80,85 % | `verificacion_experimentos/alertas_por_ventana_salida.txt` |
| 50 eventos/s | 6.195 | **375** | 99 | 26,40 % | ídem; ventanas cerradas con `experimento2_cierre_ventanas_rate50.txt` |

Precisión en AWS (TODO 5 de Athena): 93,75 % a 20/s; acumulado 29,79 % tras la corrida a 50/s; diferencia 110 de 438 = 25,11 %.

## Experimento 3 · Partition key

Mismo stream en los tres casos (`--duration 60 --fast --seed 7`, 1.227 eventos), cada uno en un topic de 3 particiones.
La llave se cambió temporalmente y `producer.py` quedó restaurado (llave = `customer_id`).

| Llave | p0 | p1 | p2 | Evidencia |
|---|---|---|---|---|
| `customer_id` (la nuestra) | 491 | 341 | 395 | `experimento3_partition_key.txt` |
| Constante | 0 | 1.227 | 0 | |
| `transaction_id` | 438 | 397 | 392 | |
