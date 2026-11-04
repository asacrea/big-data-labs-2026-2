# Lab 2 · Entrega

**Equipo:** Luis Alberto Ortiz Quiroga · Santiago Vélez Casallas
**Entrega:** viernes 6 de noviembre de 2026 · **Peso:** 30%
**Enunciado:** [`labs/02-lakehouse/README.md`](../README.md) (del profesor, sin cambios)

Este archivo es el índice de nuestra entrega. Sigue la misma estructura del enunciado:
**Parte A · Batch**, **Parte B · Streaming**, **Ejecución en AWS** (las dos partes) y el informe.
Los archivos dados (`run_local.sh`, `run_aws.sh`, `tests/`) y las validaciones de cada job no se modificaron.
Única excepción: `jobs/common.py`, porque Glue 5.0 no entrega la columna `__src_timestamp` (explicado en el informe).

Estado: ✅ hecho y verificado · 🟡 hecho, falta verificar o falta evidencia · ⬜ pendiente

---

## Parte A · Batch

### Código (TODO del enunciado)

| Archivo | TODO | Estado |
|---|---|---|
| `ingest.py` | 1–2: partición de llegada y JSON Lines | ✅ Luis · `test_ingest.py` 7/7 |
| `jobs/bronze.py` | 1–2: metadatos de ingesta y `replaceWhere` | ✅ Luis · validaciones en verde en los 2 lotes |
| `jobs/silver.py` | 1–5: deduplicación, tipado, calidad, `MERGE`, PII | ✅ Luis · validaciones en verde en los 2 lotes |
| `jobs/gold.py` | 1–3: tres tablas de negocio | ✅ Luis · validaciones en verde en los 2 lotes |
| `jobs/audit.py` | 1–2: time travel | ✅ Luis · validaciones en verde en los 2 lotes |
| `sql/athena.sql` | 1–4 (batch) | ✅ corridas en Athena (`evidencias/athena/athena_resultados.md`); también probadas en local con Spark y Trino |
| `infra/serverless.yml` | ciclo de vida de la zona raw | ✅ `raw/`: STANDARD_IA a 30 días, GLACIER_IR a 90, expira a los 10 años · desplegado en AWS |

### Ejecución local (A.0 y A.1) → `evidencias/a_batch_local/`

| Paso | Estado | Evidencia |
|---|---|---|
| A.0 Generar lote 1 (30.600 transacciones) y lote 2 (2.316) | ✅ | `data/` (no se sube a git) |
| `pytest tests/test_ingest.py` | ✅ 7/7 | `pytest_test_ingest.txt` |
| Lote 1 · primera ejecución | ✅ 38 ✓ · 0 ✗ | `lote1_ejecucion1.txt` |
| Lote 1 · segunda ejecución (idempotencia) | ✅ 38 ✓ · 0 ✗ · auditoría: 0 insertadas, 0 actualizadas | `lote1_ejecucion2.txt` |
| Lote 2 · primera ejecución | ✅ 38 ✓ · 0 ✗ · 1.882 insertadas, 276 contracargos (COP 23.933.218,70) | `lote2_ejecucion1.txt` |
| Lote 2 · segunda ejecución (idempotencia) | ✅ 38 ✓ · 0 ✗ · auditoría: 0 insertadas, 0 actualizadas | `lote2_ejecucion2.txt` |

### Preguntas del informe: 1 a 6 (Bronze, Silver, MERGE, Time travel, PII, Consumo) ✅ `informe/informe_lab2.md`

---

## Parte B · Streaming

> Requisito del enunciado: `silver.transactions` debe existir (Parte A con el lote 1) ✅

### Código (TODO del enunciado)

| Archivo | TODO | Estado |
|---|---|---|
| `producer.py` | 1–3: partition key, configuración de Kafka, reintentos en Kinesis | ✅ `test_producer.py` 8/8 · prueba real en Kafka: 100 eventos, 0 perdidos |
| `jobs/stream.py` | 1–6: esquema, deduplicación con watermark, `latest_per_transaction`, `MERGE` por micro-lote, métricas, alertas | ✅ 17 ✓ · 0 ✗ en availableNow y en vivo |
| `sql/athena.sql` | 5–6 (streaming) | ✅ corridas en Athena: 93,75 % a 20/s y ~25,11 % a 50/s |

### Ejecución local (B.1 a B.3) → `evidencias/b_streaming_local/`

| Paso | Estado | Evidencia |
|---|---|---|
| B.1 `pytest tests/test_producer.py` en verde | ✅ 8/8 | `pytest_test_producer.txt` |
| B.2 `run_local.sh stream` en modo availableNow · primera vez | ✅ 12.288 eventos · 17 ✓ · 0 ✗ | `productor_fast_600s.txt`, `stream_availablenow_1.txt` |
| B.2 `run_local.sh stream` en modo availableNow · segunda vez (idempotencia) | ✅ 0 filas leídas en las 4 consultas · tablas sin cambios | `stream_availablenow_2_idempotencia.txt` |
| B.2 `run_local.sh stream 180` en vivo, con el productor en paralelo | ✅ 2.487 eventos en vivo · 17 ✓ · 0 ✗ | `productor_en_vivo_120s.txt`, `stream_en_vivo_180s.txt` |
| B.3 Experimento 1 · Watermark (availableNow vs en vivo) | ✅ 6 descartadas en availableNow, 51 en vivo a 20/s, 100 a 50/s · causa verificada con los checkpoints · respondido en el informe | `experimentos_resumen.md` |
| B.3 Experimento 2 · Carga (20 vs 50 eventos/s) | ✅ local: 47 alertas (80,85 %) a 20/s y 375 (26,40 %) a 50/s · AWS: 93,75 % y ~25,11 % · respondido en el informe | `experimentos_resumen.md` |
| B.3 Experimento 3 · Partition key (constante y `transaction_id`) | ✅ constante 0/1.227/0; `transaction_id` 438/397/392; `customer_id` 491/341/395 · respondido en el informe | `experimento3_partition_key.txt` |

### Preguntas del informe: 7 a 14 (Garantías, Watermark, Latencia, Precisión, Estado final, Partition key, Batch y streaming, Costos) ✅ `informe/informe_lab2.md`

---

## Ejecución en AWS (Partes A y B) → `evidencias/aws_glue/`, `evidencias/athena/`, `evidencias/limpieza/`

Todo el mismo día, cuando las dos partes estén en verde en local. Kinesis cobra por hora mientras exista.
Comandos exactos, costos y verificación de la limpieza: [`AWS_PASO_A_PASO.md`](AWS_PASO_A_PASO.md).

**Corrida completa: 2026-11-04.** Antes hubo una corrida fallida en el stream (2026-11-02) y una prueba reducida (2026-11-03): `aws_glue/ejecucion_20261102_1633.txt` y `aws_glue/prueba_*`.

| Paso | Estado | Evidencia |
|---|---|---|
| Instalar Serverless Framework v3 | ✅ 3.40.0 · `serverless print` resuelve el stack sin errores | |
| Crear `.envs/aws.env` con `STUDENT_ID`; credenciales de la AWS CLI (nunca se sube) | ✅ | |
| `serverless deploy --stage dev` | ✅ | `aws_glue/ejecucion_20261104_0008.txt` |
| Parte A: `run_aws.sh` lote 1 y lote 2 · 4 jobs batch en `Succeeded` | ✅ 8 jobs `SUCCEEDED`, salida de AWS CLI (sin captura de la consola) | `capturas/02_glue_10_jobs_succeeded.png` |
| Parte B: `run_aws.sh stream 300` · job `paytrack-stream` en `Succeeded` | ✅ a 20 y a 50 eventos/s, salida de AWS CLI y log de CloudWatch | `capturas/02`, `03` y `04` |
| Athena: 6 consultas con resultados y MB escaneados | ✅ por CLI, en las dos pasadas | `athena/athena_resultados*.md` |
| Limpieza: bucket vacío, stack borrado y **ningún stream de Kinesis** | ✅ verificado por CLI | `limpieza/recursos_eliminados.png` |

---

## Diagrama e informe

| Entregable | Estado | Dónde |
|---|---|---|
| Diagrama de la arquitectura en draw.io: zonas, tablas por capa, jobs, consultas de streaming y servicios de AWS | ✅ | `diagrama/arquitectura_lakehouse.drawio` |
| Informe en PDF (máx. 8 páginas) con las respuestas y los experimentos | ✅ 14 preguntas, experimentos y diagrama | `informe/informe_lab2.pdf` |

## Entregables del enunciado (lista literal)

- [ ] Tu código: `ingest.py`, `producer.py`, los 5 jobs, `athena.sql` y `serverless.yml`
- [ ] Captura de `pytest` en verde (los dos archivos de tests)
- [ ] Salida de `run_local.sh` para los dos lotes, incluida la segunda ejecución del lote 2 (idempotencia)
- [ ] Salida de `run_local.sh stream` en modo availableNow (dos veces: idempotencia) y en vivo
- [ ] Captura de los 4 jobs batch en `Succeeded` para los dos lotes, y del job `paytrack-stream` en `Succeeded`
- [ ] Resultados y MB escaneados de las 6 consultas de Athena
- [ ] Diagrama de la arquitectura hecho en draw.io: zonas, tablas por capa, jobs, consultas de streaming y servicios de AWS
- [ ] Informe en PDF (máx. 8 páginas) con las respuestas y los experimentos
- [ ] **Recursos eliminados**, incluido el stream de Kinesis

## Rama y PR

Todo el Lab 2 va en la rama `lab2`, creada desde el `main` del profesor. La entrega del Lab 1 (PR #7) sale de
`main` y no se toca. Al terminar se abre un PR nuevo de `lab2` hacia `asacrea/big-data-labs-2026-2:main`.
