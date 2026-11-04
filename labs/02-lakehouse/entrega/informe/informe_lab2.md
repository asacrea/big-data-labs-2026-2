# Lab 2 · Lakehouse end-to-end en AWS: batch y streaming

**SI7006 Almacenamiento y Procesamiento de Grandes Datos · Maestría en Ingeniería de Datos · EAFIT 2026-2**  
**Caso:** PayTrack Colombia · **Equipo:** Luis Alberto Ortiz Quiroga · Santiago Vélez Casallas  
**Repositorio:** https://github.com/santiagovelcasallas/big-data-labs-2026-2 (rama `lab2`)

> Borrador. Se entrega en PDF, máximo 8 páginas, con las respuestas y los experimentos.
> Las preguntas son las del enunciado, literales; las evidencias van en `entrega/evidencias/`.

## Parte A · Batch

**1. Bronze.** ¿Por qué Bronze lee todo como texto? ¿Qué problema evita `replaceWhere` frente a `append`? Muéstralo con lo que pasó en tu prueba de idempotencia.

_Pendiente._

**2. Silver.** ¿Cuántas filas quedaron en cuarentena por cada motivo en cada lote? ¿Por qué se deduplica **antes** de aplicar las reglas de calidad?

_Pendiente._

**3. MERGE.** ¿Por qué la actualización solo debe ocurrir cuando el `status` cambió? ¿Qué reportaría la auditoría si quitaras esa condición y re-ejecutaras un lote?

_Pendiente._

**4. Time travel.** ¿Cuántos contracargos encontró la auditoría y qué monto revierten? ¿Qué pasaría con esta auditoría después de un `VACUUM`?

_Pendiente._

**5. PII.** `silver.customers` se actualiza sobrescribiendo los cambios (tipo 1). ¿Qué análisis de negocio se pierde? ¿Cómo lo resolverías con lo que hiciste en el Lab 1?

_Pendiente._

**6. Consumo.** Compara los MB escaneados del TODO 1 y del TODO 2 de Athena. ¿Por qué existe Gold si Silver ya responde la pregunta? ¿Qué pierdes?

_Pendiente._

## Parte B · Streaming

**7. Garantías de entrega.** ¿Qué garantía da tu productor: *at-most-once*, *at-least-once* o *exactly-once*? ¿Y Kinesis con tus reintentos? Explica qué combinación de checkpoint, Delta y `MERGE` hace que las tablas queden como si cada evento se hubiera procesado exactamente una vez. Usa tu prueba de idempotencia como evidencia.

_Pendiente._

**8. Watermark.** Responde el experimento 1. ¿Por qué la consulta `silver` no usa watermark? ¿Qué pasaría con los pagos tardíos si lo usara?

_Pendiente._

**9. Latencia.** ¿Cuánto tarda en aparecer una alerta en `gold.fraud_alerts` después del último pago de la ráfaga? ¿Qué parte de ese tiempo se debe al modo *append* y al watermark? ¿Cómo la bajarías y qué perderías?

_Pendiente._

**10. Precisión.** Reporta la precisión de las alertas (TODO 5) a 20 y a 50 eventos/s. ¿Por qué cambia? ¿Qué ajustarías para mejorarla?

_Pendiente._

**11. Estado final.** En el TODO 4, ¿qué escenario concreto del productor revertiría un contracargo si quitaras la condición sobre `'chargeback'`? ¿Por qué `latest_per_transaction` no basta para evitarlo?

_Pendiente._

**12. Partition key.** Con el experimento 3: ¿cómo se repartió la carga con cada llave? ¿Cambiaría el número de alertas que detecta Spark con `transaction_id` como llave? Si Spark agrupa por cliente de todas formas, ¿para qué sirve la partition key? (piensa en el contracargo y su pago).

_Pendiente._

**13. Batch y streaming en la misma tabla.** Revisa `DESCRIBE HISTORY` de `silver.transactions` después del streaming: ¿cuántas versiones creó?, ¿cuántas filas reescribió cada `MERGE` (`numTargetRowsCopied`)? ¿Qué problema anticipas a 500K pagos/día y cómo lo resolverías? ¿Qué pasaría si el job silver batch y el stream escribieran al mismo tiempo? Con el TODO 6 de Athena: ¿cuál es la fuente de verdad, la capa rápida o la batch?

_Pendiente._

**14. Costos y operación.** Estima el costo mensual de las dos partes con 500K transacciones diarias (S3, Glue, Glue Streaming 24/7, Kinesis y Athena). ¿Qué cambiarías si el streaming no necesitara correr 24/7? ¿Por qué no se debe poner una regla de expiración de S3 sobre `lakehouse/`?

_Pendiente._

## Experimentos (B.3)

### Experimento 1 · Watermark

**Anota las filas *descartadas por watermark* de `metrics` en modo availableNow (`--fast`) y en vivo.**

| Corrida | Eventos | Descartadas en `metrics` | % |
|---|---|---|---|
| Local · availableNow (`--fast`, 10 min simulados a 20/s) | 12.288 | **6** | 0,05 % |
| Local · en vivo a 20/s | 2.487 | **51** (46 + 5 al apagar) | 2,05 % |
| Local · en vivo a 50/s | 6.195 | **100** (97 + 3 al apagar) | 1,61 % |
| AWS · en vivo a 20/s | 4.965 | **79** | 1,59 % |
| AWS · en vivo a 50/s | 12.307 | **132** | 1,07 % |

**¿Por qué son tan distintas si el productor envía la misma proporción de eventos tardíos?**
Porque el watermark no mide el tiempo real: es el mayor `event_time` visto menos 2 minutos, y solo avanza entre micro-lotes.
Lo que cambia es cuánto tiempo de evento cabe en cada micro-lote.

- **En vivo**, cada micro-lote cubre unos 10 s. El watermark sigue de cerca al tiempo de evento, así que un pago con más de 2 minutos de retraso llega cuando el umbral ya lo superó. A 20/s llegaron 61 eventos con más de 120 s de retraso y Spark descartó 51. La diferencia es consistente con que Spark filtra con el watermark del lote anterior, que va unos segundos atrás.
- **Con `--fast`**, los 10 minutos simulados se procesaron en 4 micro-lotes con datos, de unos 4.000 eventos en los tres primeros. El tardío llega junto a eventos de su misma época, y Spark filtra con el watermark del lote anterior: los dos primeros lotes no descartan nada porque ese valor aún es 0.

Se comprobó recalculando evento por evento con los watermarks guardados en el checkpoint: 0 + 0 + 5 + 1 = **6 filas, igual a lo que reportó Spark**.
Conclusión: `--fast` subestima los descartes, y la cifra representativa de producción es la de la corrida en vivo.

![Salida del experimento 1](../evidencias/capturas/10_exp1_watermark.png)

### Experimento 2 · Carga

**Repite la prueba en vivo con `--rate 50`. ¿Qué pasa con el número de alertas?**
Crece mucho más que la carga: con 2,5 veces más eventos hay **8 veces más alertas**. Los aciertos crecen como la carga y las falsas alarmas se disparan.

| Local, solo ventanas de cada corrida | Eventos | Alertas | Aciertos | Falsas alarmas | Precisión |
|---|---|---|---|---|---|
| 20 eventos/s | 2.487 | 47 | 38 | 9 | 80,85 % |
| 50 eventos/s | 6.195 | 375 | 99 | 276 | 26,40 % |

Las ventanas de la corrida a 50/s no se habían cerrado al terminar, porque el productor dura 120 s y el watermark va 2 minutos atrás. Se cerraron enviando 60 eventos nuevos, y se contaron solo las ventanas de cada corrida.

**Mide su precisión en AWS con el TODO 5 de Athena, a 20 y a 50 eventos/s.**

| AWS, TODO 5 | Alertas | Aciertos | Precisión |
|---|---|---|---|
| Tras la corrida a 20 eventos/s | 32 | 30 | **93,75 %** |
| Acumulado tras la corrida a 50 eventos/s | 470 | 140 | 29,79 % |
| Diferencia atribuible a la corrida a 50 eventos/s | 438 | 110 | **25,11 %** |

El 25,11 % es una aproximación: esa diferencia incluye las últimas ventanas de la corrida a 20/s, que se cerraron durante la de 50/s. Las tablas de AWS ya no existen, así que no se puede separar, pero la medición local limpia (26,40 %) da un valor muy parecido.

**Por qué cae la precisión.** La regla (6 o más pagos en 30 s) no cambia, pero la actividad normal sí. Con 500 clientes, un cliente hace en promedio 1,14 pagos cada 30 s a 20/s y 2,85 a 50/s. Si se modela como una distribución de Poisson, la probabilidad de llegar a 6 pagos por azar pasa de 0,12 % a 6,96 %: unas 60 veces más. Las ráfagas de fraude reales solo crecen con la tasa.

![Salida del experimento 2](../evidencias/capturas/11_exp2_carga_alertas.png)

### Experimento 3 · Partition key

**Cambia temporalmente `partition_key` para que devuelva un valor constante, y repite con `transaction_id`. Revisa el reparto con `kafka-get-offsets.sh`.**
Mismo stream en los tres casos (1.227 eventos, `--seed 7`), en topics de 3 particiones. `customer_id` es nuestra llave, incluida como referencia. La llave se restauró al terminar.

| Llave | p0 | p1 | p2 | Partición más cargada frente al reparto ideal (409) |
|---|---|---|---|---|
| Constante | 0 | **1.227** | 0 | +200 % |
| `transaction_id` | 438 | 397 | 392 | +7 % |
| `customer_id` | 491 | 341 | 395 | +20 % |

- **Constante:** todo cae en una sola partición, así que no hay paralelismo y los demás consumidores quedan sin trabajo.
- **`transaction_id`:** es el reparto más parejo, porque casi cada evento tiene una llave distinta.
- **`customer_id`:** reparte peor porque hay solo 500 llaves y las ráfagas concentran varios pagos en una misma llave. Es lo que se paga por mantener en orden todos los eventos de un cliente.

![Salida del experimento 3](../evidencias/capturas/12_exp3_partition_key.png)

## Anexos

- **A.** Diagrama de la arquitectura (`entrega/diagrama/`)
- **B.** Evidencias de ejecución local, AWS, Athena y limpieza (`entrega/evidencias/`)
