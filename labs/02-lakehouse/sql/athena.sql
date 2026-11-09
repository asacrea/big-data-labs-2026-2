-- ============================================================
-- Consumo: consultas Athena sobre las tablas Delta del lakehouse
--
-- Ejecútalas en la consola de Athena con el workgroup paytrack-lakehouse-wg.
-- Bucket del deploy: paytrack-lakehouse-zuniga-733347224713
-- Anota los MB escaneados de cada consulta (pestaña "Query stats").
-- ============================================================

-- ── Dado: registrar las tablas Delta en el catálogo ─────────
-- Athena lee el esquema y las particiones directamente del _delta_log.
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.silver_transactions
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/silver/transactions/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_daily_merchant_summary
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/gold/daily_merchant_summary/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_weekly_report
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/gold/fraud_weekly_report/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_hourly_volume
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/gold/hourly_volume/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_status_changes
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/gold/status_changes/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Tablas del streaming (parte B)
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_merchant_minute_metrics
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/gold/merchant_minute_metrics/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_alerts
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/gold/fraud_alerts/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.bronze_transaction_events
LOCATION 's3://paytrack-lakehouse-zuniga-733347224713/lakehouse/bronze/transaction_events/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- ── TODO 1 ──────────────────────────────────────────────────
-- Los 5 comercios con mayor monto aprobado en septiembre de 2026 y su tasa
-- de aprobación del mes, usando gold_daily_merchant_summary.
-- Ojo: no promedies approval_rate_pct diaria; calcula la del mes.
-- Filtra por las columnas de partición.

SELECT
    merchant_id,
    MAX(merchant_name)    AS merchant_name,
    SUM(approved_cop)     AS approved_cop,
    SUM(num_transactions) AS transacciones,
    -- Tasa del mes = aprobadas del mes / transacciones del mes. Las aprobadas de cada día se
    -- recuperan de la tasa diaria (tasa x transacciones / 100) y se redondean porque la tasa
    -- viene con 2 decimales. Promediar las tasas diarias le daría el mismo peso a un día de
    -- 10 transacciones que a uno de 300.
    ROUND(100.0 * SUM(ROUND(approval_rate_pct * num_transactions / 100)) / SUM(num_transactions), 2)
                          AS approval_rate_pct
FROM paytrack_lakehouse.gold_daily_merchant_summary
WHERE year = '2026' AND month = '09'   -- columnas de partición: Athena solo lee la carpeta de septiembre
GROUP BY merchant_id
ORDER BY approved_cop DESC
LIMIT 5;


-- ── TODO 2 ──────────────────────────────────────────────────
-- La misma pregunta del TODO 1, pero calculada desde silver_transactions.
-- Compara resultados y MB escaneados con el TODO 1.

SELECT
    merchant_id,
    MAX(merchant_name)                                            AS merchant_name,
    SUM(CASE WHEN status = 'approved' THEN amount_cop ELSE 0 END) AS approved_cop,
    COUNT(*)                                                      AS transacciones,
    ROUND(100.0 * COUNT_IF(status = 'approved') / COUNT(*), 2)    AS approval_rate_pct
FROM paytrack_lakehouse.silver_transactions
WHERE year = '2026' AND month = '09'   -- silver también está particionada por year/month
GROUP BY merchant_id
ORDER BY approved_cop DESC
LIMIT 5;


-- ── TODO 3 ──────────────────────────────────────────────────
-- Para cada segmento de cliente: la semana con más eventos de fraude y
-- cuántos eventos tuvo (gold_fraud_weekly_report).
-- Pista: funciones de ventana (ROW_NUMBER ... OVER (PARTITION BY ...)).

WITH por_semana AS (
    -- la tabla tiene una fila por semana, patrón y segmento: primero se suman los patrones
    SELECT segment, week_start, SUM(num_events) AS eventos
    FROM paytrack_lakehouse.gold_fraud_weekly_report
    GROUP BY segment, week_start
),
ranking AS (
    SELECT
        segment, week_start, eventos,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY eventos DESC, week_start) AS puesto
    FROM por_semana
)
SELECT segment, week_start, eventos
FROM ranking
WHERE puesto = 1
ORDER BY eventos DESC;


-- ── TODO 4 ──────────────────────────────────────────────────
-- Impacto de los contracargos del último lote: número de transacciones y
-- monto revertido por categoría de comercio (gold_status_changes + silver_transactions).

-- gold_status_changes solo guarda los cambios del último MERGE del batch (el lote 2).
SELECT
    s.merchant_category,
    COUNT(*)          AS contracargos,
    SUM(c.amount_cop) AS monto_revertido_cop
FROM paytrack_lakehouse.gold_status_changes c
JOIN paytrack_lakehouse.silver_transactions s
  ON s.transaction_id = c.transaction_id
WHERE c.old_status = 'approved' AND c.new_status = 'chargeback'
GROUP BY s.merchant_category
ORDER BY monto_revertido_cop DESC;


-- ── TODO 5 (streaming) ──────────────────────────────────────
-- Precisión de las alertas de ráfagas: de las alertas de gold_fraud_alerts,
-- ¿qué porcentaje corresponde a clientes que de verdad cometieron fraude en
-- esa ventana? Una alerta es un acierto si alguno de los pagos del cliente con
-- event_time dentro de [window_start, window_end) tiene is_fraud = true
-- (bronze_transaction_events). Reporta alertas, aciertos y % de precisión.

WITH evaluadas AS (
    SELECT
        a.window_start, a.window_end, a.customer_id,
        MAX(CASE WHEN e.is_fraud THEN 1 ELSE 0 END) AS acierto
    FROM paytrack_lakehouse.gold_fraud_alerts a
    LEFT JOIN paytrack_lakehouse.bronze_transaction_events e
           ON e.customer_id = a.customer_id
          AND e.event_type = 'payment'
          AND e.event_time >= a.window_start
          AND e.event_time <  a.window_end
    GROUP BY a.window_start, a.window_end, a.customer_id
)
SELECT
    COUNT(*)                                  AS alertas,
    SUM(acierto)                              AS aciertos,
    ROUND(100.0 * SUM(acierto) / COUNT(*), 1) AS precision_pct
FROM evaluadas;

-- La misma medición separada por corrida (20 y 50 eventos/s). Una pausa de más de
-- 2 minutos entre ventanas separa una corrida de la siguiente.
WITH evaluadas AS (
    SELECT
        a.window_start, a.window_end, a.customer_id,
        MAX(CASE WHEN e.is_fraud THEN 1 ELSE 0 END) AS acierto
    FROM paytrack_lakehouse.gold_fraud_alerts a
    LEFT JOIN paytrack_lakehouse.bronze_transaction_events e
           ON e.customer_id = a.customer_id
          AND e.event_type = 'payment'
          AND e.event_time >= a.window_start
          AND e.event_time <  a.window_end
    GROUP BY a.window_start, a.window_end, a.customer_id
),
marcadas AS (
    SELECT
        *,
        CASE WHEN window_start > LAG(window_start) OVER (ORDER BY window_start) + INTERVAL '2' MINUTE
             THEN 1 ELSE 0 END AS empieza_corrida
    FROM evaluadas
),
numeradas AS (
    SELECT *, 1 + SUM(empieza_corrida) OVER (ORDER BY window_start ROWS UNBOUNDED PRECEDING) AS corrida
    FROM marcadas
)
SELECT
    corrida,
    MIN(window_start)                         AS desde,
    MAX(window_end)                           AS hasta,
    COUNT(*)                                  AS alertas,
    SUM(acierto)                              AS aciertos,
    ROUND(100.0 * SUM(acierto) / COUNT(*), 1) AS precision_pct
FROM numeradas
GROUP BY corrida
ORDER BY corrida;


-- ── TODO 6 (streaming) ──────────────────────────────────────
-- Capa rápida vs. capa batch: para cada día que cubre el streaming, compara el
-- número de pagos por comercio según gold_merchant_minute_metrics (suma de las
-- ventanas) con el número de transacciones del stream en silver_transactions
-- (las que tienen transaction_id con prefijo 'TXNS'). Muestra la diferencia.
-- ¿De dónde sale? (pista: watermark y ventanas que aún no se cierran)

WITH capa_rapida AS (
    SELECT CAST(window_start AS DATE) AS dia, merchant_id, SUM(num_payments) AS pagos_streaming
    FROM paytrack_lakehouse.gold_merchant_minute_metrics
    GROUP BY 1, 2
),
capa_batch AS (
    SELECT CAST(event_ts AS DATE) AS dia, merchant_id, COUNT(*) AS pagos_silver
    FROM paytrack_lakehouse.silver_transactions
    WHERE year = '2026' AND month = '11'   -- mes en que corrió el stream: no lee las particiones del batch
      AND transaction_id LIKE 'TXNS%'
    GROUP BY 1, 2
)
SELECT
    COALESCE(b.dia, r.dia)                                       AS dia,
    COALESCE(b.merchant_id, r.merchant_id)                       AS merchant_id,
    COALESCE(b.pagos_silver, 0)                                  AS pagos_silver,
    COALESCE(r.pagos_streaming, 0)                               AS pagos_streaming,
    COALESCE(b.pagos_silver, 0) - COALESCE(r.pagos_streaming, 0) AS diferencia
FROM capa_batch b
FULL OUTER JOIN capa_rapida r
  ON r.dia = b.dia AND r.merchant_id = b.merchant_id
ORDER BY dia, merchant_id;

