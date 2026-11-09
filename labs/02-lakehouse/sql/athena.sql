-- ============================================================
-- Consumo: consultas Athena sobre las tablas Delta del lakehouse
--
-- Ejecútalas en la consola de Athena con el workgroup paytrack-lakehouse-wg.
-- Reemplaza <BUCKET> por el nombre de tu bucket (salida del deploy).
-- Anota los MB escaneados de cada consulta (pestaña "Query stats").
-- ============================================================

-- ── Dado: registrar las tablas Delta en el catálogo ─────────
-- Athena lee el esquema y las particiones directamente del _delta_log.
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.silver_transactions
LOCATION 's3://<BUCKET>/lakehouse/silver/transactions/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_daily_merchant_summary
LOCATION 's3://<BUCKET>/lakehouse/gold/daily_merchant_summary/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_weekly_report
LOCATION 's3://<BUCKET>/lakehouse/gold/fraud_weekly_report/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_hourly_volume
LOCATION 's3://<BUCKET>/lakehouse/gold/hourly_volume/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_status_changes
LOCATION 's3://<BUCKET>/lakehouse/gold/status_changes/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Tablas del streaming (parte B)
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_merchant_minute_metrics
LOCATION 's3://<BUCKET>/lakehouse/gold/merchant_minute_metrics/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_alerts
LOCATION 's3://<BUCKET>/lakehouse/gold/fraud_alerts/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.bronze_transaction_events
LOCATION 's3://<BUCKET>/lakehouse/bronze/transaction_events/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- ── TODO 1 ──────────────────────────────────────────────────
-- Los 5 comercios con mayor monto aprobado en septiembre de 2026 y su tasa
-- de aprobación del mes, usando gold_daily_merchant_summary.
-- Ojo: no promedies approval_rate_pct diaria; calcula la del mes.
-- Filtra por las columnas de partición.
-- La tasa del mes se pondera por el número de transacciones de cada día:
-- aprobadas del día = approval_rate_pct * num_transactions / 100.
SELECT
    merchant_id,
    merchant_name,
    SUM(approved_cop)                                                   AS approved_cop,
    SUM(num_transactions)                                               AS num_transactions,
    ROUND(100.0 * SUM(approval_rate_pct * num_transactions / 100.0)
          / SUM(num_transactions), 2)                                   AS approval_rate_pct
FROM paytrack_lakehouse.gold_daily_merchant_summary
WHERE year = '2026' AND month = '09'
GROUP BY merchant_id, merchant_name
ORDER BY approved_cop DESC
LIMIT 5;


-- ── TODO 2 ──────────────────────────────────────────────────
-- La misma pregunta del TODO 1, pero calculada desde silver_transactions.
-- Compara resultados y MB escaneados con el TODO 1.
SELECT
    merchant_id,
    merchant_name,
    SUM(CASE WHEN status = 'approved' THEN amount_cop ELSE 0 END)       AS approved_cop,
    COUNT(*)                                                            AS num_transactions,
    ROUND(100.0 * COUNT_IF(status = 'approved') / COUNT(*), 2)          AS approval_rate_pct
FROM paytrack_lakehouse.silver_transactions
WHERE year = '2026' AND month = '09'
GROUP BY merchant_id, merchant_name
ORDER BY approved_cop DESC
LIMIT 5;


-- ── TODO 3 ──────────────────────────────────────────────────
-- Para cada segmento de cliente: la semana con más eventos de fraude y
-- cuántos eventos tuvo (gold_fraud_weekly_report).
-- Pista: funciones de ventana (ROW_NUMBER ... OVER (PARTITION BY ...)).
-- Si dos semanas empatan, se muestra la más antigua.
WITH weekly AS (
    SELECT segment, week_start, SUM(num_events) AS num_events
    FROM paytrack_lakehouse.gold_fraud_weekly_report
    GROUP BY segment, week_start
),
ranked AS (
    SELECT
        segment,
        week_start,
        num_events,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY num_events DESC, week_start) AS rn
    FROM weekly
)
SELECT segment, week_start, num_events
FROM ranked
WHERE rn = 1
ORDER BY segment;


-- ── TODO 4 ──────────────────────────────────────────────────
-- Impacto de los contracargos del último lote: número de transacciones y
-- monto revertido por categoría de comercio (gold_status_changes + silver_transactions).
SELECT
    t.merchant_category,
    COUNT(*)          AS num_chargebacks,
    SUM(c.amount_cop) AS reverted_cop
FROM paytrack_lakehouse.gold_status_changes c
JOIN paytrack_lakehouse.silver_transactions t
  ON t.transaction_id = c.transaction_id
WHERE c.new_status = 'chargeback'
GROUP BY t.merchant_category
ORDER BY reverted_cop DESC;


-- ── TODO 5 (streaming) ──────────────────────────────────────
-- Precisión de las alertas de ráfagas: de las alertas de gold_fraud_alerts,
-- ¿qué porcentaje corresponde a clientes que de verdad cometieron fraude en
-- esa ventana? Una alerta es un acierto si alguno de los pagos del cliente con
-- event_time dentro de [window_start, window_end) tiene is_fraud = true
-- (bronze_transaction_events). Reporta alertas, aciertos y % de precisión.
-- Los reenvíos de bronze no afectan: BOOL_OR solo pregunta si hubo algún pago fraudulento.
WITH scored AS (
    SELECT
        a.window_start,
        a.window_end,
        a.customer_id,
        COALESCE(BOOL_OR(p.is_fraud), false) AS is_hit
    FROM paytrack_lakehouse.gold_fraud_alerts a
    LEFT JOIN paytrack_lakehouse.bronze_transaction_events p
           ON p.customer_id = a.customer_id
          AND p.event_type = 'payment'
          AND p.event_time >= a.window_start
          AND p.event_time <  a.window_end
    GROUP BY a.window_start, a.window_end, a.customer_id
)
SELECT
    COUNT(*)                                       AS alertas,
    COUNT_IF(is_hit)                               AS aciertos,
    ROUND(100.0 * COUNT_IF(is_hit) / COUNT(*), 2)  AS precision_pct
FROM scored;


-- ── TODO 6 (streaming) ──────────────────────────────────────
-- Capa rápida vs. capa batch: para cada día que cubre el streaming, compara el
-- número de pagos por comercio según gold_merchant_minute_metrics (suma de las
-- ventanas) con el número de transacciones del stream en silver_transactions
-- (las que tienen transaction_id con prefijo 'TXNS'). Muestra la diferencia.
-- ¿De dónde sale? (pista: watermark y ventanas que aún no se cierran)
-- Ambas tablas guardan instantes en la misma zona horaria, así que el día es comparable.
WITH fast_layer AS (
    SELECT CAST(window_start AS date) AS day, merchant_id, SUM(num_payments) AS fast_payments
    FROM paytrack_lakehouse.gold_merchant_minute_metrics
    GROUP BY CAST(window_start AS date), merchant_id
),
silver_layer AS (
    SELECT CAST(event_ts AS date) AS day, merchant_id, COUNT(*) AS silver_payments
    FROM paytrack_lakehouse.silver_transactions
    WHERE transaction_id LIKE 'TXNS%'
    GROUP BY CAST(event_ts AS date), merchant_id
)
SELECT
    COALESCE(f.day, s.day)                                        AS day,
    COALESCE(f.merchant_id, s.merchant_id)                        AS merchant_id,
    COALESCE(f.fast_payments, 0)                                  AS fast_payments,
    COALESCE(s.silver_payments, 0)                                AS silver_payments,
    COALESCE(s.silver_payments, 0) - COALESCE(f.fast_payments, 0) AS difference
FROM fast_layer f
FULL OUTER JOIN silver_layer s
  ON f.day = s.day AND f.merchant_id = s.merchant_id
ORDER BY day, merchant_id;
