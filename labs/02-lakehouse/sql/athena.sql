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

SELECT
    merchant_id,
    merchant_name,
    SUM(approved_cop) AS approved_cop,
    SUM(num_transactions) AS num_transactions,
    -- tasa del mes: aprobadas / total, reconstruyendo las aprobadas de cada día
    ROUND(100.0 * SUM(approval_rate_pct * num_transactions / 100.0) / SUM(num_transactions), 2) AS approval_rate_pct
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
    SUM(CASE WHEN status = 'approved' THEN amount_cop END) AS approved_cop,
    COUNT(*) AS num_transactions,
    ROUND(100.0 * COUNT_IF(status = 'approved') / COUNT(*), 2) AS approval_rate_pct
FROM paytrack_lakehouse.silver_transactions
WHERE year = '2026' AND month = '09'
GROUP BY merchant_id, merchant_name
ORDER BY approved_cop DESC
LIMIT 5;

-- ── TODO 3 ──────────────────────────────────────────────────
-- Para cada segmento de cliente: la semana con más eventos de fraude y
-- cuántos eventos tuvo (gold_fraud_weekly_report).
-- Pista: funciones de ventana (ROW_NUMBER ... OVER (PARTITION BY ...)).

WITH weekly AS (
    -- el reporte tiene una fila por patrón: se suman los patrones de cada semana
    SELECT segment, week_start, SUM(num_events) AS num_events
    FROM paytrack_lakehouse.gold_fraud_weekly_report
    GROUP BY segment, week_start
),
ranked AS (
    SELECT segment, week_start, num_events,
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
    COUNT(*) AS num_chargebacks,
    SUM(c.amount_cop) AS reversed_cop
FROM paytrack_lakehouse.gold_status_changes c
JOIN paytrack_lakehouse.silver_transactions t ON t.transaction_id = c.transaction_id
WHERE c.new_status = 'chargeback'
GROUP BY t.merchant_category
ORDER BY reversed_cop DESC;

-- ── TODO 5 (streaming) ──────────────────────────────────────
-- Precisión de las alertas de ráfagas: de las alertas de gold_fraud_alerts,
-- ¿qué porcentaje corresponde a clientes que de verdad cometieron fraude en
-- esa ventana? Una alerta es un acierto si alguno de los pagos del cliente con
-- event_time dentro de [window_start, window_end) tiene is_fraud = true
-- (bronze_transaction_events). Reporta alertas, aciertos y % de precisión.

WITH alert_hits AS (
    SELECT
        a.window_start,
        a.customer_id,
        MAX(CASE WHEN e.is_fraud THEN 1 ELSE 0 END) AS is_hit
    FROM paytrack_lakehouse.gold_fraud_alerts a
    LEFT JOIN paytrack_lakehouse.bronze_transaction_events e
           ON e.customer_id = a.customer_id
          AND e.event_type = 'payment'
          AND e.event_time >= a.window_start
          AND e.event_time < a.window_end
    GROUP BY a.window_start, a.customer_id
)
SELECT
    COUNT(*) AS alerts,
    SUM(is_hit) AS hits,
    ROUND(100.0 * SUM(is_hit) / COUNT(*), 2) AS precision_pct
FROM alert_hits;

-- ── TODO 6 (streaming) ──────────────────────────────────────
-- Capa rápida vs. capa batch: para cada día que cubre el streaming, compara el
-- número de pagos por comercio según gold_merchant_minute_metrics (suma de las
-- ventanas) con el número de transacciones del stream en silver_transactions
-- (las que tienen transaction_id con prefijo 'TXNS'). Muestra la diferencia.
-- ¿De dónde sale? (pista: watermark y ventanas que aún no se cierran)

WITH fast AS (
    SELECT CAST(window_start AS DATE) AS day, merchant_id, SUM(num_payments) AS fast_payments
    FROM paytrack_lakehouse.gold_merchant_minute_metrics
    GROUP BY CAST(window_start AS DATE), merchant_id
),
batch AS (
    SELECT CAST(event_ts AS DATE) AS day, merchant_id, COUNT(*) AS silver_payments
    FROM paytrack_lakehouse.silver_transactions
    WHERE transaction_id LIKE 'TXNS%'
    GROUP BY CAST(event_ts AS DATE), merchant_id
)
SELECT
    COALESCE(f.day, b.day) AS day,
    COALESCE(f.merchant_id, b.merchant_id) AS merchant_id,
    COALESCE(f.fast_payments, 0) AS fast_payments,
    COALESCE(b.silver_payments, 0) AS silver_payments,
    COALESCE(b.silver_payments, 0) - COALESCE(f.fast_payments, 0) AS difference
FROM fast f
FULL OUTER JOIN batch b ON f.day = b.day AND f.merchant_id = b.merchant_id
ORDER BY day, merchant_id;
