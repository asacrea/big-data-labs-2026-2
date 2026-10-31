-- ============================================================
-- Consultas analíticas sobre el esquema OLTP
-- Ejecuta con EXPLAIN ANALYZE para ver los planes de ejecución.
-- Tu tarea: explicar en el informe por qué cada una es costosa en OLTP.
-- ============================================================

-- Q1: Monto total por categoría de comercio (últimos 30 días)
EXPLAIN ANALYZE
SELECT
    m.category,
    COUNT(*)                           AS num_transactions,
    SUM(t.amount_cop)                  AS total_cop,
    AVG(t.amount_cop)                  AS avg_cop
FROM oltp.transactions t
JOIN oltp.merchants m ON t.merchant_id = m.merchant_id
WHERE t.created_at >= CURRENT_DATE - 30
  AND t.status = 'approved'
GROUP BY m.category
ORDER BY total_cop DESC;

-- Q2: Tasa de fraude por segmento de cliente
EXPLAIN ANALYZE
SELECT
    c.segment,
    COUNT(*)                                              AS total_txn,
    SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END)          AS fraud_txn,
    ROUND(
        100.0 * SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                     AS fraud_rate_pct
FROM oltp.transactions t
JOIN oltp.customers c ON t.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY fraud_rate_pct DESC;

-- Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días)
EXPLAIN ANALYZE
SELECT
    EXTRACT(DOW  FROM t.created_at) AS day_of_week,
    EXTRACT(HOUR FROM t.created_at) AS hour,
    COUNT(*)                        AS num_transactions,
    SUM(t.amount_cop)               AS total_cop
FROM oltp.transactions t
WHERE t.status = 'approved'
  AND t.created_at >= CURRENT_DATE - 7
GROUP BY day_of_week, hour
ORDER BY day_of_week, hour;
