-- ============================================================
-- Parte D, punto 4: experimentos con los índices de fact_transactions
-- Cada experimento va entre BEGIN y ROLLBACK. El ROLLBACK deshace el
-- DROP INDEX y los SET LOCAL, y la base queda como estaba.
-- Las consultas son las mismas de sql/olap_queries.sql.
-- ============================================================

-- CURRENT_DATE con la fecha de Colombia, para que las ventanas de
-- 30 y 7 días sean las mismas de la Parte A aunque pasen las 7 p. m.
SET TIME ZONE 'America/Bogota';

\echo '=== 1. Línea base: con los tres índices ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT m.category, COUNT(*) AS num_transactions,
       SUM(f.amount_cop) AS total_cop, AVG(f.amount_cop) AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dw.dim_date     d ON f.date_key     = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30 AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT c.segment, COUNT(*) AS total_txn,
       SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_txn,
       ROUND(100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY fraud_rate_pct DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT d.day_of_week, t.hour, COUNT(*) AS num_transactions, SUM(f.amount_cop) AS total_cop
FROM dw.fact_transactions f
JOIN dw.dim_date d ON f.date_key = d.date_key
JOIN dw.dim_time t ON f.time_key = t.time_key
WHERE f.is_approved AND d.full_date >= CURRENT_DATE - 7
GROUP BY d.day_of_week, t.hour
ORDER BY d.day_of_week, t.hour;

\echo '=== 2. Sin ix_fact_date: Q1 y Q3 ==='
BEGIN;
DROP INDEX dw.ix_fact_date;
EXPLAIN (ANALYZE, BUFFERS)
SELECT m.category, COUNT(*) AS num_transactions,
       SUM(f.amount_cop) AS total_cop, AVG(f.amount_cop) AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dw.dim_date     d ON f.date_key     = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30 AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT d.day_of_week, t.hour, COUNT(*) AS num_transactions, SUM(f.amount_cop) AS total_cop
FROM dw.fact_transactions f
JOIN dw.dim_date d ON f.date_key = d.date_key
JOIN dw.dim_time t ON f.time_key = t.time_key
WHERE f.is_approved AND d.full_date >= CURRENT_DATE - 7
GROUP BY d.day_of_week, t.hour
ORDER BY d.day_of_week, t.hour;
ROLLBACK;

\echo '=== 3. Sin ix_fact_merchant: Q1 ==='
BEGIN;
DROP INDEX dw.ix_fact_merchant;
EXPLAIN (ANALYZE, BUFFERS)
SELECT m.category, COUNT(*) AS num_transactions,
       SUM(f.amount_cop) AS total_cop, AVG(f.amount_cop) AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dw.dim_date     d ON f.date_key     = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30 AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;
ROLLBACK;

\echo '=== 4. Sin ix_fact_customer: Q2 ==='
BEGIN;
DROP INDEX dw.ix_fact_customer;
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.segment, COUNT(*) AS total_txn,
       SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_txn,
       ROUND(100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY fraud_rate_pct DESC;
ROLLBACK;

\echo '=== 5. Q2 obligada a usar índices (enable_seqscan = off) ==='
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.segment, COUNT(*) AS total_txn,
       SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_txn,
       ROUND(100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY fraud_rate_pct DESC;
ROLLBACK;

\echo '=== 6. Q1 obligada a buscar día por día en ix_fact_date ==='
BEGIN;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT m.category, COUNT(*) AS num_transactions,
       SUM(f.amount_cop) AS total_cop, AVG(f.amount_cop) AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dw.dim_date     d ON f.date_key     = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30 AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;
ROLLBACK;

\echo '=== 7. Q1 y Q2 con random_page_cost = 1.1 (valor para SSD) ==='
BEGIN;
SET LOCAL random_page_cost = 1.1;
EXPLAIN (ANALYZE, BUFFERS)
SELECT m.category, COUNT(*) AS num_transactions,
       SUM(f.amount_cop) AS total_cop, AVG(f.amount_cop) AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dw.dim_date     d ON f.date_key     = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30 AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT c.segment, COUNT(*) AS total_txn,
       SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_txn,
       ROUND(100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY fraud_rate_pct DESC;
ROLLBACK;