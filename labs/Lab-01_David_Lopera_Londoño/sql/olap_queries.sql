-- ============================================================
-- Las mismas consultas de negocio sobre el esquema estrella
--
-- Reescribe cada consulta de oltp_problems.sql usando SOLO tablas dw.*
-- Reglas (las valida check_queries.py):
--   * Mantén el EXPLAIN ANALYZE delante de cada SELECT.
--   * Devuelve las mismas columnas, con los mismos nombres y en el mismo
--     orden que la versión OLTP: los resultados deben ser idénticos.
--   * Usa las dimensiones para filtrar y agrupar: nada de EXTRACT() ni
--     funciones de fecha sobre la tabla de hechos.
-- Después compara los planes de ejecución con los de la versión OLTP.
-- ============================================================

-- Q1: Monto total por categoría de comercio (últimos 30 días)
-- El filtro de fecha se aplica sobre dim_date (una fila por día) y el de
-- estado sobre la medida booleana is_approved (ya no es texto).
EXPLAIN ANALYZE
SELECT
    m.category,
    COUNT(*)                           AS num_transactions,
    SUM(f.amount_cop)                  AS total_cop,
    AVG(f.amount_cop)                  AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m ON f.merchant_key = m.merchant_key
JOIN dw.dim_date     d ON f.date_key     = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30
  AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;

-- Q2: Tasa de fraude por segmento de cliente
-- Pista: dim_customer es SCD2. ¿Debes filtrar por is_current? ¿Por qué?
-- No. Cada hecho ya apunta a la versión del cliente vigente cuando ocurrió
-- la transacción, así que el fraude se atribuye al segmento que tenía en ese
-- momento. Filtrar is_current descartaría las transacciones ligadas a
-- versiones históricas y contaría solo las del segmento actual.
EXPLAIN ANALYZE
SELECT
    c.segment,
    COUNT(*)                                              AS total_txn,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)          AS fraud_txn,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                     AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY fraud_rate_pct DESC;

-- Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días)
-- day_of_week y hour ya vienen precalculados en dim_date y dim_time:
-- no hay EXTRACT() en tiempo de consulta.
EXPLAIN ANALYZE
SELECT
    d.day_of_week                   AS day_of_week,
    t.hour                          AS hour,
    COUNT(*)                        AS num_transactions,
    SUM(f.amount_cop)               AS total_cop
FROM dw.fact_transactions f
JOIN dw.dim_date d ON f.date_key = d.date_key
JOIN dw.dim_time t ON f.time_key = t.time_key
WHERE f.is_approved
  AND d.full_date >= CURRENT_DATE - 7
GROUP BY d.day_of_week, t.hour
ORDER BY d.day_of_week, t.hour;