-- ============================================================
-- Verificación del OLTP cargado (Parte A)
-- Archivo nuestro, no del profesor: confirma que load_oltp.py cargó
-- todo y que los datos son consistentes antes de construir el DW.
--
-- Uso:
--   docker exec -i docker-postgres-1 psql -U postgres -d paytrack \
--     < labs/01-modelado/sql/verify_oltp.sql
-- ============================================================

-- 1. Las 5 tablas existen
\dt oltp.*

-- 2. Conteos: deben coincidir con lo que imprimió generate_data.py
--    (500 clientes, 8 comercios, 5 métodos, N transacciones, eventos de fraude)
SELECT 'customers' AS tabla, COUNT(*) AS filas FROM oltp.customers
UNION ALL SELECT 'merchants',       COUNT(*) FROM oltp.merchants
UNION ALL SELECT 'payment_methods', COUNT(*) FROM oltp.payment_methods
UNION ALL SELECT 'transactions',    COUNT(*) FROM oltp.transactions
UNION ALL SELECT 'fraud_events',    COUNT(*) FROM oltp.fraud_events;

-- 3. Rango de fechas y monto total (las consultas usan CURRENT_DATE:
--    el máximo debe ser cercano a hoy o Q1/Q3 devuelven vacío)
SELECT MIN(created_at) AS primera, MAX(created_at) AS ultima,
       SUM(amount_cop) AS total_cop
FROM oltp.transactions;

-- 4. Distribución por estado y fraude
SELECT status, COUNT(*) AS txn, SUM(is_fraud::INT) AS fraudes
FROM oltp.transactions GROUP BY status ORDER BY status;

-- 5. Chequeos de consistencia: todos deben dar 0
SELECT 'fraude sin evento' AS chequeo, COUNT(*) AS debe_ser_0
FROM oltp.transactions t
WHERE t.is_fraud
  AND NOT EXISTS (SELECT 1 FROM oltp.fraud_events f WHERE f.transaction_id = t.transaction_id)
UNION ALL
SELECT 'evento en transacción no fraude', COUNT(*)
FROM oltp.fraud_events f JOIN oltp.transactions t USING (transaction_id)
WHERE NOT t.is_fraud
UNION ALL
SELECT 'transacciones con más de un evento', COUNT(*)
FROM (SELECT transaction_id FROM oltp.fraud_events GROUP BY 1 HAVING COUNT(*) > 1) x
UNION ALL
SELECT 'transacción anterior a la creación del cliente', COUNT(*)
FROM oltp.transactions t JOIN oltp.customers c USING (customer_id)
WHERE t.created_at < c.created_at;

-- 6. Catálogos
SELECT method_id, method_name FROM oltp.payment_methods ORDER BY method_id;
SELECT segment, COUNT(*) AS clientes FROM oltp.customers GROUP BY segment ORDER BY segment;
SELECT category, COUNT(*) AS comercios FROM oltp.merchants GROUP BY category ORDER BY category;
