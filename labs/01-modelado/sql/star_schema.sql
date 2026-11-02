-- ============================================================
-- PayTrack Star Schema (OLAP)
-- Esquema estrella para análisis de transacciones
--
-- Las dimensiones de calendario (dim_date, dim_time) están dadas.
-- Tú diseñas el resto: dimensiones de negocio, tabla de hechos e índices.
--
-- Contrato mínimo (lo usan load_star.py y check_queries.py):
--   * Cada dimensión tiene una surrogate key llamada <dimensión>_key
--     (customer_key, merchant_key, payment_method_key) y conserva su
--     natural key del OLTP (customer_id, merchant_id, method_name).
--   * dim_payment_method tiene la columna method_type.
--   * dim_customer es SCD Tipo 2 con valid_from, valid_to e is_current.
--   * fact_transactions tiene transaction_id, las 5 foreign keys
--     (date_key, time_key, customer_key, merchant_key, payment_method_key)
--     y las medidas amount_cop, is_fraud, is_approved y fraud_risk_score.
-- Todo lo demás (tipos de dato, atributos descriptivos, restricciones,
-- índices) es decisión de diseño tuya y debes justificarlo en el informe.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ── Dimensiones de calendario (dadas) ──────────────────────

-- Dim: Fecha (granularidad: día)
CREATE TABLE dw.dim_date (
    date_key        INT          PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE         NOT NULL,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(20)  NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,     -- 0 = domingo … 6 = sábado
    day_name        VARCHAR(20)  NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_holiday_co   BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Dim: Hora (granularidad: minuto del día)
CREATE TABLE dw.dim_time (
    time_key        INT          PRIMARY KEY,  -- HHMM
    hour            SMALLINT     NOT NULL,
    minute          SMALLINT     NOT NULL,
    period_of_day   VARCHAR(20)  NOT NULL      -- morning, afternoon, evening, night
);

-- ── TODO 1: Dimensión cliente (SCD Tipo 2) ─────────────────
-- ¿Qué atributos del cliente sirven para analizar el negocio?
-- ¿Cuáles NO deberían estar en el data warehouse (piensa en datos personales)?
-- ¿Por qué la PK no puede ser customer_id si la dimensión es SCD2?
--
-- Solo atributos analíticos. name, email y monthly_income_cop NO llegan al DW:
-- son datos personales / financieros que ninguna pregunta de BI necesita
-- (minimización de datos, Ley 1581 de 2012).
-- city y segment cambian en el tiempo: SCD2 guarda una fila por versión para
-- que cada transacción se atribuya a la ciudad/segmento que el cliente tenía
-- cuando pagó. Por eso customer_id se repite y la PK es la surrogate key.
CREATE TABLE dw.dim_customer (
    customer_key    INT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     VARCHAR(10)  NOT NULL,     -- natural key del OLTP
    city            VARCHAR(100),              -- nullable como en el origen
    segment         VARCHAR(50),               -- mass, premium, high_value
    signup_date     DATE         NOT NULL,     -- fecha de alta (antigüedad, cohortes)
    valid_from      TIMESTAMP    NOT NULL,     -- inicio de vigencia (incluido)
    valid_to        TIMESTAMP    NOT NULL DEFAULT '9999-12-31',  -- fin (excluido)
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    CHECK (valid_from < valid_to),
    UNIQUE (customer_id, valid_from)
);

-- A lo sumo una versión vigente por cliente
CREATE UNIQUE INDEX uq_dim_customer_current
    ON dw.dim_customer (customer_id) WHERE is_current;

-- ── TODO 2: Dimensión comercio ─────────────────────────────
-- SCD Tipo 1: los atributos de un comercio cambian muy poco y su historia no
-- se necesita para las preguntas de BI; si cambian se sobrescriben.
CREATE TABLE dw.dim_merchant (
    merchant_key    INT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id     VARCHAR(10)  NOT NULL UNIQUE,  -- natural key del OLTP
    merchant_name   VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    city            VARCHAR(100)
);

-- ── TODO 3: Dimensión método de pago ───────────────────────
-- method_type no existe en el OLTP: es un enriquecimiento para agrupar
-- métodos. NOT NULL + CHECK hacen fallar la carga si llega un método nuevo
-- sin clasificar, en vez de dejarlo nulo en silencio.
CREATE TABLE dw.dim_payment_method (
    payment_method_key INT         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    method_name        VARCHAR(50) NOT NULL UNIQUE,  -- natural key del OLTP
    method_type        VARCHAR(20) NOT NULL
        CHECK (method_type IN ('card', 'digital_wallet', 'bank_transfer'))
);

-- ── TODO 4: Tabla de hechos ────────────────────────────────
-- Grano: una fila por transacción.
-- Cada foreign key debe referenciar la PK de su dimensión.
--
-- transaction_id es una dimensión degenerada; como PK garantiza el grano.
-- Medidas: amount_cop (aditiva), is_fraud e is_approved (aditivas como
-- conteos), fraud_risk_score (no aditiva: se promedia; NULL sin evento).
CREATE TABLE dw.fact_transactions (
    transaction_id      VARCHAR(20)   PRIMARY KEY,
    date_key            INT           NOT NULL REFERENCES dw.dim_date (date_key),
    time_key            INT           NOT NULL REFERENCES dw.dim_time (time_key),
    customer_key        INT           NOT NULL REFERENCES dw.dim_customer (customer_key),
    merchant_key        INT           NOT NULL REFERENCES dw.dim_merchant (merchant_key),
    payment_method_key  INT           NOT NULL REFERENCES dw.dim_payment_method (payment_method_key),
    amount_cop          NUMERIC(15,2) NOT NULL,
    is_fraud            BOOLEAN       NOT NULL,
    is_approved         BOOLEAN       NOT NULL,
    fraud_risk_score    NUMERIC(5,4)
);

-- ── TODO 5: Índices para las consultas analíticas ──────────
-- Revisa los filtros y JOIN de olap_queries.sql. ¿Qué columnas de la
-- tabla de hechos conviene indexar? Mide el efecto con EXPLAIN ANALYZE.
--
-- Decisión (medida con EXPLAIN ANALYZE, ver informe): un solo índice cubriente.
-- * Q1 y Q3 filtran por una ventana de fechas (vía dim_date) y solo leen
--   merchant_key / time_key, amount_cop e is_approved: con INCLUDE se
--   resuelven con Index Only Scan sin tocar la tabla.
-- * Un índice simple en date_key no se usa: la tabla no está ordenada por
--   fecha (correlación ≈ 0), así que leer filas por índice es acceso aleatorio
--   a casi todas las páginas y el optimizador prefiere el seq scan.
-- * Índices en customer_key, merchant_key, payment_method_key y time_key
--   tampoco se usan (Q2 lee toda la tabla y los joins son hash joins con
--   dimensiones pequeñas) y encarecen la carga; se omiten.
-- El Index Only Scan necesita el mapa de visibilidad: tras cada carga
-- conviene VACUUM ANALYZE dw.fact_transactions.
CREATE INDEX idx_fact_date_cover
    ON dw.fact_transactions (date_key)
    INCLUDE (merchant_key, time_key, amount_cop, is_approved);

