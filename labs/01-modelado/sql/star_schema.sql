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
--   * dim_customer es SCD Tipo 2 convalid_from, valid_to e is_current.
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
-- ¿Por qué la PRIMARY KEY no puede ser customer_id si la dimensión es SCD2?
CREATE TABLE dw.dim_customer (
    customer_key    SERIAL       PRIMARY KEY,
    customer_id     VARCHAR(10)  NOT NULL,          -- natural key (no única: SCD2)
    city            VARCHAR(100),
    segment         VARCHAR(50),
    income_band     VARCHAR(20),
    valid_from      TIMESTAMP    NOT NULL,
    valid_to        TIMESTAMP    NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_dim_customer_nk ON dw.dim_customer (customer_id, valid_from, valid_to);

CREATE TABLE dw.dim_merchant (
    merchant_key    SERIAL       PRIMARY KEY,
    merchant_id     VARCHAR(10)  NOT NULL UNIQUE,
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    city            VARCHAR(100)
);

CREATE TABLE dw.dim_payment_method (
    payment_method_key SERIAL    PRIMARY KEY,
    method_name     VARCHAR(50)  NOT NULL UNIQUE,
    method_type     VARCHAR(50)  NOT NULL
);

CREATE TABLE dw.fact_transactions (
    transaction_id      VARCHAR(20)   PRIMARY KEY,
    date_key            INT           NOT NULL REFERENCES dw.dim_date(date_key),
    time_key            INT           NOT NULL REFERENCES dw.dim_time(time_key),
    customer_key        INT           NOT NULL REFERENCES dw.dim_customer(customer_key),
    merchant_key        INT           NOT NULL REFERENCES dw.dim_merchant(merchant_key),
    payment_method_key  INT           NOT NULL REFERENCES dw.dim_payment_method(payment_method_key),
    amount_cop          NUMERIC(15,2) NOT NULL,
    is_approved         BOOLEAN       NOT NULL,
    is_fraud            BOOLEAN       NOT NULL DEFAULT FALSE,
    fraud_risk_score    NUMERIC(5,4)                     
);

CREATE INDEX idx_fact_date     ON dw.fact_transactions (date_key);
CREATE INDEX idx_fact_time     ON dw.fact_transactions (time_key);
CREATE INDEX idx_fact_customer ON dw.fact_transactions (customer_key);
CREATE INDEX idx_fact_merchant ON dw.fact_transactions (merchant_key);
CREATE INDEX idx_fact_method   ON dw.fact_transactions (payment_method_key);
CREATE INDEX idx_fact_fraud    ON dw.fact_transactions (date_key, merchant_key) WHERE is_fraud;