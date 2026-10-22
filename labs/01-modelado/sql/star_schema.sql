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
--    city
--    segment (mass, premium, high_value)
--    income_band
--
-- ¿Cuáles NO deberían estar en el data warehouse (piensa en datos personales)?
--    name, email    
--
-- ¿Por qué la PK no puede ser customer_id si la dimensión es SCD2?
--
-- Porque es necesario y vital, soportar en el DW, el historial de datos (SCD). Con llaves subrogadas
-- que identifican al cliente de manera unica en un período del tiempo en donde la historia del mismo no cambia,
-- se preserva la historia antes y después de un cambio en la información del cliente como la ciudad o el segmento
-- Despues del cambio se crea un nuevo indicador unico para el mismo cliente transaccional

CREATE SEQUENCE dw.seq_customer_key       START 1;

CREATE TABLE dw.dim_customer (
    customer_key  INT         NOT NULL DEFAULT nextval('dw.seq_customer_key'),
    customer_id   VARCHAR(10) NOT NULL,
    city          VARCHAR(100),
    segment       VARCHAR(50), -- mass, premium, high_value
    income_band   NUMERIC(15,2),
    valid_from    DATE        NOT NULL,
    valid_to      DATE        NOT NULL DEFAULT DATE '9999-12-31',
    is_current    BOOLEAN     NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_customer       PRIMARY KEY (customer_key),
    CONSTRAINT uq_dim_customer_ver   UNIQUE (customer_id, valid_from),
    CONSTRAINT ck_dim_customer_range CHECK (valid_to >= valid_from)
);

-- ── TODO 2: Dimensión comercio ─────────────────────────────

CREATE SEQUENCE dw.seq_merchant_key       START 1;

CREATE TABLE dw.dim_merchant (
    merchant_key  INT          NOT NULL DEFAULT nextval('dw.seq_merchant_key'),
    merchant_id   VARCHAR(10)  NOT NULL,
    name          VARCHAR(200),
    category      VARCHAR(100),
    city          VARCHAR(100),
    CONSTRAINT pk_dim_merchant    PRIMARY KEY (merchant_key),
    CONSTRAINT uq_dim_merchant_id UNIQUE (merchant_id)
);

-- ── TODO 3: Dimensión método de pago ───────────────────────

CREATE SEQUENCE dw.seq_payment_method_key START 1;

CREATE TABLE dw.dim_payment_method (
    payment_method_key INT          NOT NULL DEFAULT nextval('dw.seq_payment_method_key'),
    method_name        VARCHAR(50)  NOT NULL, -- credit_card, debit_card, nequi
    method_type        VARCHAR(50),
    CONSTRAINT pk_dim_payment_method   PRIMARY KEY (payment_method_key),
    CONSTRAINT uq_dim_payment_method_n UNIQUE (method_name)
);

-- ── TODO 4: Tabla de hechos ────────────────────────────────
-- Grano: una fila por transacción.
-- Cada foreign key debe referenciar la PK de su dimensión.

CREATE SEQUENCE dw.seq_transaction_id       START 1;

CREATE TABLE dw.fact_transactions (
    transaction_id     INT           NOT NULL,
    customer_key       INT           NOT NULL,
    merchant_key       INT           NOT NULL,
    payment_method_key INT           NOT NULL,
    date_key           INT           NOT NULL,
    time_key           INT           NOT NULL,
    amount_cop         DECIMAL(18,2) NOT NULL,
    is_approved        BOOLEAN       NOT NULL,
    is_fraud           BOOLEAN       NOT NULL,
    fraud_risk_score   DECIMAL(5,2),
    CONSTRAINT pk_fact_transactions PRIMARY KEY (transaction_id),
    CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_key)       REFERENCES dw.dim_customer (customer_key),
    CONSTRAINT fk_fact_merchant
        FOREIGN KEY (merchant_key)       REFERENCES dw.dim_merchant (merchant_key),
    CONSTRAINT fk_fact_payment_method
        FOREIGN KEY (payment_method_key) REFERENCES dw.dim_payment_method (payment_method_key),
    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)           REFERENCES dw.dim_date (date_key),
    CONSTRAINT fk_fact_time
        FOREIGN KEY (time_key)           REFERENCES dw.dim_time (time_key)
);

-- ── TODO 5: Índices para las consultas analíticas ──────────
-- Revisa los filtros y JOIN de olap_queries.sql. ¿Qué columnas de la
-- tabla de hechos conviene indexar? Mide el efecto con EXPLAIN ANALYZE.

