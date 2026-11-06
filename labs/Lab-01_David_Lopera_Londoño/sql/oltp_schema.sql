-- ============================================================
-- PayTrack OLTP Schema
-- Esquema transaccional normalizado (3FN)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS oltp;

-- Clientes
CREATE TABLE oltp.customers (
    customer_id     VARCHAR(10)  PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    email           VARCHAR(200) UNIQUE NOT NULL,
    city            VARCHAR(100),
    segment         VARCHAR(50),  -- mass, premium, high_value
    monthly_income_cop NUMERIC(15,2),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Comercios
CREATE TABLE oltp.merchants (
    merchant_id     VARCHAR(10)  PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    city            VARCHAR(100)
);

-- Métodos de pago (catálogo)
CREATE TABLE oltp.payment_methods (
    method_id       SERIAL       PRIMARY KEY,
    method_name     VARCHAR(50)  UNIQUE NOT NULL  -- credit_card, debit_card, nequi...
);

-- Transacciones
CREATE TABLE oltp.transactions (
    transaction_id  VARCHAR(20)  PRIMARY KEY,
    customer_id     VARCHAR(10)  NOT NULL REFERENCES oltp.customers(customer_id),
    merchant_id     VARCHAR(10)  NOT NULL REFERENCES oltp.merchants(merchant_id),
    method_id       INT          NOT NULL REFERENCES oltp.payment_methods(method_id),
    amount_cop      NUMERIC(15,2) NOT NULL,
    status          VARCHAR(20)  NOT NULL,  -- approved, declined
    is_fraud        BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP    NOT NULL
);

-- Eventos de fraude
CREATE TABLE oltp.fraud_events (
    event_id        VARCHAR(15)  PRIMARY KEY,
    transaction_id  VARCHAR(20)  NOT NULL REFERENCES oltp.transactions(transaction_id),
    pattern         VARCHAR(50),
    risk_score      NUMERIC(5,4),
    detected_at     TIMESTAMP    NOT NULL
);

-- Índices básicos
CREATE INDEX idx_txn_customer   ON oltp.transactions(customer_id);
CREATE INDEX idx_txn_merchant   ON oltp.transactions(merchant_id);
CREATE INDEX idx_txn_created_at ON oltp.transactions(created_at);
