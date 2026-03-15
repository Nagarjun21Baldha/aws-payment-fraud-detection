-- =============================================================
-- Payment Fraud Detection — Redshift Schema
-- =============================================================
-- Run this script once to set up all tables in Redshift.
-- Tables mirror the S3 bronze/silver/fraud layer structure.
-- =============================================================


-- =============================================================
-- BRONZE TABLE — raw events exactly as received from Kinesis
-- No transformations, no filtering. Source of truth.
-- =============================================================
CREATE TABLE IF NOT EXISTS bronze.payment_events_raw (
    transaction_id      VARCHAR(100)    NOT NULL,
    user_id             VARCHAR(50)     NOT NULL,
    amount              DECIMAL(12, 2)  NOT NULL,
    currency            VARCHAR(10)     DEFAULT 'USD',
    merchant_name       VARCHAR(200),
    merchant_category   VARCHAR(100),
    user_country        VARCHAR(10),
    ip_country          VARCHAR(10),
    device              VARCHAR(50),
    timestamp           VARCHAR(50),
    is_fraud            BOOLEAN,
    fraud_type          VARCHAR(50),
    ingestion_layer     VARCHAR(20)     DEFAULT 'bronze',
    ingested_at         TIMESTAMP       DEFAULT SYSDATE
)
DISTSTYLE EVEN
SORTKEY (transaction_id);


-- =============================================================
-- SILVER TABLE — cleaned, validated, enriched events
-- This is the primary table for all analytics.
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.payment_events_clean (
    transaction_id      VARCHAR(100)    NOT NULL,
    user_id             VARCHAR(50)     NOT NULL,
    amount              DECIMAL(12, 2)  NOT NULL,
    currency            VARCHAR(10)     DEFAULT 'USD',
    merchant_name       VARCHAR(200),
    merchant_category   VARCHAR(100),
    user_country        VARCHAR(10),
    ip_country          VARCHAR(10),
    device              VARCHAR(50),
    event_timestamp     TIMESTAMP,
    event_date          DATE,
    event_hour          SMALLINT,
    country_mismatch    BOOLEAN         DEFAULT FALSE,
    amount_bucket       VARCHAR(20),
    is_fraud            BOOLEAN         DEFAULT FALSE,
    fraud_type          VARCHAR(50),
    ingestion_layer     VARCHAR(20)     DEFAULT 'silver',
    ingested_at         TIMESTAMP       DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (user_id)           -- distribute by user_id for fast user-level aggregations
SORTKEY (event_date, event_timestamp);   -- sort by date for fast time-range queries


-- =============================================================
-- FRAUD TABLE — flagged transactions only
-- Optimized for fraud investigation and reporting.
-- =============================================================
CREATE TABLE IF NOT EXISTS fraud.flagged_transactions (
    transaction_id      VARCHAR(100)    NOT NULL,
    user_id             VARCHAR(50)     NOT NULL,
    amount              DECIMAL(12, 2)  NOT NULL,
    merchant_name       VARCHAR(200),
    merchant_category   VARCHAR(100),
    user_country        VARCHAR(10),
    ip_country          VARCHAR(10),
    device              VARCHAR(50),
    event_timestamp     TIMESTAMP,
    event_date          DATE,
    country_mismatch    BOOLEAN,
    amount_bucket       VARCHAR(20),
    fraud_type          VARCHAR(50),
    -- fraud rules engine columns
    flag_velocity       BOOLEAN         DEFAULT FALSE,
    flag_spike          BOOLEAN         DEFAULT FALSE,
    flag_geography      BOOLEAN         DEFAULT FALSE,
    fraud_reason        VARCHAR(200),
    flagged_at          TIMESTAMP       DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (event_date, fraud_type);


-- =============================================================
-- USER PROFILES TABLE — rolling spend averages per user
-- Used by fraud rules engine to detect amount spikes.
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.user_profiles (
    user_id             VARCHAR(50)     NOT NULL,
    avg_transaction     DECIMAL(12, 2),
    total_transactions  INTEGER         DEFAULT 0,
    total_spend         DECIMAL(15, 2)  DEFAULT 0,
    first_seen          DATE,
    last_seen           DATE,
    home_country        VARCHAR(10),
    risk_score          DECIMAL(5, 2)   DEFAULT 0.0,
    updated_at          TIMESTAMP       DEFAULT SYSDATE,
    PRIMARY KEY (user_id)
)
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (user_id);


-- =============================================================
-- MERCHANT SUMMARY TABLE — aggregated merchant-level stats
-- Pre-aggregated for fast dashboard queries.
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.merchant_summary (
    merchant_name       VARCHAR(200)    NOT NULL,
    merchant_category   VARCHAR(100),
    total_transactions  INTEGER         DEFAULT 0,
    total_volume        DECIMAL(15, 2)  DEFAULT 0,
    fraud_count         INTEGER         DEFAULT 0,
    fraud_rate          DECIMAL(5, 2)   DEFAULT 0.0,
    avg_transaction     DECIMAL(12, 2)  DEFAULT 0,
    summary_date        DATE,
    updated_at          TIMESTAMP       DEFAULT SYSDATE
)
DISTSTYLE ALL                           -- small table, replicate to all nodes
SORTKEY (summary_date, merchant_name);