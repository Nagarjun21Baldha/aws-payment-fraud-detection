-- =============================================================
-- Load Data from S3 into Redshift
-- =============================================================
-- Replace YOUR_BUCKET, YOUR_REGION, YOUR_IAM_ROLE
-- with your actual AWS values before running.
--
-- Run after schema.sql has been executed.
-- =============================================================


-- =============================================================
-- LOAD BRONZE — raw events from S3 bronze layer
-- =============================================================
COPY bronze.payment_events_raw
FROM 's3://YOUR_BUCKET/bronze/payments/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_IAM_ROLE'
FORMAT AS PARQUET
REGION 'YOUR_REGION';


-- =============================================================
-- LOAD SILVER — cleaned events from S3 silver layer
-- =============================================================
COPY silver.payment_events_clean
FROM 's3://YOUR_BUCKET/silver/payments/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_IAM_ROLE'
FORMAT AS PARQUET
REGION 'YOUR_REGION';


-- =============================================================
-- LOAD FRAUD — flagged transactions from S3 fraud layer
-- =============================================================
COPY fraud.flagged_transactions
FROM 's3://YOUR_BUCKET/silver/fraud/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_IAM_ROLE'
FORMAT AS PARQUET
REGION 'YOUR_REGION';


-- =============================================================
-- POPULATE USER PROFILES — compute rolling averages
-- Run after silver table is loaded.
-- =============================================================
INSERT INTO silver.user_profiles (
    user_id,
    avg_transaction,
    total_transactions,
    total_spend,
    first_seen,
    last_seen,
    home_country,
    updated_at
)
SELECT
    user_id,
    AVG(amount)             AS avg_transaction,
    COUNT(*)                AS total_transactions,
    SUM(amount)             AS total_spend,
    MIN(event_date)         AS first_seen,
    MAX(event_date)         AS last_seen,
    MAX(user_country)       AS home_country,
    SYSDATE                 AS updated_at
FROM silver.payment_events_clean
GROUP BY user_id
ON CONFLICT (user_id)
DO UPDATE SET
    avg_transaction    = EXCLUDED.avg_transaction,
    total_transactions = EXCLUDED.total_transactions,
    total_spend        = EXCLUDED.total_spend,
    last_seen          = EXCLUDED.last_seen,
    updated_at         = EXCLUDED.updated_at;


-- =============================================================
-- POPULATE MERCHANT SUMMARY — daily aggregation
-- =============================================================
INSERT INTO silver.merchant_summary (
    merchant_name,
    merchant_category,
    total_transactions,
    total_volume,
    fraud_count,
    fraud_rate,
    avg_transaction,
    summary_date,
    updated_at
)
SELECT
    merchant_name,
    merchant_category,
    COUNT(*)                                    AS total_transactions,
    SUM(amount)                                 AS total_volume,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)  AS fraud_count,
    ROUND(
        SUM(CASE WHEN is_fraud THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2
    )                                           AS fraud_rate,
    AVG(amount)                                 AS avg_transaction,
    CURRENT_DATE                                AS summary_date,
    SYSDATE                                     AS updated_at
FROM silver.payment_events_clean
GROUP BY merchant_name, merchant_category;