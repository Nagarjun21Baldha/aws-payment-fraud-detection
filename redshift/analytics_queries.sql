-- =============================================================
-- Payment Fraud Detection — Analytics Queries
-- =============================================================
-- These queries power fraud dashboards and operational reports.
-- All queries run against the silver and fraud schemas.
-- =============================================================


-- =============================================================
-- QUERY 1: Daily fraud summary
-- How many transactions were flagged today vs yesterday?
-- Used by: Operations leadership dashboard
-- =============================================================
SELECT
    event_date,
    COUNT(*)                                            AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)          AS fraud_count,
    ROUND(
        SUM(CASE WHEN is_fraud THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                                   AS fraud_rate_pct,
    SUM(amount)                                         AS total_volume,
    SUM(CASE WHEN is_fraud THEN amount ELSE 0 END)      AS fraud_volume,
    AVG(amount)                                         AS avg_transaction
FROM silver.payment_events_clean
WHERE event_date >= CURRENT_DATE - 30           -- last 30 days
GROUP BY event_date
ORDER BY event_date DESC;


-- =============================================================
-- QUERY 2: Fraud by type and merchant category
-- Which merchant categories have the highest fraud rates?
-- Used by: Fraud analytics team
-- =============================================================
SELECT
    merchant_category,
    fraud_type,
    COUNT(*)                                AS fraud_count,
    SUM(amount)                             AS fraud_volume,
    ROUND(AVG(amount), 2)                   AS avg_fraud_amount,
    MIN(amount)                             AS min_amount,
    MAX(amount)                             AS max_amount
FROM fraud.flagged_transactions
WHERE event_date >= CURRENT_DATE - 7        -- last 7 days
GROUP BY merchant_category, fraud_type
ORDER BY fraud_count DESC;


-- =============================================================
-- QUERY 3: High risk users
-- Users with the most fraud flags — potential account takeover
-- Used by: Trust & safety operations team
-- =============================================================
SELECT
    f.user_id,
    COUNT(*)                                AS fraud_flag_count,
    SUM(f.amount)                           AS total_flagged_amount,
    COUNT(DISTINCT f.fraud_type)            AS distinct_fraud_types,
    MIN(f.event_timestamp)                  AS first_flagged,
    MAX(f.event_timestamp)                  AS last_flagged,
    DATEDIFF(day,
        MIN(f.event_timestamp),
        MAX(f.event_timestamp)
    )                                       AS days_active,
    p.avg_transaction                       AS normal_avg_spend,
    p.home_country
FROM fraud.flagged_transactions f
LEFT JOIN silver.user_profiles p
    ON f.user_id = p.user_id
GROUP BY f.user_id, p.avg_transaction, p.home_country
HAVING COUNT(*) >= 2                        -- flagged more than once
ORDER BY fraud_flag_count DESC
LIMIT 20;


-- =============================================================
-- QUERY 4: Hourly transaction volume and fraud rate
-- When during the day does fraud peak?
-- Used by: Operations scheduling and alerting
-- =============================================================
SELECT
    event_hour,
    COUNT(*)                                            AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)          AS fraud_count,
    ROUND(
        SUM(CASE WHEN is_fraud THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                                   AS fraud_rate_pct,
    SUM(amount)                                         AS total_volume,
    ROUND(AVG(amount), 2)                               AS avg_amount
FROM silver.payment_events_clean
WHERE event_date = CURRENT_DATE - 1         -- yesterday's data
GROUP BY event_hour
ORDER BY event_hour;


-- =============================================================
-- QUERY 5: Country mismatch analysis
-- Which IP countries are associated with most fraud?
-- Used by: Fraud rules tuning
-- =============================================================
SELECT
    user_country,
    ip_country,
    COUNT(*)                                AS transaction_count,
    SUM(amount)                             AS total_volume,
    ROUND(AVG(amount), 2)                   AS avg_amount,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY user_country)
    , 2)                                    AS pct_of_user_country_txns
FROM silver.payment_events_clean
WHERE country_mismatch = TRUE
GROUP BY user_country, ip_country
ORDER BY transaction_count DESC
LIMIT 20;


-- =============================================================
-- QUERY 6: Real-time fraud alerts — last 15 minutes
-- Used by: Live monitoring dashboard and SNS alerting
-- =============================================================
SELECT
    transaction_id,
    user_id,
    amount,
    merchant_name,
    user_country,
    ip_country,
    fraud_reason,
    flagged_at,
    DATEDIFF(minute, flagged_at, SYSDATE)   AS minutes_ago
FROM fraud.flagged_transactions
WHERE flagged_at >= SYSDATE - INTERVAL '15 minutes'
ORDER BY flagged_at DESC;


-- =============================================================
-- QUERY 7: Week over week fraud trend
-- Is fraud increasing or decreasing?
-- Used by: Weekly executive report
-- =============================================================
SELECT
    DATE_TRUNC('week', event_date)          AS week_start,
    COUNT(*)                                AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(
        SUM(CASE WHEN is_fraud THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                       AS fraud_rate_pct,
    SUM(amount)                             AS total_volume,
    LAG(
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
    ) OVER (ORDER BY DATE_TRUNC('week', event_date)) AS prev_week_fraud,
    ROUND(
        (SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) -
         LAG(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END))
         OVER (ORDER BY DATE_TRUNC('week', event_date)))
        * 100.0 /
        NULLIF(LAG(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END))
        OVER (ORDER BY DATE_TRUNC('week', event_date)), 0)
    , 2)                                    AS wow_change_pct
FROM silver.payment_events_clean
GROUP BY DATE_TRUNC('week', event_date)
ORDER BY week_start DESC;