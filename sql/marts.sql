-- AdVantage Analytics Pipeline - Mart Queries
-- These run on Athena in production
-- Each query creates one mart (pre-aggregated view) for the dashboard

-- ══════════════════════════════════════════════════════
-- mart_channel_summary: One row per channel
-- Shows: total spend, revenue, ROAS, CAC, CVR
-- ══════════════════════════════════════════════════════
SELECT
    channel,
    COUNT(DISTINCT campaign_key)             AS campaign_count,
    SUM(spend_usd)                            AS total_spend_usd,
    SUM(revenue_usd)                          AS total_revenue_usd,
    SUM(impressions)                          AS total_impressions,
    SUM(clicks)                               AS total_clicks,
    SUM(orders)                               AS total_orders,
    SUM(new_customers)                        AS total_new_customers,
    ROUND(SUM(revenue_usd) / NULLIF(SUM(spend_usd), 0), 2)         AS roas,
    ROUND(SUM(spend_usd) / NULLIF(SUM(new_customers), 0), 2)      AS cac,
    ROUND(100.0 * SUM(orders) / NULLIF(SUM(clicks), 0), 2)        AS conv_rate_pct,
    ROUND(100.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 2)   AS ctr_pct
FROM fact_campaign_daily
GROUP BY channel;

-- ══════════════════════════════════════════════════════
-- mart_campaign_summary: One row per campaign
-- Shows: campaign-level KPIs for drill-down
-- ══════════════════════════════════════════════════════
SELECT
    c.campaign_key,
    c.campaign_name,
    c.channel,
    c.campaign_type,
    c.status,
    SUM(f.spend_usd)                          AS total_spend_usd,
    SUM(f.revenue_usd)                        AS total_revenue_usd,
    SUM(f.impressions)                        AS total_impressions,
    SUM(f.clicks)                             AS total_clicks,
    SUM(f.orders)                             AS total_orders,
    SUM(f.new_customers)                      AS total_new_customers,
    ROUND(SUM(f.revenue_usd) / NULLIF(SUM(f.spend_usd), 0), 2)       AS roas,
    ROUND(SUM(f.spend_usd) / NULLIF(SUM(f.new_customers), 0), 2)    AS cac,
    ROUND(100.0 * SUM(f.orders) / NULLIF(SUM(f.clicks), 0), 2)      AS conv_rate_pct
FROM fact_campaign_daily f
JOIN dim_campaign c ON f.campaign_key = c.campaign_key
GROUP BY c.campaign_key, c.campaign_name, c.channel, c.campaign_type, c.status;

-- ══════════════════════════════════════════════════════
-- mart_monthly_summary: One row per channel per month
-- Shows: time-series KPIs for trend charts
-- ══════════════════════════════════════════════════════
SELECT
    d.year,
    d.month,
    d.month_name,
    f.channel,
    SUM(f.spend_usd)                          AS spend_usd,
    SUM(f.revenue_usd)                        AS revenue_usd,
    SUM(f.orders)                             AS total_orders,
    SUM(f.new_customers)                      AS new_customers,
    ROUND(SUM(f.revenue_usd) / NULLIF(SUM(f.spend_usd), 0), 2)     AS roas,
    ROUND(SUM(f.spend_usd) / NULLIF(SUM(f.new_customers), 0), 2)  AS cac
FROM fact_campaign_daily f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, f.channel
ORDER BY d.year, d.month, f.channel;
