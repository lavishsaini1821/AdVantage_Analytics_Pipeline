-- AdVantage Analytics Pipeline - Star Schema DDL
-- This SQL runs on: SQLite (local) | Athena (cloud)
-- Same schema works on both with minimal changes

-- ══════════════════════════════════════════════════════
-- dim_date: One row per calendar day
-- Purpose: Filter by date, group by month/quarter
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INT  PRIMARY KEY,           -- YYYYMMDD (e.g. 20260101)
    date        DATE NOT NULL,              -- Full date
    year        INT  NOT NULL,
    quarter     INT  NOT NULL,              -- 1-4
    month       INT  NOT NULL,              -- 1-12
    month_name  TEXT NOT NULL,
    day         INT  NOT NULL,              -- Day of month
    day_of_week INT  NOT NULL,              -- 0=Mon, 6=Sun
    day_name    TEXT NOT NULL,
    is_weekend  INT  NOT NULL              -- 1=Yes, 0=No
);

-- ══════════════════════════════════════════════════════
-- dim_campaign: One row per campaign
-- Purpose: Break down metrics by channel, type, status
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key   INT  PRIMARY KEY,        -- Surrogate key (auto-generated)
    campaign_id    TEXT NOT NULL,           -- Business key from ad platform
    campaign_name  TEXT NOT NULL,
    channel        TEXT NOT NULL,            -- Meta, Google, TikTok, Other
    campaign_type  TEXT NOT NULL,           -- brand_awareness, performance, prospecting, retargeting
    status        TEXT NOT NULL,            -- active, paused, completed
    start_date    DATE NOT NULL,
    end_date      DATE,
    daily_budget  REAL
);

-- ══════════════════════════════════════════════════════
-- fact_campaign_daily: One row per campaign per day
-- Grain: campaign × date
-- This is the central fact table - all KPIs derive from here
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fact_campaign_daily (
    campaign_key    INT  NOT NULL,
    date_key       INT  NOT NULL,
    channel        TEXT,
    campaign_type  TEXT,
    spend_usd      REAL NOT NULL,          -- Money spent on ads
    impressions     INT  NOT NULL,          -- Times ad was shown
    clicks         INT  NOT NULL,          -- Times ad was clicked
    orders         INT  NOT NULL,          -- Conversions
    revenue_usd    REAL NOT NULL,          -- Revenue from orders
    new_customers  INT  NOT NULL,         -- First-time buyers
    roas           REAL,                   -- Derived: revenue / spend
    cac            REAL,                   -- Derived: spend / new_customers
    conv_rate_pct  REAL,                   -- Derived: orders / clicks * 100
    ctr_pct        REAL,                   -- Derived: clicks / impressions * 100
    PRIMARY KEY (campaign_key, date_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_campaign_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_channel ON fact_campaign_daily(channel);
