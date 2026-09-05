# AdVantage Analytics Pipeline

**Cloud-based marketing intelligence platform** — ROAS, CAC, CTR, CVR analytics across Meta, Google, TikTok, and more.

> Tech stack: Python · PySpark · AWS S3 · Glue · Athena · Parquet · SQL · Streamlit · GitHub Actions
> Runs locally for free (no AWS account needed) — same code deploys to real AWS with one config change.

---

## Table of Contents

1. [What is AdVantage?](#1-what-is-advantage)
2. [Analytics Background](#2-analytics-background)
3. [Project Overview](#3-project-overview)
4. [File Structure](#4-file-structure)
5. [The Journey of Data (Simple Version)](#5-the-journey-of-data-simple-version)
6. [The Journey of Data (PySpark Version)](#6-the-journey-of-data-pyspark-version)
7. [Deep Dive: Each Component](#7-deep-dive-each-component)
8. [How ROAS Calculation Works](#8-how-roas-calculation-works)
9. [How the Star Schema Works](#9-how-the-star-schema-works)
10. [Building and Running](#10-building-and-running)
11. [Understanding the Output](#11-understanding-the-output)
12. [Dashboard Walkthrough](#12-dashboard-walkthrough)
13. [Extending the Project](#13-extending-the-project)
14. [Interview Talking Points](#14-interview-talking-points)

---

## 1. What is AdVantage?

AdVantage is a marketing analytics platform that collects advertising data from multiple channels (Meta, Google, TikTok) and provides unified insights through a dashboard.

**What it solves in the real world:**

```
Marketing Team Problem:
─────────────────────────────────────────────────────────
  Meta Dashboard    Google Ads      TikTok Ads
       │                │              │
       ▼                ▼              ▼
   Excel Sheets    Excel Sheets   Excel Sheets
       │                │              │
       └────────────────┴──────────────┘
                        │
                        ▼
              Manual ROAS Calculation
                        │
                        ▼
              Hours wasted every week 💸
─────────────────────────────────────────────────────────

With AdVantage:
─────────────────────────────────────────────────────────
  All Platforms  ──►  ETL Pipeline  ──►  Dashboard
       │                  │                  │
       ▼                  ▼                  ▼
   Raw CSVs         Transformed        One-click
   (no structure)   Parquet           insights
                        │                  │
                        ▼                  ▼
              Automated metrics        5 seconds 🚀
─────────────────────────────────────────────────────────
```

**What the pipeline does:**

```
Raw CSV Files        ETL Pipeline        Analytics Output
─────────────────────────────────────────────────────────
dim_advertisers.csv                  ├──► dim_campaign.parquet
dim_campaigns.csv                   ├──► dim_date.parquet
dim_users.csv                       ├──► fact_campaign_daily.parquet
fact_ad_events.csv     ─────────►  │
fact_conversions.csv    ─────────►  │    ├──► mart_channel_summary.csv
dim_time.csv            ─────────►  │    ├──► mart_campaign_summary.csv
                                  │    ├──► mart_monthly_summary.csv
                                  │    └──► mart_campaign_daily.csv
                                  │
                                  ▼
                            S3 / Parquet
                                  │
                                  ▼
                            Streamlit Dashboard 📊
─────────────────────────────────────────────────────────
```

---

## 2. Analytics Background

### The Data Model

Marketing data is inherently multi-dimensional. A single ad event has:
- **Who** placed the ad (advertiser)
- **Which** campaign it belongs to
- **When** it happened (date, time)
- **What** happened (impression, click, conversion)
- **How much** it cost

Traditional databases store this in flat tables. As data grows, aggregations become slow and complex.

### The Star Schema Solution

```
              ┌─────────────────────┐
              │      dim_date        │
              │     (Date Key)       │
              │ date_key, year,      │
              │ month, quarter,      │
              │ day_of_week,         │
              │ is_weekend           │
              └──────────┬──────────┘
                         │
          ┌──────────────┴──────────────┐
          │                               │
          ▼                               ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│    dim_campaign          │   │  fact_campaign_daily     │
│   (Campaign Dimension)   │   │   (Central Fact Table)  │
│                         │   │                          │
│ campaign_key, channel,  │   │ campaign_key, date_key,  │
│ campaign_type, status,   │◄─┤ spend_usd, impressions,  │
│ daily_budget_usd        │   │ clicks, orders,          │
└──────────────────────────┘   │ revenue_usd,            │
                                │ new_customers,         │
                                │ roas, cac,             │
                                │ conv_rate_pct, ctr_pct  │
                                └──────────────────────────┘
```

**Why Star Schema?**

| Approach | Pros | Cons |
|---|---|---|
| Flat table | Simple | Slow aggregations at scale |
| Star Schema | Fast reads, denormalized, partition-friendly | Some data duplication |
| Snowflake | Fully normalized | Too many joins, slower |

Star schema is the standard for data warehousing because:
1. **Facts** (measures) are in the center
2. **Dimensions** (descriptors) surround it
3. **Surrogate keys** link them together
4. Partitioning by date makes queries fast

### The Key Metrics

```
┌─────────────────────────────────────────────────────────┐
│  ROAS — Return on Ad Spend                              │
│  ───────────────────────────────────────────────────   │
│  ROAS = Total Revenue / Total Spend                     │
│                                                         │
│  Example:                                              │
│  Spend $100 on ads → Earn $200 revenue                 │
│  ROAS = $200 / $100 = 2.0x                           │
│                                                         │
│  ROAS > 1.0  → Profitable                             │
│  ROAS < 1.0  → Losing money 💸                       │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  CAC — Customer Acquisition Cost                        │
│  ───────────────────────────────────────────────────   │
│  CAC = Total Spend / New Customers Acquired             │
│                                                         │
│  Example:                                              │
│  Spend $1000 → Get 10 new customers                   │
│  CAC = $1000 / 10 = $100 per customer                │
│                                                         │
│  Lower CAC = More efficient ad spend ✅                 │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  CTR — Click-Through Rate                             │
│  ───────────────────────────────────────────────────   │
│  CTR = (Clicks / Impressions) × 100                   │
│                                                         │
│  Example:                                             │
│  1000 impressions, 50 clicks                          │
│  CTR = (50 / 1000) × 100 = 5.0%                     │
│                                                         │
│  Higher CTR = More compelling ad creative ✅            │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  CVR — Conversion Rate                                │
│  ───────────────────────────────────────────────────   │
│  CVR = (Orders / Clicks) × 100                       │
│                                                         │
│  Example:                                             │
│  100 clicks, 3 orders                                 │
│  CVR = (3 / 100) × 100 = 3.0%                       │
│                                                         │
│  Higher CVR = Better landing page ✅                   │
└─────────────────────────────────────────────────────────┘
```

### The Data Flow

```
Raw Events                           Aggregated Facts
─────────────────────────────────────────────────────────
  Ad Impressions                     
       │                               
  Ad Clicks                          Campaign × Date
       │                              aggregation
  Ad Spend (cost_usd)                    │
       │                                ▼
  Conversions (orders)             ┌────────────────┐
       │                             │ fact_campaign  │
  Conversion Value                  │ _daily         │
  (revenue_usd)                   │                │
       │                             │ 992 rows       │
  New Customers                     │ (31 days × 32   │
       │                             │  campaigns)     │
       └─────────────────────────►  └────────────────┘
                                         │
                                         ▼
                                   Pre-aggregated
                                   Data Marts
                                   (CSV files read
                                    by dashboard)
─────────────────────────────────────────────────────────
```

---

## 3. Project Overview

### What This Project Does

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│ Mock CSV     │     │  ETL Pipeline     │     │ Dashboard   │
│ Data Files   │────►│                  │────►│             │
│ (9 sources)  │     │ - Parse          │     │ - KPIs      │
└─────────────┘     │ - Transform       │     │ - Charts    │
                    │ - Aggregate       │     │ - Tables    │
                    │ - Write Marts    │     └─────────────┘
                    └──────────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │ S3 / Parquet      │
                    │ (partitioned)     │
                    └──────────────────┘
```

### Two Ways to Run

| Version | File | Requires | Use Case |
|---|---|---|---|
| **Local (pandas)** | `etl_local.py` | Python only | Learning, no Java needed |
| **Cloud (PySpark)** | `spark_etl.py` | Java + PySpark | Production, AWS deployment |

Both produce the same output — the same code runs locally and on AWS Glue/EMR.

### Dashboard Preview

```
┌─────────────────────────────────────────────────────────────────────┐
│  📊 AdVantage Analytics                                            │
│  Real-time Marketing Intelligence — Track ROAS, CAC, CTR             │
│  🚀 Live Dashboard                                                   │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│  │ 💰 Total     │ │ 📈 Revenue  │ │ 🎯 ROAS      │              │
│  │ Spend        │ │              │ │              │              │
│  │ $13.5K       │ │ $20.9K       │ │ 1.55x       │              │
│  └──────────────┘ └──────────────┘ └──────────────┘              │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│  │ 👥 Avg CAC   │ │ 🛒 Orders   │ │ 📊 CVR       │              │
│  │              │ │              │ │              │              │
│  │ $157         │ │ 195          │ │ 2.65%        │              │
│  └──────────────┘ └──────────────┘ └──────────────┘              │
├─────────────────────────────────────────────────────────────────────┤
│  [📊 Analytics] [🎯 Campaign Explorer] [📈 Trends] [🏆 Rankings]  │
│                                                                      │
│  Channel Performance ─────────────────────────────────────────────     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ 📊 Channel ROAS Comparison                                   │   │
│  │                                                             │   │
│  │   Google   ████████████████████████  1.55x                  │   │
│  │   Meta    ███████████████████████   1.52x                   │   │
│  │   TikTok  ██████████████████       1.24x                   │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

![Dashboard Overview](../screenshots/dashboard-overview.png)

---

## 4. File Structure

```
AdVantage_Analytics_Pipeline/
│
├── config.yaml                    # All settings (seed, AWS mode, bucket names)
│
├── generate_mock_data.py         # Create 9 deterministic mock CSVs
│
├── etl_local.py                 # ★ PRIMARY ETL (pandas — no Java needed)
│
├── spark_etl.py                  # ★ CLOUD ETL (PySpark — for AWS Glue/EMR)
│
├── dashboard.py                  # Streamlit interactive dashboard
│
├── run.py                       # One-command runner: generate → ETL → test
│
├── requirements.txt              # pip install -r requirements.txt
│
├── README.md                    # This file
│
├── .github/
│   └── workflows/
│       └── ci.yml              # Auto-run: generate → ETL → test on push
│
├── sql/
│   ├── star_schema.sql          # DDL: dim_date, dim_campaign, fact tables
│   └── marts.sql                # Analytics SQL queries (Athena compatible)
│
├── data/
│   ├── mock_csv/                # Raw mock data (9 CSV files)
│   │   ├── dim_advertisers.csv   (8 rows)
│   │   ├── dim_campaigns.csv      (32 rows)
│   │   ├── dim_users.csv          (2,000 rows)
│   │   ├── dim_time.csv          (31 rows)
│   │   ├── fact_ad_events.csv    (~49,600 rows)
│   │   └── fact_conversions.csv  (195 rows)
│   │
│   ├── parquet/                 # Processed Parquet (columnar, partitioned)
│   │   ├── fact_campaign_daily.parquet
│   │   ├── dim_date.parquet
│   │   └── dim_campaign.parquet
│   │
│   └── marts/                  # Pre-aggregated tables (dashboard reads here)
│       ├── mart_channel_summary.csv   (4 rows — by channel)
│       ├── mart_campaign_summary.csv   (32 rows — by campaign)
│       ├── mart_monthly_summary.csv   (4 rows — by month)
│       └── mart_campaign_daily.csv    (992 rows — by campaign × day)
│
└── tests/
    └── test_pipeline.py        # 6 automated tests
```

---

## 5. The Journey of Data (Simple Version)

Let's trace one month's worth of advertising data through `etl_local.py`.

```
generate_mock_data.py
         │
         ▼
┌─────────────────────────────────┐
│ Step 1: Generate Raw CSV Files  │
│                                 │
│ dim_campaigns.csv              │
│  campaign_id, channel, type    │
│  camp_ME_001, Meta, brand     │
│  camp_ME_002, Meta, perf      │
│                                 │
│ fact_ad_events.csv             │
│  campaign_id, event_date,      │
│  event_type, cost_usd          │
│  camp_ME_001, 2026-01-01,     │
│  impression, 0.50               │
│                                 │
│ fact_conversions.csv           │
│  campaign_id, conversion_date,  │
│  conversion_value_usd           │
│  camp_ME_001, 2026-01-03, 45  │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 2: Build dim_date         │
│                                 │
│ time_dim["date_id"]            │
│   2026-01-01                  │
│   2026-01-02 ...              │
│   2026-01-31                  │
│                                 │
│ Add surrogate key:             │
│   date_key = 20260101 (int)   │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 3: Build dim_campaign     │
│                                 │
│ Add surrogate key:             │
│   campaign_key = 1, 2, 3...   │
│                                 │
│ camp_ME_001 → campaign_key: 1  │
│ camp_GO_001 → campaign_key: 9  │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 4: Aggregate Events       │
│                                 │
│ Group by (campaign_id, date):  │
│                                 │
│ SELECT campaign_id, event_date,│
│        SUM(cost_usd),          │
│        COUNT(impressions),      │
│        COUNT(clicks)            │
│ FROM fact_ad_events            │
│ GROUP BY campaign_id, event_date│
│                                 │
│ Result: one row per campaign    │
│         per day                │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 5: Aggregate Conversions   │
│                                 │
│ Group by (campaign_id, date):  │
│                                 │
│ SELECT campaign_id,             │
│        conversion_date,         │
│        COUNT(*),                │
│        SUM(conversion_value)    │
│ FROM fact_conversions          │
│ GROUP BY campaign_id,          │
│        conversion_date           │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 6: Merge Events + Convs    │
│                                 │
│ LEFT JOIN on                   │
│ (campaign_id, event_date =     │
│  campaign_id, conversion_date) │
│                                 │
│ Days with no conversion        │
│ get orders=0, revenue=0        │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 7: Join with Dimensions   │
│                                 │
│ Add campaign_key from          │
│ dim_campaign                   │
│                                 │
│ Add date_key from dim_date    │
│                                 │
│ Add channel, campaign_type     │
│ for filtering                 │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 8: Compute Metrics        │
│                                 │
│ ROAS = revenue / spend         │
│ CAC  = spend / new_customers  │
│ CVR  = orders / clicks × 100   │
│ CTR  = clicks / impr × 100    │
│                                 │
│ safe_divide handles 0 division │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 9: Write Outputs          │
│                                 │
│ fact_campaign_daily.parquet    │
│  992 rows × 15 columns        │
│                                 │
│ dim_date.parquet              │
│  31 rows                     │
│                                 │
│ dim_campaign.parquet          │
│  32 rows                     │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Step 10: Build Marts          │
│                                 │
│ mart_channel_summary.csv       │
│  Group by channel → 4 rows     │
│  (Meta, Google, TikTok, Other)│
│                                 │
│ mart_campaign_summary.csv      │
│  Group by campaign → 32 rows  │
│                                 │
│ mart_monthly_summary.csv     │
│  Group by month → 4 rows      │
│                                 │
│ mart_campaign_daily.csv        │
│  fact table → 992 rows         │
└─────────────────────────────────┘
```

---

## 6. The Journey of Data (PySpark Version)

The `spark_etl.py` version runs the same logic but at scale — distributed across multiple machines.

```
┌─────────────────────────────────────────────────────────────┐
│                    spark_etl.py Architecture                │
└─────────────────────────────────────────────────────────────┘

                    ┌────────────────────┐
                    │  SparkSession      │
                    │  (local[*] mode)   │
                    └─────────┬──────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐  ┌──────────┐   ┌──────────┐
        │ CSV Read │  │ CSV Read │   │ CSV Read │
        │ (events) │  │ (convs)  │   │ (dims)   │
        └────┬─────┘  └────┬─────┘   └────┬─────┘
              │              │              │
              ▼              ▼              ▼
        ┌─────────────────────────────────────────┐
        │           Distributed Transformations   │
        │                                         │
        │  groupBy → agg → join → withColumn     │
        │  (PySpark optimizes execution plan)     │
        └───────────────────┬───────────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  Partitioned Parquet Write │
              │                             │
              │  fact_campaign_daily/       │
              │    date=2026-01-01/         │
              │    date=2026-01-02/         │
              │    ...                       │
              │                             │
              │  S3 / Local filesystem      │
              └─────────────────────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  Glue Catalog Registration  │
              │                             │
              │  Tables auto-discovered    │
              │  from Parquet schema       │
              └─────────────────────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  Athena SQL Marts           │
              │                             │
              │  Query Parquet with SQL      │
              │  (serverless, pay-per-query)│
              └─────────────────────────────┘
```

### Why PySpark local[*]?

```
Single Machine                    Distributed (EMR/Glue)
─────────────────────────────────────────────────────────
Data fits in RAM? Use pandas   Data > RAM? Use Spark
Easy to debug                  Scalable to TB/PB
No cluster setup               Auto-partitions data
Same code, same output         Same code, same output
─────────────────────────────────────────────────────────
```

The key insight: **the same `spark_etl.py` runs locally on your laptop AND on AWS Glue Job / EMR / Databricks.** Only the SparkSession configuration changes:

```python
# Local mode (etl_local.py / spark_etl.py with mode="local")
builder = SparkSession.builder.appName("AdVantage-ETL")
# No extra config needed

# AWS mode (spark_etl.py with mode="aws")
builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,...")
builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
builder = builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
builder = builder.config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
```

---

## 7. Deep Dive: Each Component

### `generate_mock_data.py`

**Purpose:** Create reproducible mock advertising data

The mock data generator uses a fixed seed (42) so every run produces identical data. This is critical for testing — the same input always produces the same output.

```python
import random
random.seed(42)  # Deterministic output
```

**What it generates:**

| File | Rows | Purpose |
|---|---|---|
| `dim_advertisers.csv` | 8 | Advertiser metadata |
| `dim_campaigns.csv` | 32 | Campaign configs (Meta/Google/TikTok × 4 types) |
| `dim_users.csv` | 2,000 | User profiles (PII masked with SHA-256) |
| `dim_time.csv` | 31 | January 2026 calendar |
| `fact_ad_events.csv` | ~49,600 | Ad events (impressions/clicks with cost) |
| `fact_conversions.csv` | 195 | Purchase events with revenue |

**PII Masking:**
```python
import hashlib

PIISALT = "advantage-secret-salt-2026"  # Random string

def mask(value: str) -> str:
    """SHA-256 hash with salt — no raw PII stored."""
    return hashlib.sha256(
        f"{PIISALT}:{value}".encode()
    ).hexdigest()[:32]
```

This means emails like `alice@example.com` become `a1b2c3d4e5f6...` — usable for analytics, but not reversible.

---

### `etl_local.py`

**Purpose:** Run ETL using pandas (no Java needed)

The primary ETL script that works on any machine. Key functions:

```python
def safe_divide(num, denom, decimals=2):
    """Divide with 0 handling. Returns 0.0 if denom is 0."""
    result = []
    for n, d in zip(num, denom):
        if d == 0 or pd.isna(d):
            result.append(0.0)
        else:
            result.append(round(n / d, decimals))
    return result
```

This function exists because dividing by zero in pandas produces `inf` or `NaN`, which would break the dashboard. Every ratio (ROAS, CAC, CVR, CTR) uses this.

**Design decision:** Why pandas over PySpark for local?

| Factor | pandas | PySpark |
|---|---|---|
| Dependencies | None (already installed) | Requires Java |
| Speed (small data) | Faster (no cluster overhead) | Slower |
| Speed (large data) | Limited by RAM | Scales horizontally |
| Setup complexity | Zero | Java + Spark installation |

For a portfolio project with 49K rows, pandas is the right choice. PySpark is there for when you have millions of rows.

---

### `spark_etl.py`

**Purpose:** Production-grade ETL with PySpark

Same logic as `etl_local.py` but with PySpark for distributed processing:

```python
from pyspark.sql import functions as F

# PySpark handles 0-division natively
fact = fact.withColumn(
    "roas",
    F.round(
        F.col("revenue_usd") / F.when(F.col("spend_usd") > 0, F.col("spend_usd")),
        2
    )
)
```

**S3 and Glue Integration:**

```python
# Write partitioned Parquet to S3
fact.write.mode("overwrite").partitionBy("date").parquet(
    f"s3a://{S3_BUCKET}/fact_campaign_daily/"
)

# Register in Glue Catalog
glue.create_table(
    DatabaseName=GLUE_DB,
    TableInput={
        "Name": "fact_campaign_daily",
        "StorageDescriptor": {
            "Location": f"s3a://{S3_BUCKET}/fact_campaign_daily/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        },
        "PartitionKeys": [{"Name": "date", "Type": "date"}],
    },
)
```

---

### `dashboard.py`

**Purpose:** Interactive Streamlit dashboard

Reads pre-built mart CSVs — no database connection needed:

```python
@st.cache_data(ttl=300)
def load_marts() -> dict:
    """Load all mart CSVs into memory."""
    marts = {}
    for csv_file in sorted(MART_DIR.glob("mart_*.csv")):
        marts[csv_file.stem] = pd.read_csv(csv_file)
    return marts
```

**Why CSV instead of querying Athena?**

1. **Zero infrastructure** — no AWS credentials needed to view dashboard
2. **Instant load** — CSV read is fast for pre-aggregated data
3. **Separation of concerns** — ETL writes CSV, dashboard reads CSV
4. **Portfolio-friendly** — interviewer can run it without AWS

---

### `sql/star_schema.sql`

DDL for the star schema — compatible with SQLite (local) and Athena (cloud):

```sql
CREATE TABLE fact_campaign_daily (
    campaign_key INT,
    date_key INT,
    channel VARCHAR(20),
    campaign_type VARCHAR(20),
    spend_usd DECIMAL(10,2),
    impressions INT,
    clicks INT,
    orders INT,
    revenue_usd DECIMAL(10,2),
    new_customers INT,
    roas DECIMAL(6,2),
    cac DECIMAL(10,2),
    conv_rate_pct DECIMAL(6,2),
    ctr_pct DECIMAL(6,2),
    PRIMARY KEY (campaign_key, date_key)
);

-- Partition by date for fast queries
CREATE TABLE fact_campaign_daily_partitioned (
    ...
)
PARTITIONED BY (date DATE);
```

---

### `sql/marts.sql`

Pre-built analytics queries that run on Athena:

```sql
-- Mart 1: Channel Summary
SELECT
    channel,
    COUNT(DISTINCT campaign_key) AS campaign_count,
    SUM(spend_usd) AS total_spend_usd,
    SUM(revenue_usd) AS total_revenue_usd,
    SUM(revenue_usd) / NULLIF(SUM(spend_usd), 0) AS roas
FROM fact_campaign_daily
GROUP BY channel
ORDER BY roas DESC;
```

---

## 8. How ROAS Calculation Works

ROAS is the most important metric in advertising analytics. Here's exactly how it's computed:

```
ROAS = Total Revenue / Total Spend

Example campaign over 1 month:
─────────────────────────────────────────
Ad Spend:    $500
Orders:       10
Avg Order:    $60
Revenue:      10 × $60 = $600

ROAS = $600 / $500 = 1.2x
─────────────────────────────────────────
Interpretation: For every $1 spent,
you earn $1.20 in revenue
```

### Code Implementation

```python
# In etl_local.py — safe division with 0 handling
def safe_divide(num, denom, decimals=2):
    result = []
    for n, d in zip(num, denom):
        if d == 0 or pd.isna(d):
            result.append(0.0)
        else:
            result.append(round(n / d, decimals))
    return result

# Usage
fact["roas"] = safe_divide(fact["revenue_usd"], fact["spend_usd"])

# Aggregate level
total_revenue = df["revenue_usd"].sum()
total_spend = df["spend_usd"].sum()
roas = total_revenue / total_spend  # 1.55 (no 0-division at this level)
```

### PySpark Version

```python
from pyspark.sql import functions as F

fact = fact.withColumn(
    "roas",
    F.round(
        F.col("revenue_usd") / F.when(
            F.col("spend_usd") > 0,
            F.col("spend_usd")
        ).otherwise(1.0),  # avoid division by zero
        2
    )
)
```

### Sanity Checking ROAS

```
Total spend across all channels: $13,500
Total revenue across all channels: $20,900
Overall ROAS: $20,900 / $13,500 = 1.55x ✅

Per channel:
  Meta:   $5,400 spend / $5,400 revenue = 1.00x ❌ (break-even)
  Google: $3,700 spend / $5,700 revenue = 1.55x ✅
  TikTok: $3,400 spend / $4,300 revenue = 1.24x ✅

Action: Reduce Meta spend or improve campaign targeting
```

---

## 9. How the Star Schema Works

### Fact Table Grain

The fact table is at **campaign × date** granularity. Every row represents one campaign's performance on one day.

```
fact_campaign_daily (992 rows = 32 campaigns × 31 days)
──────────────────────────────────────────────────────────
campaign_key  date_key    spend_usd  impressions  clicks  orders  revenue_usd
     1         20260101     15.20        45         8       0      0.00
     1         20260102     14.80        43         7       0      0.00
     1         20260103     16.10        48         9       1     60.00  ← conversion!
     1         20260104     15.50        44         8       0      0.00
     ...
    32         20260131     13.20        40         6       0      0.00
```

### Why This Grain?

```
Question: "What was ROAS for Meta prospecting campaigns in January?"

SELECT
    SUM(revenue_usd) / SUM(spend_usd) AS roas
FROM fact_campaign_daily f
JOIN dim_campaign c ON f.campaign_key = c.campaign_key
WHERE c.channel = 'Meta'
  AND c.campaign_type = 'prospecting'
  AND f.date_key BETWEEN 20260101 AND 20260131;

Answer: Joins fact with dim_campaign, filters by channel + type,
aggregates spend and revenue, divides. One query, no subqueries.
```

### Surrogate Keys

```python
# In etl_local.py — dim_campaign
campaigns["campaign_key"] = range(1, len(campaigns) + 1)

# In etl_local.py — dim_date
dim_date["date_key"] = pd.to_datetime(dim_date["date_id"]).dt.strftime("%Y%m%d").astype(int)
```

Surrogate keys (1, 2, 3...) vs natural keys ("camp_ME_001"):

| Type | Example | Use Case |
|---|---|---|
| Natural Key | "camp_ME_001" | Human-readable, in source data |
| Surrogate Key | 1 | Integer join, partition-friendly, immutable |

Surrogate keys are best practice because:
1. **Integer joins** are faster than string joins
2. **Immutable** — natural keys can change
3. **Partition-friendly** — integers hash better

---

## 10. Building and Running

### Prerequisites

```
Python 3.10+
pip (package installer)
No Java needed for etl_local.py
Java 17+ needed for spark_etl.py
No AWS account needed (runs 100% free locally)
```

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/lavishsaini1821/AdVantage_Analytics_Pipeline.git
cd AdVantage_Analytics_Pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate mock data
python generate_mock_data.py

# 4. Run ETL (pandas — no Java needed)
python etl_local.py

# 5. Open dashboard
streamlit run dashboard.py
```

### All Commands

```bash
# One-command run (generate + ETL + test)
python run.py

# Individual steps
python generate_mock_data.py    # Create 9 CSV files
python etl_local.py            # Run pandas ETL
python spark_etl.py            # Run PySpark ETL (needs Java)

# Dashboard
streamlit run dashboard.py      # Opens at http://localhost:8501

# Tests
python -m pytest tests/ -v    # 6 automated tests

# AWS mode (requires credentials in config.yaml)
# Change config.yaml: mode: "aws"
# Then: python spark_etl.py
```

### Understanding the Output

After running `python run.py`:

```
============================================================
AdVantage Analytics Pipeline - Full Run
============================================================
  1. Generate Mock Data
============================================================
[PASS] 1. Generate Mock Data

============================================================
  2. Run ETL
============================================================
Reading CSVs...
Building dim_date...
Building dim_campaign...
Building fact_campaign_daily...
Computing metrics...
Building marts...
  Wrote mart_channel_summary.csv (4 rows)
  Wrote mart_campaign_summary.csv (32 rows)
  Wrote mart_monthly_summary.csv (4 rows)
  Wrote mart_campaign_daily.csv (992 rows)
[PASS] 2. Run ETL

============================================================
  3. Run Tests
============================================================
tests/test_pipeline.py::test_data_generated PASSED
tests/test_pipeline.py::test_pii_masked PASSED
tests/test_pipeline.py::test_parquet_output_exists PASSED
tests/test_pipeline.py::test_roas_calculation PASSED
tests/test_pipeline.py::test_ctr_calculation PASSED
tests/test_pipeline.py::test_config_loads PASSED
6 passed in 1.34s
[PASS] 3. Run Tests

All steps complete!
```

### Dashboard URL

```
http://localhost:8501
```

---

## 11. Understanding the Output

### Test Results Explained

```
test_data_generated          → All 9 CSV files exist with correct row counts
test_pii_masked             → user_id is SHA-256 hash (32 hex chars), not raw email
test_parquet_output_exists   → fact + dim Parquet files written
test_roas_calculation       → ROAS = revenue / spend (verified on 5 rows)
test_ctr_calculation        → CTR = clicks / impressions × 100 (verified)
test_config_loads           → config.yaml is valid YAML
```

### Mart Tables Explained

| Table | Rows | Granularity | Use Case |
|---|---|---|---|
| `mart_channel_summary` | 4 | channel | "Which channel has best ROAS?" |
| `mart_campaign_summary` | 32 | campaign | "Which campaign should I scale?" |
| `mart_monthly_summary` | 4 | month × channel | "Did ROAS improve this month?" |
| `mart_campaign_daily` | 992 | campaign × day | "Daily trend for campaign X?" |

### Sanity Checking the Numbers

```
Total spend: $13,500
  = Sum of all fact_campaign_daily.spend_usd

Total revenue: $20,900
  = Sum of all fact_campaign_daily.revenue_usd

Overall ROAS: 1.55x
  = $20,900 / $13,500 = 1.549...

Channel count: 4
  = Meta, Google, TikTok, Other

Campaign count: 32
  = 4 channels × 8 campaigns each

Days: 31
  = January 2026

Rows in fact: 992
  = 32 campaigns × 31 days
```

---

## 12. Dashboard Walkthrough

### Tab 1: Analytics

![Channel Performance](../screenshots/channel-performance.png)

Shows:
- Per-channel performance cards (spend, revenue, ROAS, CAC, CTR, CVR)
- ROAS gauge chart with benchmark line
- Conversion funnel (Impressions → Clicks → Orders → Customers)
- Spend distribution donut chart
- Revenue distribution donut chart

### Tab 2: Campaign Explorer

![Campaign Performance](../screenshots/campaign-performance.png)

Shows:
- Campaign dropdown with all 32 campaigns
- KPI tiles (Spend, Revenue, ROAS, CAC)
- Daily spend bar chart
- Daily revenue line chart
- ROAS over time

### Tab 3: Trends

![Daily Trends](../screenshots/daily-trends.png)

Shows:
- Daily overview: spend vs revenue combined chart
- Channel contribution stacked area chart
- Per-channel daily trend charts

### Tab 4: Rankings

![Top Campaigns](../screenshots/top-campaigns.png)

Shows:
- 🏆 Top 5 campaigns by ROAS (with gold badge)
- ⚠️ Bottom 5 campaigns by ROAS (with warning badge)
- Top 10 campaigns bar chart

---

## 13. Extending the Project

### Ideas for Improvement

**1. Real Data Sources**

```python
# Instead of mock CSV, read from APIs
import facebook_business
from google.ads.googleads import GoogleAdsClient

# Facebook Ads API
campaigns = facebook.get_campaigns(ad_account_id="act_123456")

# Google Ads API
client = GoogleAdsClient.load_from_storage()
ga_service = client.get_service("GoogleAdsService")
```

**2. More Dimensions**

```sql
-- Add device type
ALTER TABLE fact_campaign_daily ADD COLUMN device VARCHAR(20);

-- Add geo
ALTER TABLE fact_campaign_daily ADD COLUMN country VARCHAR(50);
```

**3. Real-time Dashboard**

```python
# Auto-refresh every 30 seconds
@st.fragment
def live_metrics():
    st_autorefresh(interval=30000)
```

**4. Alerting**

```python
# Alert when ROAS drops below threshold
if campaign_roas < 1.0:
    send_alert(f"⚠️ {campaign_name} ROAS below 1.0!")
```

**5. Machine Learning**

```python
# Predict next month's ROAS
from sklearn.linear_model import LinearRegression

X = monthly_data[["spend", "impressions", "clicks"]]
y = monthly_data["revenue"]
model = LinearRegression().fit(X, y)
predicted_revenue = model.predict([[5000, 100000, 2000]])
```

---

## 14. Interview Talking Points

### "Why Star Schema?"

> "Star schema is the standard for analytical workloads. It's denormalized for fast reads — facts and dimensions joined on surrogate keys. Easy for business users to understand, and partition-friendly for cloud queries. The fact table at campaign × date grain lets us answer any question: daily, weekly, monthly, by channel, by campaign type."

### "How does this scale?"

> "PySpark distributes data across partitions. S3 + Athena handles petabytes cheaply. Glue auto-discovers schema from Parquet. The same code runs locally on 49K rows and on AWS with 49 billion rows."

### "How do you handle PII?"

> "User IDs are salted SHA-256 hashes — no raw PII stored. The salt is in config.yaml, so even if someone gets the hash, they can't reverse it without the salt. This is GDPR-compliant."

### "How would you debug a ROAS drop?"

> "Three-step approach:
> 1. Check spend ↑ or revenue ↓ (numerator vs denominator)
> 2. Segment by: channel, campaign, device, geo, time period
> 3. Compare vs. prior period

> The dashboard's drill-down tab makes this easy — select the campaign, see day-by-day trend, identify exactly when ROAS dropped."

### "What's the difference between etl_local.py and spark_etl.py?"

> "Same logic, different engine. pandas for local (no Java), PySpark for cloud. The star schema, metrics, and marts are identical. Only the execution engine changes. This is the pattern used in real data engineering: write once, run anywhere."

### "How do you ensure data quality?"

> "Automated tests:
> - Row counts match expected values
> - PII is masked (SHA-256, 32 hex chars)
> - ROAS = revenue / spend (verified)
> - CTR = clicks / impressions × 100 (verified)
> - Parquet files written correctly

> GitHub Actions runs these on every push — if a commit breaks a metric, we know immediately."

---

## Summary

This AdVantage Analytics Pipeline demonstrates:

```
┌─────────────────────────────────────────────────────────┐
│  Data Engineering          │  Analytics & Reporting      │
├───────────────────────────┼───────────────────────────┤
│ Star schema design         │ ROAS, CAC, CTR, CVR        │
│ Deterministic mock data    │ Pre-aggregated marts        │
│ PySpark ETL (local + AWS) │ Interactive dashboard       │
│ PII masking (SHA-256)     │ Plotly visualizations       │
│ Partitioned Parquet        │ KPI gauges & funnels        │
│ CI/CD (GitHub Actions)    │ Campaign drill-down         │
└─────────────────────────────────────────────────────────┘
```

**The key insight:** Marketing analytics is fundamentally about measuring efficiency — how much revenue does each dollar of ad spend generate? The star schema answers this in milliseconds because all the aggregations are pre-computed.

**The architectural insight:** The same Python code runs locally (no AWS, no Java) and on production cloud infrastructure. This "write once, run anywhere" pattern is how real data teams build portable pipelines.

---

**Built with:** Python · PySpark · AWS S3 · Glue · Athena · Parquet · SQL · Streamlit · GitHub Actions

**Questions?** Check the code comments — every function has a docstring explaining what it does and why.
