# AdVantage Analytics Pipeline

**Cloud-based marketing intelligence platform** — ROAS, CAC, CTR, CVR analytics across Meta, Google, TikTok.

> Tech stack: Python + PySpark + AWS S3 + Glue + Athena + Parquet + SQL + Streamlit + GitHub Actions
> Runs locally for free (no AWS account needed) — same code deploys to real AWS with one config change.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Language | **Python** | All scripts |
| Data Gen | **Python** | Deterministic mock CSVs |
| ETL | **PySpark** (local[*] / Glue Job) | Transform, aggregate, write Parquet |
| Storage | **S3** (LocalStack / real AWS) | Store Parquet files |
| Catalog | **AWS Glue** | Table schema registry |
| Query | **AWS Athena** | SQL on Parquet |
| Marts | **SQL / pandas** | Pre-aggregated summary tables |
| Dashboard | **Streamlit + Plotly** | Interactive KPIs |
| CI/CD | **GitHub Actions** | Auto-run on every push |

---

## Architecture

```
mock CSV files
    │
    ├──► etl_local.py          (pandas, no Java needed)
    │
    └──► spark_etl.py          (PySpark, needs Java)
              │
              ├──► S3 / Parquet  (data/parquet/)
              ├──► Glue Catalog   (registers tables)
              ├──► Athena        (runs SQL marts)
              └──► data/marts/*.csv  (dashboard reads here)
                           │
                           ▼
                   dashboard.py    (Streamlit UI)
```

---

## Quick Start (Free — No AWS)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate mock advertising data
python generate_mock_data.py

# 3. Run ETL (pandas version — no Java needed)
python etl_local.py

# 4. Launch dashboard
streamlit run dashboard.py
```

### For PySpark version (needs Java)
```bash
# Install Java 17+, then:
python spark_etl.py
```

---

## Project Structure

```
advantage_pipeline_project/
├── config.yaml              # All settings (seed, AWS mode, S3 bucket)
├── generate_mock_data.py    # Create mock CSVs (deterministic, seed=42)
├── etl_local.py            # ETL using pandas (no Java)
├── spark_etl.py            # ETL using PySpark (for AWS Glue / EMR)
├── dashboard.py            # Streamlit dashboard (reads mart CSV files)
├── sql/
│   ├── star_schema.sql      # Star schema DDL (SQLite + Athena compatible)
│   └── marts.sql           # 3 analytical marts (Athena SQL)
├── data/
│   ├── mock_csv/           # Raw mock data (9 CSV files)
│   ├── parquet/            # Processed Parquet files
│   └── marts/              # Pre-aggregated marts (dashboard reads here)
├── tests/
│   └── test_pipeline.py    # 6 automated tests
└── .github/workflows/
    └── ci.yml              # GitHub Actions CI
```

---

## Metrics

| Metric | Formula |
|---|---|
| **ROAS** | `SUM(revenue_usd) / SUM(spend_usd)` |
| **CAC** | `SUM(spend_usd) / SUM(new_customers)` |
| **CVR** | `SUM(orders) / SUM(clicks) × 100` |
| **CTR** | `SUM(clicks) / SUM(impressions) × 100` |

---

## Star Schema

```
          dim_date (day)
     ┌─────────────────────┐
     │ date_key, year,     │
     │ month, is_weekend    │
     └──────────┬───────────┘
                │
     ┌──────────┴──────────────────────────┐
     │   fact_campaign_daily               │
     │   (campaign × date)                │
     │   spend_usd, impressions, clicks,    │
     │   orders, revenue_usd, new_customers│
     │   roas, cac, conv_rate_pct, ctr  │
     └──────────┬──────────────────────────┘
                │
     ┌──────────┴────────────────┐
     │   dim_campaign             │
     │   campaign_key, channel,   │
     │   campaign_type, status   │
     └───────────────────────────┘
```

---

## AWS Deployment (When You Have an Account)

```yaml
# In config.yaml:
aws:
  mode: "aws"                          # Change from "local"
  access_key: "AKIA..."               # Your AWS credentials
  secret_key: "..."
  region: "us-east-1"
  s3_bucket: "advantage-data"
  glue_db: "advantage_db"
```

Then:
1. Create S3 bucket: `advantage-data`
2. Run Glue crawler on `s3://advantage-data/`
3. Query with Athena: `SELECT * FROM advantage_db.fact_campaign_daily`
4. Deploy: same `spark_etl.py` works on Glue Job / EMR / Databricks

---

## Interview Talking Points

**"Why Star Schema?"**
Denormalized for fast reads — facts and dimensions joined on keys. Easy for business users to understand. Partition-friendly for cloud.

**"How does this scale?"**
PySpark distributes data across partitions. S3 + Athena handles petabytes cheaply. Glue auto-discovers schema.

**"How do you handle PII?"**
User IDs are salted SHA-256 hashes — no raw PII stored.

**"How would you debug a ROAS drop?"**
1. Check spend ↑ or revenue ↓ (numerator vs denominator)
2. Segment by: channel, campaign, device, geo, time
3. Compare vs. prior period

---

*AdVantage — Python · PySpark · AWS S3 · Glue · Athena · Parquet · SQL · Streamlit · GitHub Actions*
