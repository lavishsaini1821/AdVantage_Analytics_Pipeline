"""
AdVantage Analytics Pipeline - Local ETL (pandas version)

Same logic as spark_etl.py but uses pandas instead of PySpark.
No Java needed. Works on any machine.

If PySpark is available with Java, use spark_etl.py instead for
production-style distributed processing.

Usage:
    python etl_local.py
"""

from pathlib import Path

import pandas as pd
import yaml

# ── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "mock_csv"
PARQUET_DIR = BASE_DIR / "data" / "parquet"
MART_DIR = BASE_DIR / "data" / "marts"
MART_DIR.mkdir(parents=True, exist_ok=True)

with (BASE_DIR / "config.yaml").open() as f:
    cfg = yaml.safe_load(f)

with (BASE_DIR / "config.yaml").open() as _:
    pass  # already loaded above


def load_csv(name: str) -> pd.DataFrame:
    """Load a CSV into a pandas DataFrame."""
    return pd.read_csv(INPUT_DIR / f"{name}.csv")


def build_star_schema() -> None:
    """Build star schema: dims + fact + derived metrics."""
    print("Reading CSVs...")

    advertisers = load_csv("dim_advertisers")
    campaigns = load_csv("dim_campaigns")
    time_dim = load_csv("dim_time")
    events = load_csv("fact_ad_events")
    conversions = load_csv("fact_conversions")

    # ── dim_date ──
    print("Building dim_date...")
    dim_date = time_dim.copy()
    dim_date["date_key"] = pd.to_datetime(dim_date["date_id"]).dt.strftime("%Y%m%d").astype(int)

    # ── dim_campaign: add surrogate key ──
    print("Building dim_campaign...")
    campaigns["campaign_key"] = range(1, len(campaigns) + 1)

    # ── fact_campaign_daily ──
    # Aggregate events by (campaign_id, event_date)
    print("Building fact_campaign_daily...")
    events["event_date"] = pd.to_datetime(events["event_date"])

    event_agg = (
        events
        .groupby(["campaign_id", "event_date"], as_index=False)
        .agg(
            spend_usd=("cost_usd", "sum"),
            impressions=("event_type", lambda x: (x == "impression").sum()),
            clicks=("event_type", lambda x: (x == "click").sum()),
        )
    )

    # Aggregate conversions by (campaign_id, conversion_date)
    conversions["conversion_date"] = pd.to_datetime(conversions["conversion_date"])
    conv_agg = (
        conversions
        .groupby(["campaign_id", "conversion_date"], as_index=False)
        .agg(
            orders=("conversion_id", "count"),
            revenue_usd=("conversion_value_usd", "sum"),
            new_customers=("is_new_customer", lambda x: x.sum()),
        )
    )

    # Merge events + conversions on (campaign_id, date)
    fact = event_agg.merge(
        conv_agg,
        left_on=["campaign_id", "event_date"],
        right_on=["campaign_id", "conversion_date"],
        how="left",
    ).fillna(0)

    # Join with campaigns to get campaign_key and other dims
    fact = fact.merge(
        campaigns[["campaign_id", "campaign_key", "channel", "campaign_type"]],
        on="campaign_id",
        how="left",
    )

    # Date key
    fact["date_key"] = fact["event_date"].dt.strftime("%Y%m%d").astype(int)
    fact["date"] = fact["event_date"].dt.strftime("%Y-%m-%d")

    # ── Derived metrics ──
    # ROAS = revenue / spend
    # CAC  = spend / new_customers
    # CVR  = orders / clicks * 100
    # CTR  = clicks / impressions * 100
    print("Computing metrics...")

    def safe_divide(num, denom, decimals=2):
        """Divide with 0 handling. Returns 0.0 if denom is 0."""
        result = []
        for n, d in zip(num, denom):
            if d == 0 or pd.isna(d):
                result.append(0.0)
            else:
                result.append(round(n / d, decimals))
        return result

    fact["roas"] = safe_divide(fact["revenue_usd"], fact["spend_usd"])
    fact["cac"] = safe_divide(fact["spend_usd"], fact["new_customers"])
    fact["conv_rate_pct"] = safe_divide(fact["orders"] * 100, fact["clicks"])
    fact["ctr_pct"] = safe_divide(fact["clicks"] * 100, fact["impressions"])

    # Select final columns
    fact = fact[[
        "campaign_key", "date_key", "date", "channel", "campaign_type",
        "spend_usd", "impressions", "clicks", "orders", "revenue_usd",
        "new_customers", "roas", "cac", "conv_rate_pct", "ctr_pct"
    ]]

    # ── Write output ──
    print(f"Writing Parquet to {PARQUET_DIR}...")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Write partitioned Parquet (by channel for variety)
    fact.to_parquet(PARQUET_DIR / "fact_campaign_daily.parquet", index=False)
    dim_date.to_parquet(PARQUET_DIR / "dim_date.parquet", index=False)
    campaigns[["campaign_key", "campaign_id", "campaign_name", "channel",
               "campaign_type", "status", "start_date", "end_date", "daily_budget_usd"]
    ].to_parquet(PARQUET_DIR / "dim_campaign.parquet", index=False)

    return fact


def build_marts(fact: pd.DataFrame) -> None:
    """Build 4 pre-aggregated marts for the dashboard."""
    print("\nBuilding marts...")

    # ── Mart 1: channel_summary ──
    mart_channel = (
        fact.groupby("channel")
        .agg(
            campaign_count=("campaign_key", "nunique"),
            total_spend_usd=("spend_usd", "sum"),
            total_revenue_usd=("revenue_usd", "sum"),
            total_impressions=("impressions", "sum"),
            total_clicks=("clicks", "sum"),
            total_orders=("orders", "sum"),
            total_new_customers=("new_customers", "sum"),
        )
        .reset_index()
    )
    mart_channel["roas"] = mart_channel.apply(
        lambda r: round(r.total_revenue_usd / r.total_spend_usd, 2)
        if r.total_spend_usd > 0 else 0.0, axis=1)
    mart_channel["cac"] = mart_channel.apply(
        lambda r: round(r.total_spend_usd / r.total_new_customers, 2)
        if r.total_new_customers > 0 else 0.0, axis=1)
    mart_channel["conv_rate_pct"] = mart_channel.apply(
        lambda r: round(r.total_orders / r.total_clicks * 100, 2)
        if r.total_clicks > 0 else 0.0, axis=1)
    mart_channel["ctr_pct"] = mart_channel.apply(
        lambda r: round(r.total_clicks / r.total_impressions * 100, 2)
        if r.total_impressions > 0 else 0.0, axis=1)
    mart_channel.to_csv(MART_DIR / "mart_channel_summary.csv", index=False)
    print(f"  Wrote mart_channel_summary.csv ({len(mart_channel)} rows)")

    # ── Mart 2: campaign_summary ──
    mart_campaign = (
        fact.groupby(["campaign_key", "channel", "campaign_type"])
        .agg(
            total_spend_usd=("spend_usd", "sum"),
            total_revenue_usd=("revenue_usd", "sum"),
            total_impressions=("impressions", "sum"),
            total_clicks=("clicks", "sum"),
            total_orders=("orders", "sum"),
            total_new_customers=("new_customers", "sum"),
        )
        .reset_index()
    )
    mart_campaign["roas"] = mart_campaign.apply(
        lambda r: round(r.total_revenue_usd / r.total_spend_usd, 2)
        if r.total_spend_usd > 0 else 0.0, axis=1)
    mart_campaign["cac"] = mart_campaign.apply(
        lambda r: round(r.total_spend_usd / r.total_new_customers, 2)
        if r.total_new_customers > 0 else 0.0, axis=1)
    mart_campaign["conv_rate_pct"] = mart_campaign.apply(
        lambda r: round(r.total_orders / r.total_clicks * 100, 2)
        if r.total_clicks > 0 else 0.0, axis=1)
    mart_campaign.to_csv(MART_DIR / "mart_campaign_summary.csv", index=False)
    print(f"  Wrote mart_campaign_summary.csv ({len(mart_campaign)} rows)")

    # ── Mart 3: monthly_summary ──
    fact["month"] = pd.to_datetime(fact["date"]).dt.month
    mart_monthly = (
        fact.groupby(["month", "channel"])
        .agg(
            spend_usd=("spend_usd", "sum"),
            revenue_usd=("revenue_usd", "sum"),
            total_orders=("orders", "sum"),
            new_customers=("new_customers", "sum"),
        )
        .reset_index()
    )
    mart_monthly["roas"] = mart_monthly.apply(
        lambda r: round(r.revenue_usd / r.spend_usd, 2)
        if r.spend_usd > 0 else 0.0, axis=1)
    mart_monthly["cac"] = mart_monthly.apply(
        lambda r: round(r.spend_usd / r.new_customers, 2)
        if r.new_customers > 0 else 0.0, axis=1)
    mart_monthly.to_csv(MART_DIR / "mart_monthly_summary.csv", index=False)
    print(f"  Wrote mart_monthly_summary.csv ({len(mart_monthly)} rows)")

    # ── Mart 4: campaign_daily ──
    fact[[
        "campaign_key", "date", "channel", "campaign_type",
        "spend_usd", "impressions", "clicks", "orders", "revenue_usd",
        "new_customers", "roas", "cac", "conv_rate_pct", "ctr_pct"
    ]].to_csv(MART_DIR / "mart_campaign_daily.csv", index=False)
    print(f"  Wrote mart_campaign_daily.csv ({len(fact)} rows)")


def main() -> None:
    print("=" * 60)
    print("AdVantage Local ETL (pandas)")
    print("=" * 60)

    fact = build_star_schema()
    build_marts(fact)

    print("\n" + "=" * 60)
    print("ETL Complete!")
    print(f"  Parquet: {PARQUET_DIR}")
    print(f"  Marts:   {MART_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
