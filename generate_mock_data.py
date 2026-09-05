"""
AdVantage Analytics Pipeline - Mock Data Generator

Generates deterministic mock advertising data:
- dim_advertisers, dim_campaigns, dim_users, dim_time
- fact_ad_events (50K rows)
- fact_conversions (195 rows)

PII is masked using salted SHA-256 hashes.
Fixed seed=42 ensures reproducible output across runs.

Usage:
    python generate_mock_data.py
"""

import csv
import hashlib
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

import yaml

# ── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "mock_csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

with (BASE_DIR / "config.yaml").open() as f:
    cfg = yaml.safe_load(f)

SEED = cfg["data"]["seed"]
START = date.fromisoformat(cfg["data"]["start_date"])
DAYS = cfg["data"]["days"]
NUM_CAMPAIGNS = cfg["data"]["num_campaigns"]
NUM_USERS = cfg["data"]["num_users"]
NUM_EVENTS = cfg["data"]["events_per_day"] * DAYS
NUM_CONVERSIONS = cfg["data"]["num_conversions"]
PIISALT = cfg["pii_salt"]

# Fixed seed ensures same data on every run
random.seed(SEED)


# ── Helpers ────────────────────────────────────────────────────────────────────

def mask(value: str) -> str:
    """Return salted SHA-256 hash (first 32 chars) for PII fields."""
    return hashlib.sha256(f"{PIISALT}:{value}".encode()).hexdigest()[:32]


def write_csv(filename: str, rows: List[Dict]) -> None:
    """Write list of dicts to CSV file."""
    if not rows:
        return
    path = OUTPUT_DIR / filename
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows -> {filename}")


def daterange(n: int) -> List[date]:
    """Return list of n consecutive dates starting from START."""
    return [START + timedelta(days=i) for i in range(n)]


# ── Dimension Tables ────────────────────────────────────────────────────────

def gen_advertisers() -> List[Dict]:
    """One row per advertiser (Meta, Google, TikTok, Other)."""
    advertisers = [
        ("adv_001", "Acme Corp", "ecommerce", "Meta"),
        ("adv_002", "Beta Inc", "ecommerce", "Meta"),
        ("adv_003", "Gamma Ltd", "fintech", "Google"),
        ("adv_004", "Delta Co", "fintech", "Google"),
        ("adv_005", "Epsilon AI", "gaming", "TikTok"),
        ("adv_006", "Zeta Firm", "travel", "Other"),
        ("adv_007", "Eta Group", "health", "Other"),
        ("adv_008", "Theta LLC", "b2b_saas", "Other"),
    ]
    return [
        {
            "advertiser_id": aid,
            "advertiser_name": name,
            "industry": industry,
            "channel": channel,
            "account_status": "active",
        }
        for aid, name, industry, channel in advertisers
    ]


def gen_campaigns() -> List[Dict]:
    """One row per campaign (32 total: 8 per major channel)."""
    campaigns = []
    types = ["brand_awareness", "performance", "prospecting", "retargeting"]
    statuses = ["active", "paused", "completed"]
    dates = daterange(DAYS)

    # 8 campaigns per major channel
    for channel in ["Meta", "Google", "TikTok", "Other"]:
        for i in range(8):
            cid = f"camp_{channel[:2].upper()}_{i+1:03d}"
            campaign_type = types[i % len(types)]
            status = statuses[i % len(statuses)]
            budgets = [500, 1000, 2000, 3000, 1500, 2500, 800, 1200]
            campaigns.append({
                "campaign_id": cid,
                "campaign_name": f"{channel} Campaign {i+1}",
                "advertiser_id": f"adv_{(i % 2) + 1:03d}",
                "channel": channel,
                "campaign_type": campaign_type,
                "status": status,
                "start_date": str(dates[0]),
                "end_date": str(dates[-1]),
                "daily_budget_usd": budgets[i],
            })
    return campaigns


def gen_time() -> List[Dict]:
    """One row per day (date dimension)."""
    rows = []
    for i, d in enumerate(daterange(DAYS)):
        rows.append({
            "date_id": str(d),
            "year": d.year,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "day_of_month": d.day,
            "day_of_week": d.weekday(),          # 0=Mon
            "day_name": d.strftime("%A"),
            "is_weekend": 1 if d.weekday() >= 5 else 0,
            "quarter": (d.month - 1) // 3 + 1,
            "week_of_year": d.isocalendar()[1],
        })
    return rows


def gen_users() -> List[Dict]:
    """One row per anonymized user (PII masked)."""
    rows = []
    for i in range(NUM_USERS):
        age = random.choice(["18-24", "25-34", "35-44", "45-54"])
        gender = random.choice(["male", "female", "unknown"])
        rows.append({
            "user_id": mask(f"user_{i}@example.com"),   # PII masked
            "age_band": age,
            "gender": gender,
            "country_code": random.choice(["US", "CA", "GB", "DE", "IN"]),
            "acquisition_channel": random.choice(["paid", "organic", "referral"]),
            "lifetime_value_usd": round(random.uniform(10, 500), 2),
        })
    return rows


# ── Fact Tables ─────────────────────────────────────────────────────────────

def gen_ad_events(campaigns: List[Dict], users: List[Dict]) -> List[Dict]:
    """One row per ad event (impression or click). ~50K total."""
    rows = []
    dates = daterange(DAYS)
    user_ids = [u["user_id"] for u in users]

    for _ in range(NUM_EVENTS):
        campaign = random.choice(campaigns)
        d = random.choice(dates)
        event_type = random.choices(["impression", "click"], weights=[85, 15])[0]

        # Spend: impressions are cheap, clicks cost more
        if event_type == "impression":
            cost = round(random.uniform(0.01, 0.05), 4)
        else:
            cost = round(random.uniform(0.5, 3.0), 2)

        rows.append({
            "event_id": f"ev_{mask(str(random.random()))[:12]}",
            "event_date": str(d),
            "campaign_id": campaign["campaign_id"],
            "channel": campaign["channel"],
            "event_type": event_type,
            "user_id": random.choice(user_ids),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "geo": random.choice(["US", "CA", "GB", "DE", "IN"]),
            "cost_usd": cost,
            "revenue_usd": 0.0,
            "is_conversion": 0,
        })
    return rows


def gen_conversions(campaigns: List[Dict], users: List[Dict]) -> List[Dict]:
    """One row per conversion event. ~195 total."""
    rows = []
    dates = daterange(DAYS)
    user_ids = [u["user_id"] for u in users]

    for _ in range(NUM_CONVERSIONS):
        campaign = random.choice(campaigns)
        d = random.choice(dates)
        conv_type = random.choice(["purchase", "signup", "lead", "install"])
        value = round(random.uniform(10, 200), 2)
        is_new = random.choice([0, 1])

        rows.append({
            "conversion_id": f"cv_{mask(str(random.random()))[:12]}",
            "conversion_date": str(d),
            "campaign_id": campaign["campaign_id"],
            "channel": campaign["channel"],
            "user_id": random.choice(user_ids),
            "conversion_type": conv_type,
            "conversion_value_usd": value,
            "is_new_customer": is_new,
        })
    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Generating mock data (seed={})...".format(SEED))

    # Generate dimensions first (fact tables reference them)
    advertisers = gen_advertisers()
    campaigns = gen_campaigns()
    users = gen_users()
    time_dim = gen_time()

    # Generate fact tables
    ad_events = gen_ad_events(campaigns, users)
    conversions = gen_conversions(campaigns, users)

    # Write all CSVs
    write_csv("dim_advertisers.csv", advertisers)
    write_csv("dim_campaigns.csv", campaigns)
    write_csv("dim_users.csv", users)
    write_csv("dim_time.csv", time_dim)
    write_csv("fact_ad_events.csv", ad_events)
    write_csv("fact_conversions.csv", conversions)

    print("\nDone! Data saved to: {}".format(OUTPUT_DIR))


if __name__ == "__main__":
    main()
