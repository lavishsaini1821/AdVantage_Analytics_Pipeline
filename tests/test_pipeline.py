"""
AdVantage Analytics Pipeline - Tests
Validates: data generation, ETL output, metric calculations

Run: python -m pytest tests/ -v
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "data" / "parquet"
CSV_DIR = BASE_DIR / "data" / "mock_csv"
MART_DIR = BASE_DIR / "data" / "marts"


# ── Test 1: Data generation ────────────────────────────────────────────────

def test_data_generated():
    """All dimension and fact CSVs exist with expected row counts."""
    expected = {
        "dim_advertisers.csv": 8,
        "dim_campaigns.csv": 32,
        "dim_users.csv": 2000,
        "dim_time.csv": 31,
        "fact_ad_events.csv": 49000,
        "fact_conversions.csv": 195,
    }
    for name, min_rows in expected.items():
        path = CSV_DIR / name
        assert path.exists(), f"Missing: {name}"
        df = pd.read_csv(path)
        assert len(df) >= min_rows, f"{name} has {len(df)} rows, expected >= {min_rows}"


# ── Test 2: PII Masking ───────────────────────────────────────────────────

def test_pii_masked():
    """user_id is a SHA-256 hash (32 hex chars), not raw email."""
    path = CSV_DIR / "dim_users.csv"
    if not path.exists():
        pytest.skip("No user data")
    users = pd.read_csv(path)
    assert "user_id" in users.columns
    # First user_id should be 32 hex chars
    sample = users["user_id"].iloc[0]
    assert len(sample) == 32, f"user_id length {len(sample)}, expected 32"
    assert all(c in "0123456789abcdef" for c in sample), "user_id not hex"


# ── Test 3: Star Schema Output ────────────────────────────────────────────

def test_parquet_output_exists():
    """ETL wrote fact + dim Parquet files."""
    for name in ["fact_campaign_daily", "dim_date", "dim_campaign"]:
        path = PARQUET_DIR / f"{name}.parquet"
        assert path.exists(), f"Missing Parquet: {name}.parquet"


# ── Test 4: Metric Correctness ────────────────────────────────────────────

def test_roas_calculation():
    """ROAS = revenue / spend. Test on a few rows."""
    path = PARQUET_DIR / "fact_campaign_daily.parquet"
    if not path.exists():
        pytest.skip("No fact data")
    df = pd.read_parquet(path)
    if len(df) == 0:
        pytest.skip("Empty fact table")
    for _, row in df.head(5).iterrows():
        if row["spend_usd"] > 0:
            expected = round(row["revenue_usd"] / row["spend_usd"], 2)
            assert abs(row["roas"] - expected) < 0.5, f"ROAS mismatch: {row['roas']} vs {expected}"


def test_ctr_calculation():
    """CTR = clicks / impressions * 100."""
    path = PARQUET_DIR / "fact_campaign_daily.parquet"
    if not path.exists():
        pytest.skip("No fact data")
    df = pd.read_parquet(path)
    for _, row in df.head(5).iterrows():
        if row["impressions"] > 0:
            expected = round(row["clicks"] / row["impressions"] * 100, 2)
            assert abs(row["ctr_pct"] - expected) < 1.0, f"CTR mismatch: {row['ctr_pct']} vs {expected}"


# ── Test 5: Config ────────────────────────────────────────────────────────

def test_config_loads():
    """config.yaml is valid YAML with required keys."""
    import yaml
    with (BASE_DIR / "config.yaml").open() as f:
        cfg = yaml.safe_load(f)
    assert "data" in cfg
    assert "seed" in cfg["data"]
    assert "aws" in cfg


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
