"""
AdVantage Analytics Pipeline - One-command pipeline runner

Runs: generate data -> PySpark ETL -> tests
Same flow as GitHub Actions CI.

Usage:
    python run.py
"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def step(label: str, cmd: list) -> bool:
    """Run one pipeline step. Return True on success."""
    print("\n" + "=" * 60)
    print(f"  {label}")
    print("=" * 60)
    result = subprocess.run(cmd, cwd=str(BASE_DIR))
    return result.returncode == 0


def main() -> int:
    print("AdVantage Analytics Pipeline - Full Run")

    steps = [
        ("1. Generate Mock Data", ["python", "generate_mock_data.py"]),
        ("2. Run ETL", ["python", "etl_local.py"]),
        ("3. Run Tests", ["python", "-m", "pytest", "tests/", "-v"]),
    ]

    for label, cmd in steps:
        if not step(label, cmd):
            print(f"\n[FAIL] Stopped after: {label}")
            return 1
        print(f"[PASS] {label}")

    print("\n" + "=" * 60)
    print("All steps complete!")
    print("=" * 60)
    print("\nNext: streamlit run dashboard.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
