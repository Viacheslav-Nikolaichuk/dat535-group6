"""
Diagnostic tool to verify connectivity, latency, and authentication
for Strømpris (Hva Koster Strømmen) and MET Norway Frost APIs.

Usage:
    python scripts/test_api_connections.py
"""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Reconfigure stdout for UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from config.settings import FROST_BASE_URL, FROST_CLIENT_ID, STROMPRIS_BASE_URL
from src.ingestion.frost_client import FrostClient
from src.ingestion.strompris_client import StromprisClient


def test_strompris() -> dict:
    """Test Strømpris API connectivity and response format."""
    print("\n[1/2] Testing Strømpris API (Hva Koster Strømmen)...")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    client = StromprisClient()

    start = time.time()
    try:
        res = client.fetch_prices(date_str=yesterday, zone="NO1")
        elapsed = (time.time() - start) * 1000
        print(f"  [OK] Connected to: {res['source_uri']}")
        print(f"  [OK] HTTP Latency: {elapsed:.1f}ms")
        print(f"  [OK] Records Returned: {res['record_count']} hourly spot prices")
        sample = res["records"][0]
        print(f"  [OK] Sample [00:00]: {sample['NOK_per_kWh']:.4f} NOK/kWh (EUR: {sample['EUR_per_kWh']})")
        return {"status": "SUCCESS", "latency_ms": elapsed, "count": res["record_count"]}
    except Exception as e:
        print(f"  [FAIL] Strømpris API test failed: {e}")
        return {"status": "FAILED", "error": str(e)}


def test_frost() -> dict:
    """Test MET Norway Frost API authentication and observation retrieval."""
    print("\n[2/2] Testing MET Norway Frost API...")
    if not FROST_CLIENT_ID:
        print("  [WARN] FROST_CLIENT_ID is not configured in .env or environment!")
        print("    Register at https://frost.met.no/auth/requestCredentials.html")
        return {"status": "MISSING_CREDENTIALS"}

    masked_id = FROST_CLIENT_ID[:8] + "..." + FROST_CLIENT_ID[-4:] if len(FROST_CLIENT_ID) > 12 else "***"
    print(f"  [OK] Frost Client ID detected: {masked_id}")

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    client = FrostClient()

    start = time.time()
    try:
        res = client.fetch_observations(
            date_str=yesterday,
            station_id="SN18700",  # Oslo Blindern
            elements=["air_temperature", "wind_speed"],
        )
        elapsed = (time.time() - start) * 1000
        print(f"  [OK] Authentication & connectivity successful!")
        print(f"  [OK] HTTP Latency: {elapsed:.1f}ms")
        print(f"  [OK] Station: SN18700 (Oslo Blindern)")
        print(f"  [OK] Total observations returned: {res['record_count']}")
        if res["records"]:
            sample = res["records"][0]
            ref_time = sample.get("referenceTime")
            obs_preview = [
                f"{o['elementId']}={o['value']}{o.get('unit', '')}"
                for o in sample.get("observations", [])[:2]
            ]
            print(f"  [OK] Sample [{ref_time}]: {', '.join(obs_preview)}")
        return {"status": "SUCCESS", "latency_ms": elapsed, "count": res["record_count"]}
    except Exception as e:
        print(f"  [FAIL] Frost API test failed: {e}")
        return {"status": "FAILED", "error": str(e)}


def check_tomorrow_prices() -> None:
    """Check availability of tomorrow's electricity prices."""
    print("\n[INFO] Checking Day-Ahead (Tomorrow) Electricity Prices...")
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    client = StromprisClient()
    try:
        res = client.fetch_prices(date_str=tomorrow, zone="NO1")
        print(f"  [OK] Tomorrow's prices ({tomorrow}) are ALREADY published! ({res['record_count']} records)")
    except FileNotFoundError:
        print(f"  [INFO] Tomorrow's prices ({tomorrow}) not yet published.")
        print("    Nord Pool day-ahead auction publishes prices daily around 13:00 CET.")
    except Exception as e:
        print(f"  ! Could not verify tomorrow's prices: {e}")


def main() -> int:
    print("=" * 60)
    print("DAT535 API CONNECTIVITY & DIAGNOSTIC TEST")
    print("=" * 60)

    s_res = test_strompris()
    f_res = test_frost()
    check_tomorrow_prices()

    print("\n" + "=" * 60)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 60)
    print(f"Strømpris API: {s_res.get('status')}")
    print(f"Frost MET API: {f_res.get('status')}")
    print("=" * 60)

    success = (s_res.get("status") == "SUCCESS") and (f_res.get("status") == "SUCCESS")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
