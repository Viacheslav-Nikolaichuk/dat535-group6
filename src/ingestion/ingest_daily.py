"""
Daily Batch Ingestion Pipeline Entrypoint.
Extracts daily electricity spot prices (NO1-NO5) and meteorological observations
from MET Norway, envelopes them with audit metadata, and persists to Bronze storage.

Usage:
    python src/ingestion/ingest_daily.py                  # Ingests yesterday's data
    python src/ingestion/ingest_daily.py --date 2024-01-15 # Ingests specific historical date
    python src/ingestion/ingest_daily.py --dry-run        # Tests API without writing to disk
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import sys
import time
from typing import Dict, List, Optional

# Ensure project root is on sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    BRONZE_DIR,
    FROST_CLIENT_ID,
    PRICE_ZONES,
    ZONES_CONFIG_FILE,
)
from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.frost_client import FrostClient
from src.ingestion.strompris_client import StromprisClient
from src.utils.hashing import generate_batch_id

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_daily")


def load_stations_config(config_path: Path = ZONES_CONFIG_FILE) -> Dict:
    """Load bidding zone to weather stations mapping from JSON configuration."""
    if not config_path.exists():
        logger.warning(f"Config file not found at {config_path}. Using fallback mapping.")
        return {
            "NO1": ["SN18700"],
            "NO2": ["SN39040"],
            "NO3": ["SN68860"],
            "NO4": ["SN90450"],
            "NO5": ["SN50540"],
        }

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    zone_map = {}
    for zone_code, zone_info in data.get("zones", {}).items():
        stations = [s["id"] for s in zone_info.get("stations", [])]
        if not stations and "primary_station" in zone_info:
            stations = [zone_info["primary_station"]]
        zone_map[zone_code] = stations

    return zone_map


def get_default_target_date() -> str:
    """Return yesterday's UTC date as YYYY-MM-DD for complete 24h T-1 batch ingestion."""
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def run_daily_ingestion(
    target_date: Optional[str] = None,
    zones: Optional[List[str]] = None,
    dry_run: bool = False,
    output_dir: Optional[Path] = None,
    allow_mock_weather: bool = False,
) -> int:
    """
    Execute daily batch ingestion for all configured zones and stations.

    Returns:
        0 on success, 1 on error.
    """
    start_time = time.time()
    batch_id = generate_batch_id()
    date_str = target_date or get_default_target_date()
    target_zones = [z.upper() for z in (zones or PRICE_ZONES)]

    logger.info("=" * 65)
    logger.info("STARTING DAILY BRONZE INGESTION PIPELINE")
    logger.info(f"Run Batch ID:  {batch_id}")
    logger.info(f"Target Date:   {date_str} (UTC)")
    logger.info(f"Target Zones:  {', '.join(target_zones)}")
    logger.info(f"Dry Run Mode:  {dry_run}")
    logger.info("=" * 65)

    writer = BronzeWriter(bronze_dir=output_dir or BRONZE_DIR)
    strom_client = StromprisClient()
    frost_client = FrostClient()

    zone_station_map = load_stations_config()

    # Track metrics
    metrics = {
        "batch_id": batch_id,
        "date": date_str,
        "electricity_records": 0,
        "electricity_files": 0,
        "weather_records": 0,
        "weather_files": 0,
        "errors": [],
    }

    # 1. Ingest Electricity Spot Prices
    logger.info(">>> Extracting Electricity Spot Prices (Strømpris API)...")
    for zone in target_zones:
        try:
            res = strom_client.fetch_prices(date_str=date_str, zone=zone)
            records = res["records"]
            source_uri = res["source_uri"]
            metrics["electricity_records"] += len(records)

            if not dry_run:
                saved_path = writer.write_electricity_prices(
                    zone=zone,
                    date_str=date_str,
                    payload=records,
                    source_uri=source_uri,
                    batch_id=batch_id,
                    overwrite=True,
                )
                metrics["electricity_files"] += 1
                logger.info(f"[OK] [{zone}] Ingested {len(records)} hourly prices -> {saved_path}")
            else:
                logger.info(f"[OK] [DRY RUN] [{zone}] Extracted {len(records)} hourly prices")
        except Exception as e:
            err_msg = f"Failed to ingest electricity prices for {zone}: {e}"
            logger.error(f"[FAIL] {err_msg}")
            metrics["errors"].append(err_msg)

    # 2. Ingest Meteorological Observations
    logger.info(">>> Extracting Weather Observations (MET Norway Frost API)...")
    stations_to_query = set()
    for zone in target_zones:
        stations_to_query.update(zone_station_map.get(zone, []))

    for station_id in sorted(stations_to_query):
        try:
            res = frost_client.fetch_observations(
                date_str=date_str,
                station_id=station_id,
                elements=["air_temperature", "wind_speed"],
                allow_mock_fallback=allow_mock_weather or not bool(FROST_CLIENT_ID),
            )
            records = res["records"]
            source_uri = res["source_uri"]
            metrics["weather_records"] += len(records)

            if not dry_run:
                saved_path = writer.write_weather_observations(
                    station_id=station_id,
                    date_str=date_str,
                    payload=records,
                    source_uri=source_uri,
                    batch_id=batch_id,
                    overwrite=True,
                )
                metrics["weather_files"] += 1
                logger.info(f"[OK] [{station_id}] Ingested {len(records)} observations -> {saved_path}")
            else:
                logger.info(f"[OK] [DRY RUN] [{station_id}] Extracted {len(records)} observations")
        except Exception as e:
            err_msg = f"Failed to ingest weather observations for station {station_id}: {e}"
            logger.error(f"[FAIL] {err_msg}")
            metrics["errors"].append(err_msg)

    elapsed = time.time() - start_time

    # 3. Execution Summary Report
    logger.info("=" * 65)
    logger.info("DAILY BRONZE INGESTION SUMMARY")
    logger.info("=" * 65)
    logger.info(f"Status:               {'SUCCESS' if not metrics['errors'] else 'COMPLETED WITH ERRORS'}")
    logger.info(f"Execution Time:       {elapsed:.2f}s")
    logger.info(f"Batch Identifier:     {batch_id}")
    logger.info(f"Electricity Records:  {metrics['electricity_records']} across {metrics['electricity_files']} files")
    logger.info(f"Weather Observations: {metrics['weather_records']} across {metrics['weather_files']} files")
    if metrics["errors"]:
        logger.error(f"Total Errors ({len(metrics['errors'])}):")
        for err in metrics["errors"]:
            logger.error(f"  - {err}")
    logger.info("=" * 65)

    return 0 if not metrics["errors"] else 1


def main() -> int:
    """CLI Argument Parser."""
    parser = argparse.ArgumentParser(
        description="DAT535 Daily Bronze Layer Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to ingest in YYYY-MM-DD format (defaults to yesterday UTC)",
    )
    parser.add_argument(
        "--zones",
        nargs="+",
        choices=PRICE_ZONES,
        default=PRICE_ZONES,
        help="List of price zones to ingest (default: all NO1-NO5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform extract and validation without saving files to Bronze storage",
    )
    parser.add_argument(
        "--allow-mock-weather",
        action="store_true",
        help="Use synthetic weather data if Frost credentials are unavailable",
    )

    args = parser.parse_args()
    return run_daily_ingestion(
        target_date=args.date,
        zones=args.zones,
        dry_run=args.dry_run,
        allow_mock_weather=args.allow_mock_weather,
    )


if __name__ == "__main__":
    sys.exit(main())
