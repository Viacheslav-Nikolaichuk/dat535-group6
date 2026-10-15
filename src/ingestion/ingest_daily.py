"""
Daily Batch Ingestion Pipeline Entrypoint.
Extracts daily electricity spot prices (NO1-NO5) and meteorological observations
from MET Norway, envelopes them with audit metadata, and persists to Bronze storage.

Writes are append-only and checksum-deduplicated (see bronze_writer.py), so any date can be
re-ingested safely: unchanged payloads are skipped, revised payloads become new versions.

Usage:
    python src/ingestion/ingest_daily.py                                   # Ingests yesterday (UTC)
    python src/ingestion/ingest_daily.py --date 2024-01-15                 # Ingests one historical date
    python src/ingestion/ingest_daily.py --lookback-days 3                 # Re-checks the last 3 days (catch-up)
    python src/ingestion/ingest_daily.py --start-date 2023-01-01 --end-date 2023-12-31 --request-delay 0.5
    python src/ingestion/ingest_daily.py --dry-run                         # Tests API without writing to disk
"""

import argparse
from datetime import date, datetime, timedelta, timezone
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
    PRICE_ZONES,
    ZONES_CONFIG_FILE,
)
from src.ingestion.bronze_writer import (
    STATUS_CREATED,
    STATUS_NEW_VERSION,
    STATUS_UNCHANGED,
    BronzeWriter,
)
from src.ingestion.frost_client import FrostClient
from src.ingestion.strompris_client import StromprisClient
from src.utils.hashing import generate_batch_id, get_utc_now_iso

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_daily")

WEATHER_ELEMENTS = ["air_temperature", "wind_speed"]

EXIT_OK = 0
EXIT_PARTIAL_FAILURE = 1
EXIT_CONFIG_ERROR = 2


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


# ---------------------------------------------------------------------- #
# Date resolution
# ---------------------------------------------------------------------- #
def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def get_default_target_date() -> str:
    """Return yesterday's UTC date as YYYY-MM-DD for complete 24h T-1 batch ingestion."""
    return (_utc_today() - timedelta(days=1)).strftime("%Y-%m-%d")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def resolve_target_dates(
    target_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: Optional[int] = None,
    today: Optional[date] = None,
) -> List[str]:
    """
    Resolve CLI date options into an ordered list of YYYY-MM-DD strings (oldest first).

    Exactly one mode applies:
      * target_date            -> [target_date]
      * start_date[/end_date]  -> inclusive range; end defaults to yesterday
      * lookback_days N        -> the N complete days ending yesterday
      * nothing                -> [yesterday]

    Only complete past days are allowed (<= yesterday UTC), because weather observations
    for the current day are still incomplete.
    """
    today = today or _utc_today()
    yesterday = today - timedelta(days=1)

    modes = sum(x is not None for x in (target_date, start_date or end_date, lookback_days))
    if modes > 1:
        raise ValueError("Use only one of: --date, --start-date/--end-date, --lookback-days.")

    if target_date is not None:
        start = end = _parse_date(target_date)
    elif start_date is not None or end_date is not None:
        if start_date is None:
            raise ValueError("--end-date requires --start-date.")
        start = _parse_date(start_date)
        end = _parse_date(end_date) if end_date else yesterday
    elif lookback_days is not None:
        if lookback_days < 1:
            raise ValueError("--lookback-days must be >= 1.")
        start, end = yesterday - timedelta(days=lookback_days - 1), yesterday
    else:
        start = end = yesterday

    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}.")
    if end > yesterday:
        raise ValueError(
            f"Date {end} is not a complete past day (latest allowed: {yesterday} UTC)."
        )

    n_days = (end - start).days + 1
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days)]


# ---------------------------------------------------------------------- #
# Pipeline
# ---------------------------------------------------------------------- #
def _new_metrics(batch_id: str, dates: List[str]) -> Dict:
    return {
        "batch_id": batch_id,
        "date_from": dates[0],
        "date_to": dates[-1],
        "days": len(dates),
        "electricity_records": 0,
        "weather_records": 0,
        "files": {STATUS_CREATED: 0, STATUS_NEW_VERSION: 0, STATUS_UNCHANGED: 0},
        "synthetic_files": 0,
        "empty_payloads": [],
        "errors": [],
    }


def _record_write(metrics: Dict, result, label: str, n_records: int) -> None:
    metrics["files"][result.status] += 1
    logger.info(f"[OK] {label} {n_records} records -> {result.status}: {result.path.name}")


def run_ingestion(
    dates: List[str],
    zones: Optional[List[str]] = None,
    dry_run: bool = False,
    output_dir: Optional[Path] = None,
    allow_mock_weather: bool = False,
    request_delay: float = 0.0,
    strom_client: Optional[StromprisClient] = None,
    frost_client: Optional[FrostClient] = None,
) -> int:
    """
    Ingest electricity prices and weather observations for each date in `dates`.

    Returns:
        0 on success, 1 if any zone/station/date failed, 2 on configuration error.
    """
    start_time = time.time()
    batch_id = generate_batch_id()
    target_zones = [z.upper() for z in (zones or PRICE_ZONES)]

    writer = BronzeWriter(bronze_dir=output_dir or BRONZE_DIR)
    strom_client = strom_client or StromprisClient()
    frost_client = frost_client or FrostClient()

    zone_station_map = load_stations_config()
    stations = sorted({s for z in target_zones for s in zone_station_map.get(z, [])})

    logger.info("=" * 65)
    logger.info("STARTING BRONZE INGESTION PIPELINE")
    logger.info(f"Run Batch ID:  {batch_id}")
    logger.info(f"Date Range:    {dates[0]} -> {dates[-1]} ({len(dates)} day(s), UTC)")
    logger.info(f"Target Zones:  {', '.join(target_zones)}")
    logger.info(f"Stations:      {', '.join(stations)}")
    logger.info(f"Dry Run Mode:  {dry_run}")
    logger.info(f"Bronze Dir:    {writer.bronze_dir}")
    logger.info("=" * 65)

    # Fail fast on missing credentials instead of silently producing synthetic data.
    if not frost_client.client_id and not allow_mock_weather:
        logger.error(
            "FROST_CLIENT_ID is not configured. Refusing to run: weather data cannot be "
            "ingested. Set the secret/env var, or pass --allow-mock-weather for offline "
            "testing (synthetic data will be flagged with _is_synthetic=true)."
        )
        return EXIT_CONFIG_ERROR
    if allow_mock_weather and not frost_client.client_id:
        logger.warning("!!! Mock weather enabled: synthetic observations will be written to Bronze !!!")

    metrics = _new_metrics(batch_id, dates)

    for date_str in dates:
        logger.info(f"----- {date_str} -----")

        # 1. Electricity spot prices
        for zone in target_zones:
            try:
                res = strom_client.fetch_prices(date_str=date_str, zone=zone)
                records = res["records"]
                metrics["electricity_records"] += len(records)
                if not records:
                    metrics["empty_payloads"].append(f"electricity {zone} {date_str}")
                    logger.warning(f"[EMPTY] [{zone}] {date_str}: API returned 0 price records")

                if dry_run:
                    logger.info(f"[OK] [DRY RUN] [{zone}] Extracted {len(records)} prices")
                else:
                    result = writer.write_electricity_prices(
                        zone=zone,
                        date_str=date_str,
                        payload=records,
                        source_uri=res["source_uri"],
                        batch_id=batch_id,
                    )
                    _record_write(metrics, result, f"[{zone}]", len(records))
            except Exception as e:
                err_msg = f"electricity {zone} {date_str}: {e}"
                logger.error(f"[FAIL] {err_msg}")
                metrics["errors"].append(err_msg)
            time.sleep(request_delay)

        # 2. Meteorological observations
        for station_id in stations:
            try:
                res = frost_client.fetch_observations(
                    date_str=date_str,
                    station_id=station_id,
                    elements=WEATHER_ELEMENTS,
                    allow_mock_fallback=allow_mock_weather,
                )
                records = res["records"]
                is_synthetic = bool(res.get("is_synthetic", False))
                metrics["weather_records"] += len(records)
                if not records:
                    metrics["empty_payloads"].append(f"weather {station_id} {date_str}")
                    logger.warning(f"[EMPTY] [{station_id}] {date_str}: API returned 0 observations")

                if dry_run:
                    logger.info(f"[OK] [DRY RUN] [{station_id}] Extracted {len(records)} observations")
                else:
                    result = writer.write_weather_observations(
                        station_id=station_id,
                        date_str=date_str,
                        payload=records,
                        source_uri=res["source_uri"],
                        batch_id=batch_id,
                        is_synthetic=is_synthetic,
                    )
                    if is_synthetic and result.written:
                        metrics["synthetic_files"] += 1
                    _record_write(metrics, result, f"[{station_id}]", len(records))
            except Exception as e:
                err_msg = f"weather {station_id} {date_str}: {e}"
                logger.error(f"[FAIL] {err_msg}")
                metrics["errors"].append(err_msg)
            time.sleep(request_delay)

    elapsed = time.time() - start_time

    # 3. Execution Summary Report
    files = metrics["files"]
    logger.info("=" * 65)
    logger.info("BRONZE INGESTION SUMMARY")
    logger.info("=" * 65)
    logger.info(f"Status:               {'SUCCESS' if not metrics['errors'] else 'COMPLETED WITH ERRORS'}")
    logger.info(f"Execution Time:       {elapsed:.2f}s")
    logger.info(f"Batch Identifier:     {batch_id}")
    logger.info(f"Days Processed:       {len(dates)} ({dates[0]} -> {dates[-1]})")
    logger.info(f"Electricity Records:  {metrics['electricity_records']}")
    logger.info(f"Weather Observations: {metrics['weather_records']}")
    logger.info(
        f"Files:                {files[STATUS_CREATED]} created, "
        f"{files[STATUS_NEW_VERSION]} new versions, {files[STATUS_UNCHANGED]} unchanged"
    )
    if metrics["synthetic_files"]:
        logger.warning(f"Synthetic Files:      {metrics['synthetic_files']} (mock weather)")
    if metrics["empty_payloads"]:
        logger.warning(f"Empty Payloads ({len(metrics['empty_payloads'])}):")
        for item in metrics["empty_payloads"]:
            logger.warning(f"  - {item}")
    if metrics["errors"]:
        logger.error(f"Total Errors ({len(metrics['errors'])}):")
        for err in metrics["errors"]:
            logger.error(f"  - {err}")
    logger.info("=" * 65)

    # 4. Persist run manifest if not in dry-run mode
    if not dry_run:
        manifest_payload = {
            "batch_id": batch_id,
            "created_at_utc": get_utc_now_iso(),
            "status": "SUCCESS" if not metrics["errors"] else "COMPLETED_WITH_ERRORS",
            "elapsed_seconds": round(elapsed, 2),
            "date_from": dates[0],
            "date_to": dates[-1],
            "days_count": len(dates),
            "electricity_records": metrics["electricity_records"],
            "weather_records": metrics["weather_records"],
            "files": files,
            "synthetic_files": metrics["synthetic_files"],
            "empty_payloads": metrics["empty_payloads"],
            "errors": metrics["errors"],
        }
        try:
            writer.write_manifest(manifest_payload)
        except Exception as e:
            logger.error(f"Failed to write run manifest: {e}")

    return EXIT_OK if not metrics["errors"] else EXIT_PARTIAL_FAILURE


def run_daily_ingestion(
    target_date: Optional[str] = None,
    zones: Optional[List[str]] = None,
    dry_run: bool = False,
    output_dir: Optional[Path] = None,
    allow_mock_weather: bool = False,
) -> int:
    """Backward-compatible single-date wrapper around run_ingestion()."""
    return run_ingestion(
        dates=[target_date or get_default_target_date()],
        zones=zones,
        dry_run=dry_run,
        output_dir=output_dir,
        allow_mock_weather=allow_mock_weather,
    )


def _date_arg(value: str) -> str:
    try:
        _parse_date(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date '{value}', expected YYYY-MM-DD")
    return value


def main() -> int:
    """CLI Argument Parser."""
    parser = argparse.ArgumentParser(
        description="DAT535 Bronze Layer Ingestion Pipeline (daily, catch-up, and backfill)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date",
        type=_date_arg,
        default=None,
        help="Single date to ingest in YYYY-MM-DD format (default: yesterday UTC)",
    )
    parser.add_argument("--start-date", type=_date_arg, default=None, help="Backfill range start (inclusive)")
    parser.add_argument(
        "--end-date", type=_date_arg, default=None, help="Backfill range end (inclusive, default: yesterday UTC)"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Re-check the N complete days ending yesterday (catches missed runs and late revisions)",
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
        help="Use synthetic weather data if Frost credentials are unavailable (flagged _is_synthetic)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.0,
        help="Seconds to sleep between API requests (use e.g. 0.5 for large backfills)",
    )

    args = parser.parse_args()

    try:
        dates = resolve_target_dates(
            target_date=args.date,
            start_date=args.start_date,
            end_date=args.end_date,
            lookback_days=args.lookback_days,
        )
    except ValueError as e:
        parser.error(str(e))

    return run_ingestion(
        dates=dates,
        zones=args.zones,
        dry_run=args.dry_run,
        allow_mock_weather=args.allow_mock_weather,
        request_delay=args.request_delay,
    )


if __name__ == "__main__":
    sys.exit(main())
