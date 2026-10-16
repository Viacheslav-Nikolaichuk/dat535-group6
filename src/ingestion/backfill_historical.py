"""
Historical Bronze backfill, kept separate from the daily pipeline.
Thin wrapper around `ingest_daily.run_ingestion()` (unchanged) that writes into the same Bronze
tree but never touches the days the daily job re-checks.

* The range is processed one calendar month at a time: each month is its own run with its own
  batch ID and manifest, so a crash or timeout loses at most the current month's manifest.
* Months that already have a SUCCESS run manifest are skipped, so rerunning the same range
  resumes where it stopped. Failed months are rerun; unchanged days are not rewritten (checksum).
* End dates inside the daily pipeline's lookback window are refused.

Usage:
    python src/ingestion/backfill_historical.py --start-date 2022-01-01 --end-date 2022-12-31
"""

import argparse
from datetime import date, timedelta
from itertools import groupby
import json
import logging
from pathlib import Path
import sys
from typing import List, Optional, Set, Tuple

# Ensure project root is on sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import BRONZE_DIR
from src.ingestion.frost_client import FrostClient
from src.ingestion.ingest_daily import (
    EXIT_CONFIG_ERROR,
    EXIT_OK,
    _date_arg,
    _utc_today,
    resolve_target_dates,
    run_ingestion,
)
from src.ingestion.strompris_client import StromprisClient

logger = logging.getLogger("backfill_historical")

# Must match the default `lookback_days` of the daily pipeline (.github/workflows/pipeline.yml).
DAILY_LOOKBACK_DAYS = 3


def split_by_month(dates: List[str]) -> List[List[str]]:
    """Group ordered YYYY-MM-DD strings into calendar months."""
    return [list(month) for _, month in groupby(dates, key=lambda d: d[:7])]


def completed_ranges(bronze_dir: Path) -> Set[Tuple[str, str]]:
    """(date_from, date_to) of every run manifest with status SUCCESS."""
    done = set()
    for path in (Path(bronze_dir) / "_manifests").glob("run__*.json"):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("status") == "SUCCESS":
            done.add((manifest.get("date_from"), manifest.get("date_to")))
    return done


def run_backfill(
    start_date: str,
    end_date: str,
    request_delay: float = 0.5,
    output_dir: Optional[Path] = None,
    strom_client: Optional[StromprisClient] = None,
    frost_client: Optional[FrostClient] = None,
    today: Optional[date] = None,
) -> int:
    """
    Backfill [start_date, end_date] month by month for all zones and stations.

    Returns:
        0 on success, 1 if any month had failures, 2 on configuration error (stops immediately).
    Raises:
        ValueError: on an invalid range or an end date inside the daily lookback window.
    """
    # Shifting "today" back makes resolve_target_dates reject the daily lookback window.
    cutoff_today = (today or _utc_today()) - timedelta(days=DAILY_LOOKBACK_DAYS)
    dates = resolve_target_dates(start_date=start_date, end_date=end_date, today=cutoff_today)
    bronze_dir = Path(output_dir or BRONZE_DIR)

    done = completed_ranges(bronze_dir)
    months = split_by_month(dates)
    logger.info(f"Historical backfill {dates[0]} -> {dates[-1]}: {len(dates)} day(s) in {len(months)} month(s)")

    worst = EXIT_OK
    for month in months:
        if (month[0], month[-1]) in done:
            logger.info(f"[SKIP] {month[0]} -> {month[-1]}: already completed")
            continue
        rc = run_ingestion(
            dates=month,
            output_dir=bronze_dir,
            request_delay=request_delay,
            strom_client=strom_client,
            frost_client=frost_client,
        )
        if rc == EXIT_CONFIG_ERROR:
            return rc
        worst = max(worst, rc)

    logger.info(f"Historical backfill finished with exit code {worst}")
    return worst


def main() -> int:
    """CLI Argument Parser."""
    parser = argparse.ArgumentParser(
        description="DAT535 historical Bronze backfill (separate from the daily pipeline)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start-date", type=_date_arg, required=True, help="First date (inclusive), YYYY-MM-DD")
    parser.add_argument(
        "--end-date",
        type=_date_arg,
        required=True,
        help=f"Last date (inclusive); must be before the daily {DAILY_LOOKBACK_DAYS}-day lookback window",
    )
    parser.add_argument(
        "--request-delay", type=float, default=0.5, help="Seconds to sleep between API requests (default 0.5)"
    )
    args = parser.parse_args()

    try:
        return run_backfill(args.start_date, args.end_date, request_delay=args.request_delay)
    except ValueError as e:
        parser.error(str(e))


if __name__ == "__main__":
    sys.exit(main())
