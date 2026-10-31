"""
Gold layer entrypoint: Silver -> the tables the dashboard and other consumers read.

Every run rebuilds all Gold tables from the newest successful Silver (docs/plans/gold_layer.md):
  1. find the newest Silver run manifest with status SUCCESS; without one there is nothing to build
  2. read silver/zone_hourly and silver/dim_date and build the use case tables
  3. run the checks; a failed hard check writes nothing (exit code 1), so the dashboard keeps showing
     the previous Gold
  4. write every table to gold/_staging/ and swap them all into place (as Silver does)
  5. write gold/_status/latest.json, which tells the dashboard how fresh Gold is

Tables land in <data root>/<env>/gold/:
  use case 2 (dashboard): zone_daily, price_weather_sensitivity, price_profile
  status: _status/latest.json (newest successful run), _status/runs/run__<run_id>.json (every run)

Usage:
    python src/gold/run_gold.py
    python src/gold/run_gold.py --silver-dir /tmp/silver_clean --output-dir /tmp/gold_clean
"""

import argparse
from functools import reduce
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Dict, List, Optional

# Ensure project root is on sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from config.settings import DAT535_ENV, GOLD_DIR, PRICE_ZONES, SILVER_DIR  # noqa: E402
from src.gold.dashboard import (  # noqa: E402
    build_price_profile,
    build_price_weather_sensitivity,
    build_zone_daily,
)
from src.ingestion.bronze_writer import BronzeWriter  # noqa: E402
from src.silver.quality import Check, check_dicts  # noqa: E402
from src.silver.run_silver import STAGING_DIR, publish, write_table  # noqa: E402
from src.silver.spark_session import get_spark  # noqa: E402
from src.utils.hashing import generate_batch_id, get_utc_now_iso  # noqa: E402

logger = logging.getLogger("run_gold")

EXIT_OK = 0
EXIT_CHECKS_FAILED = 1
EXIT_CONFIG_ERROR = 2

# Key columns per table. Gold tables are small (thousands of rows), so each is one Parquet file.
TABLE_KEYS = {
    "zone_daily": ["bidding_zone", "local_date"],
    "price_weather_sensitivity": ["bidding_zone", "year", "month"],
    "price_profile": ["bidding_zone", "year", "season", "day_type", "local_hour"],
}

# Which tables each use case reads; the dashboard's registry names the same tables.
USE_CASE_TABLES = {
    "prices_weather": ["zone_daily", "price_weather_sensitivity", "price_profile"],
}

STATUS_DIR = "_status"
LATEST_STATUS = "latest.json"


def latest_silver_run(silver_dir: Path) -> Optional[Dict]:
    """The newest Silver run manifest with status SUCCESS, or None."""
    manifests = []
    for path in sorted((Path(silver_dir) / "_runs").glob("run__*.json")):
        try:
            manifests.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            logger.warning(f"Unreadable Silver run manifest skipped: {path}")
    successful = [m for m in manifests if m.get("status") == "SUCCESS"]
    return max(successful, key=lambda m: m.get("finished_at_utc", ""), default=None)


def run_checks(tables: Dict[str, DataFrame], silver_price_hours: int) -> List[Check]:
    """Hard checks on the finished tables, before anything is written."""
    checks = []
    for name, keys in TABLE_KEYS.items():
        df = tables[name]
        duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
        null_keys = df.filter(reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])).count()
        checks += [
            Check(f"{name}.duplicate_keys", True, duplicates == 0, float(duplicates), f"keys sharing ({', '.join(keys)})"),
            Check(f"{name}.null_keys", True, null_keys == 0, float(null_keys), "rows with an empty key"),
        ]

    daily = tables["zone_daily"]
    rows = daily.count()
    checks.append(Check("zone_daily.not_empty", True, rows > 0, float(rows), "rows in zone_daily"))

    day_length = F.col("hours_in_day")
    too_long = daily.filter(
        day_length.isNull()
        | ~day_length.isin(23, 24, 25)
        | (F.col("price_hours") > day_length)
        | (F.col("weather_hours") > day_length)
    ).count()
    checks.append(Check("zone_daily.hours_within_day", True, too_long == 0, float(too_long),
                        "days with more hours than the day has, or no day length"))

    gold_price_hours = daily.agg(F.sum("price_hours")).first()[0] or 0
    checks.append(Check("zone_daily.price_hours_match_silver", True, gold_price_hours == silver_price_hours,
                        float(gold_price_hours - silver_price_hours),
                        f"Gold counts {gold_price_hours} price hours, Silver {silver_price_hours}"))

    sensitivity = tables["price_weather_sensitivity"]
    bad_corr = sensitivity.filter(
        ~F.col("corr_temp_price").between(-1, 1) | ~F.col("corr_wind_price").between(-1, 1)
    ).count()
    checks.append(Check("price_weather_sensitivity.correlation_range", True, bad_corr == 0, float(bad_corr),
                        "correlations outside [-1, 1]"))
    return checks


def freshness(zone_daily: DataFrame) -> Dict:
    """Newest complete day per zone. data_through is the newest day complete in every zone."""
    def newest(condition) -> Dict[str, str]:
        return {
            r.bidding_zone: r.newest.isoformat()
            for r in zone_daily.filter(condition).groupBy("bidding_zone")
            .agg(F.max("local_date").alias("newest")).orderBy("bidding_zone").collect()
        }

    complete = newest(F.col("is_complete"))
    prices = newest(F.col("price_hours") == F.col("hours_in_day"))
    return {
        "complete_day_per_zone": complete,
        "price_day_per_zone": prices,
        "data_through": min(complete.values()) if len(complete) == len(PRICE_ZONES) else None,
        "prices_through": min(prices.values()) if len(prices) == len(PRICE_ZONES) else None,
    }


def write_status(output_dir: Path, status: Dict, latest: bool) -> Path:
    """Every run gets _status/runs/run__<id>.json; a successful one also replaces _status/latest.json."""
    status_dir = Path(output_dir) / STATUS_DIR
    path = status_dir / "runs" / f"run__{status['run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    BronzeWriter._replace_json(path, status)
    if latest:
        BronzeWriter._replace_json(status_dir / LATEST_STATUS, status)
    return path


def step_summary(status: Dict) -> str:
    """Markdown for the GitHub Actions step summary."""
    fresh = status.get("freshness") or {}
    lines = [
        f"## Gold Layer: {status['status']}",
        "",
        "| Parameter | Value |",
        "|---|---|",
        f"| Environment | {status['environment']} |",
        f"| Silver run | {status['silver']['run_id']} ({status['silver']['finished_at_utc']}) |",
        f"| Elapsed | {status['elapsed_seconds']} s |",
        f"| Data through (all zones complete) | {fresh.get('data_through') or '–'} |",
        f"| Prices through | {fresh.get('prices_through') or '–'} |",
    ]
    lines += [f"| Rows: {name} | {t['rows']} |" for name, t in status["tables"].items()]
    lines += ["", "| Check | Passed | Value | Detail |", "|---|---|---|---|"]
    lines += [f"| {c['name']} | {'✅' if c['passed'] else '❌'} | {c['value']:g} | {c['detail']} |" for c in status["checks"]]
    return "\n".join(lines) + "\n"


def run_gold(
    silver_dir: Path,
    output_dir: Path,
    spark: Optional[SparkSession] = None,
) -> int:
    """
    Rebuild every Gold table from the Silver tables in `silver_dir` into `output_dir`.

    Returns:
        0 when the tables were written, 1 when a hard check failed (nothing written),
        2 when there is no successful Silver run to build from.
    """
    start_time = time.time()
    run_id = generate_batch_id()
    started_at = get_utc_now_iso()
    silver_dir, output_dir = Path(silver_dir), Path(output_dir)

    silver_run = latest_silver_run(silver_dir)
    if silver_run is None or not (silver_dir / "zone_hourly").is_dir():
        logger.error(f"No successful Silver run in {silver_dir}: run src/silver/run_silver.py first.")
        return EXIT_CONFIG_ERROR

    spark = spark or get_spark(app_name="dat535-gold")
    spark.catalog.clearCache()  # a reused session must not return an earlier run's cached Silver

    logger.info("=" * 65)
    logger.info("STARTING GOLD LAYER")
    logger.info(f"Environment:   {DAT535_ENV}")
    logger.info(f"Run ID:        {run_id}")
    logger.info(f"Silver:        {silver_dir} (run {silver_run['run_id']}, {silver_run.get('finished_at_utc')})")
    logger.info(f"Output:        {output_dir}")
    logger.info("=" * 65)

    # 1-2. Read Silver, build the use case tables.
    zone_hourly = spark.read.parquet(str(silver_dir / "zone_hourly")).cache()
    dim_date = spark.read.parquet(str(silver_dir / "dim_date"))
    zone_daily = build_zone_daily(zone_hourly, dim_date).cache()
    tables = {
        "zone_daily": zone_daily,
        "price_weather_sensitivity": build_price_weather_sensitivity(zone_hourly, zone_daily).cache(),
        "price_profile": build_price_profile(zone_hourly).cache(),
    }

    # 3. Checks, before anything is written.
    checks = run_checks(tables, silver_price_hours=zone_hourly.filter(F.col("has_price")).count())
    failed = [c for c in checks if c.hard and not c.passed]

    bounds = zone_daily.agg(F.min("local_date").alias("first"), F.max("local_date").alias("last")).first()
    status = {
        "run_id": run_id,
        "status": "FAILED" if failed else "SUCCESS",
        "environment": DAT535_ENV,
        "started_at_utc": started_at,
        "silver": {
            "run_id": silver_run["run_id"],
            "finished_at_utc": silver_run.get("finished_at_utc"),
            "input": silver_run.get("input"),
        },
        "tables": {name: {"rows": df.count()} for name, df in tables.items()},
        "first_date": bounds["first"].isoformat() if bounds["first"] else None,
        "last_date": bounds["last"].isoformat() if bounds["last"] else None,
        "freshness": freshness(zone_daily),
        "use_cases": USE_CASE_TABLES,
        "checks": check_dicts(checks),
        "is_sample": False,
    }

    # 4. Write only when every hard check passed.
    if failed:
        for c in failed:
            logger.error(f"[CHECK FAILED] {c.name}: {c.value:g} ({c.detail})")
        logger.error("Hard checks failed: nothing written, the dashboard keeps the previous Gold.")
    else:
        staging = output_dir / STAGING_DIR / run_id
        shutil.rmtree(output_dir / STAGING_DIR, ignore_errors=True)  # leftovers of a crashed run
        for name, df in tables.items():
            write_table(df, staging / name, [])
        publish(staging, output_dir, list(tables))

    # 5. The status file is written last, so it never points at tables that aren't there yet.
    status["built_at_utc"] = get_utc_now_iso()
    status["elapsed_seconds"] = round(time.time() - start_time, 2)
    path = write_status(output_dir, status, latest=not failed)

    logger.info("=" * 65)
    logger.info("GOLD LAYER SUMMARY")
    logger.info("=" * 65)
    logger.info(f"Status:          {status['status']}")
    logger.info(f"Execution Time:  {status['elapsed_seconds']:.2f}s")
    for name, t in status["tables"].items():
        logger.info(f"Rows {name + ':':<27} {t['rows']}")
    logger.info(f"Data through:    {status['freshness']['data_through']}")
    logger.info(f"Status file:     {path}")
    logger.info("=" * 65)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(step_summary(status))

    return EXIT_CHECKS_FAILED if failed else EXIT_OK


def main() -> int:
    """CLI Argument Parser."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="DAT535 Gold layer: Silver -> dashboard tables and the Gold status file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR, help=f"Silver input (default {SILVER_DIR})")
    parser.add_argument("--output-dir", type=Path, default=GOLD_DIR, help=f"Gold output (default {GOLD_DIR})")
    args = parser.parse_args()
    return run_gold(args.silver_dir, args.output_dir)


if __name__ == "__main__":
    sys.exit(main())
