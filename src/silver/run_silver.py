"""
Silver layer entrypoint: Bronze (by default its chaos copy) -> typed, deduplicated UTC tables.

Every run rebuilds all Silver tables from the whole input (docs/plans/silver_layer.md, D3):
  1. read the Bronze files and choose one version per zone/station and day
  2. clean prices and weather; every problem goes to the issue log
  3. build zone_hourly and the reference tables
  4. run the checks; a failed hard check writes nothing (exit code 1), so Gold keeps reading
     the previous Silver
  5. write every table to silver/_staging/, swap them all into place, append the scorecard to
     dq_metrics, and write the run manifest last

Tables land in <data root>/<env>/silver/: prices, weather_hourly, zone_hourly, dim_date,
dim_station, dq_issues, dq_metrics, and _runs/run__<run_id>.json.

Usage:
    python src/silver/run_silver.py                                          # SILVER_INPUT (bronze_chaos)
    python src/silver/run_silver.py --input bronze --output-dir /tmp/silver_clean   # clean baseline
"""

import argparse
from datetime import date, datetime, timezone
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

from config.settings import (  # noqa: E402
    BRONZE_CHAOS_DIR,
    BRONZE_DIR,
    DAT535_ENV,
    HISTORY_START,
    PRICE_ZONES,
    SILVER_DIR,
    SILVER_INPUT,
    SILVER_INPUTS,
)
from src.silver import prices as price_rules  # noqa: E402
from src.silver import weather as weather_rules  # noqa: E402
from src.silver.bronze_reader import (  # noqa: E402
    DOMAIN_PRICES,
    DOMAIN_WEATHER,
    STATUS_SYNTHETIC,
    explode_records,
    file_issues,
    read_bronze_files,
)
from src.silver.quality import (  # noqa: E402
    ISSUE_COLUMNS,
    chaos_score,
    check_dicts,
    load_chaos_events,
    run_checks,
    scorecard,
    step_summary,
    write_run_manifest,
)
from src.silver.reference import build_dim_date, build_dim_station  # noqa: E402
from src.silver.spark_session import get_spark  # noqa: E402
from src.silver.zone_hourly import build_zone_hourly  # noqa: E402
from src.utils.hashing import generate_batch_id, get_utc_now_iso  # noqa: E402

logger = logging.getLogger("run_silver")

EXIT_OK = 0
EXIT_CHECKS_FAILED = 1
EXIT_CONFIG_ERROR = 2

INPUT_DIRS = {"bronze_chaos": BRONZE_CHAOS_DIR, "bronze": BRONZE_DIR}

# Partition columns per table. Monthly partitions are the rebuild unit (D2); small tables are one file.
TABLE_PARTITIONS = {
    "prices": ["year", "month"],
    "weather_hourly": ["year", "month"],
    "zone_hourly": ["year", "month"],
    "dim_date": [],
    "dim_station": [],
    "dq_issues": [],  # small: thousands of rows even on the chaos copy
}

# A run writes every table here first and swaps them into place only when all writes succeeded.
STAGING_DIR = "_staging"
PREVIOUS_DIR = "_previous"


def write_table(df: DataFrame, path: Path, partitions: List[str]) -> None:
    """Replace a Silver table. One file per partition: Gold reads few, larger files.

    A full rebuild replaces the whole table (static overwrite); the session's dynamic mode is
    kept for partial rebuilds of single months. An empty table is written as one empty file
    without partitions, so it keeps its schema and stays readable (a partitioned empty write
    leaves only a _SUCCESS marker).
    """
    if partitions and df.isEmpty():
        partitions = []
    df = df.repartition(*partitions) if partitions else df.coalesce(1)
    writer = df.write.mode("overwrite").option("partitionOverwriteMode", "static")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(str(path))


def publish(staging: Path, output_dir: Path, names: List[str]) -> None:
    """Swap the tables written to `staging` into place, after every one of them was written.

    A crash while Spark writes (disk full, a killed job) therefore leaves the live tables
    untouched. The swap itself is a few directory renames on one filesystem: milliseconds, no
    Spark. Old tables wait in _previous/ until every table has been swapped.
    """
    previous = output_dir / PREVIOUS_DIR
    shutil.rmtree(previous, ignore_errors=True)
    previous.mkdir(parents=True)
    for name in names:
        live = output_dir / name
        if live.exists():
            os.replace(live, previous / name)
        os.replace(staging / name, live)
    shutil.rmtree(previous)
    shutil.rmtree(staging.parent)


def _dim_date_range(prices: DataFrame, weather_hourly: DataFrame, today: date):
    """From the history start (or earlier data) to the end of next year."""
    bounds = prices.select("local_date").unionByName(weather_hourly.select("local_date")).agg(
        F.min("local_date").alias("first"), F.max("local_date").alias("last")
    ).first()
    start = min(date.fromisoformat(HISTORY_START), bounds["first"] or today)
    end = date(max(today, bounds["last"] or today).year + 1, 12, 31)
    return start, end


def _newest_hour_per_zone(df: DataFrame) -> Dict[str, str]:
    return {
        r.bidding_zone: r.newest
        for r in df.groupBy("bidding_zone")
        .agg(F.date_format(F.max("utc_hour"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("newest"))
        .orderBy("bidding_zone")
        .collect()
    }


def run_silver(
    input_dir: Path,
    output_dir: Path,
    input_name: str,
    allow_synthetic: bool = False,
    spark: Optional[SparkSession] = None,
    today: Optional[date] = None,
) -> int:
    """
    Rebuild every Silver table from `input_dir` into `output_dir`.

    Returns:
        0 when the tables were written, 1 when a hard check failed (nothing written).
    """
    start_time = time.time()
    run_id = generate_batch_id()
    started_at = get_utc_now_iso()
    today = today or datetime.now(timezone.utc).date()
    spark = spark or get_spark()
    # Spark matches cached data by query plan, not by file contents: in a reused session a second
    # run would silently get the first run's cached Bronze instead of the new files.
    spark.catalog.clearCache()
    input_dir, output_dir = Path(input_dir), Path(output_dir)

    logger.info("=" * 65)
    logger.info("STARTING SILVER LAYER")
    logger.info(f"Environment:   {DAT535_ENV}")
    logger.info(f"Run ID:        {run_id}")
    logger.info(f"Input:         {input_name} ({input_dir})")
    logger.info(f"Output:        {output_dir}")
    logger.info(f"Spark:         {spark.version}")
    logger.info("=" * 65)

    # 1-2. Read Bronze, clean both sources.
    dim_station = build_dim_station(spark).cache()
    price_files = read_bronze_files(spark, input_dir, DOMAIN_PRICES, allow_synthetic)
    weather_files = read_bronze_files(spark, input_dir, DOMAIN_WEATHER, allow_synthetic)
    prices, price_issues, price_checked = price_rules.clean_prices(explode_records(price_files))
    weather_hourly, weather_issues, weather_checked = weather_rules.clean_weather(
        explode_records(weather_files), dim_station
    )
    prices, weather_hourly = prices.cache(), weather_hourly.cache()
    price_checked, weather_checked = price_checked.cache(), weather_checked.cache()

    # 3. The integrated table and the calendar.
    dim_date = build_dim_date(spark, *_dim_date_range(prices, weather_hourly, today)).cache()
    zone_hourly = build_zone_hourly(prices, weather_hourly, dim_date, PRICE_ZONES).cache()

    detected_at = F.to_timestamp(F.lit(started_at), "yyyy-MM-dd'T'HH:mm:ssX")
    issues = (
        file_issues(price_files, price_rules.SOURCE)
        .unionByName(price_issues)
        .unionByName(file_issues(weather_files, weather_rules.SOURCE))
        .unionByName(weather_issues)
        .select(F.lit(run_id).alias("run_id"), *ISSUE_COLUMNS, detected_at.alias("detected_at_utc"))
        .cache()
    )

    # 4. Checks, scorecard and chaos score, all before anything is written.
    records_checked = {price_rules.SOURCE: price_checked.count(), weather_rules.SOURCE: weather_checked.count()}
    records_rejected = {
        price_rules.SOURCE: price_checked.filter(F.col("status") == price_rules.STATUS_REJECTED).count(),
        weather_rules.SOURCE: weather_checked.filter(F.col("status") == weather_rules.STATUS_REJECTED).count(),
    }
    checks = run_checks(
        prices, weather_hourly, zone_hourly, dim_date, PRICE_ZONES, issues, records_checked, records_rejected,
        nok_eur_mismatches=price_checked.filter(F.col("nok_eur_mismatch")).count(),
        price_bounds=(price_rules.PRICE_MIN_NOK, price_rules.PRICE_MAX_NOK),
        temp_bounds=(weather_rules.TEMP_MIN_C, weather_rules.TEMP_MAX_C),
        wind_bounds=(weather_rules.WIND_MIN_MPS, weather_rules.WIND_MAX_MPS),
    )
    events = load_chaos_events(input_dir)
    checked_files = (
        price_files.select("source_file", "status")
        .unionByName(weather_files.select("source_file", "status"))
        .filter(F.col("status") != STATUS_SYNTHETIC)
    )
    chaos = chaos_score(spark, events, issues, checked_files) if events is not None else None

    tables = {
        "prices": prices,
        "weather_hourly": weather_hourly,
        "zone_hourly": zone_hourly,
        "dim_date": dim_date,
        "dim_station": dim_station,
    }
    files = {price_rules.SOURCE: price_files, weather_rules.SOURCE: weather_files}
    metrics = scorecard(files, records_checked, issues, tables, checks, chaos)
    failed = [c for c in checks if c.hard and not c.passed]

    manifest = {
        "run_id": run_id,
        "status": "FAILED" if failed else "SUCCESS",
        "environment": DAT535_ENV,
        "mode": "full",
        "input": input_name,
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "started_at_utc": started_at,
        "rows": {source: int(value) for source, metric, value in metrics if metric == "rows"},
        "files": {
            source: {r.status: r["count"] for r in df.groupBy("status").count().orderBy("status").collect()}
            for source, df in files.items()
        },
        "issues": [
            {"source": r.source, "issue": r.issue, "action": r.action, "count": r["count"]}
            for r in issues.groupBy("source", "issue", "action").count().orderBy("source", "issue", "action").collect()
        ],
        "checks": check_dicts(checks),
        "freshness": {
            "newest_price_hour_utc": _newest_hour_per_zone(prices),
            "newest_weather_hour_utc": _newest_hour_per_zone(weather_hourly),
        },
        "chaos": chaos,
    }

    # 5. Write only when every hard check passed; the manifest always records the outcome.
    if failed:
        for c in failed:
            logger.error(f"[CHECK FAILED] {c.name}: {c.value:g} ({c.detail})")
        logger.error("Hard checks failed: nothing written, Gold keeps the previous Silver.")
    else:
        published = {**tables, "dq_issues": issues}
        staging = output_dir / STAGING_DIR / run_id
        shutil.rmtree(output_dir / STAGING_DIR, ignore_errors=True)  # leftovers of a crashed run
        for name, df in published.items():
            write_table(df, staging / name, TABLE_PARTITIONS[name])
        publish(staging, output_dir, list(published))
        metrics_df = spark.createDataFrame(
            [(run_id, s, m, float(v)) for s, m, v in metrics], "run_id string, source string, metric string, value double"
        ).withColumn("computed_at_utc", detected_at)
        metrics_df.coalesce(1).write.mode("append").parquet(str(output_dir / "dq_metrics"))

    manifest["finished_at_utc"] = get_utc_now_iso()
    manifest["elapsed_seconds"] = round(time.time() - start_time, 2)
    path = write_run_manifest(output_dir, manifest)

    for c in checks:
        if not c.hard and not c.passed:
            logger.warning(f"[CHECK WARNING] {c.name}: {c.value:g} ({c.detail})")
    logger.info("=" * 65)
    logger.info("SILVER LAYER SUMMARY")
    logger.info("=" * 65)
    logger.info(f"Status:          {manifest['status']}")
    logger.info(f"Execution Time:  {manifest['elapsed_seconds']:.2f}s")
    for name, n in manifest["rows"].items():
        logger.info(f"Rows {name + ':':<15} {n}")
    for i in manifest["issues"]:
        logger.info(f"Issue {i['source']}/{i['issue']} ({i['action']}): {i['count']}")
    if chaos and chaos["events"]:
        logger.info(f"Chaos errors:    found {chaos['found']} of {chaos['events']} ({100 * chaos['recall_all']:.1f}%); "
                    f"{chaos['unexplained_issues']} issues not on the chaos list")
    logger.info(f"Run manifest:    {path}")
    logger.info("=" * 65)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(step_summary(manifest))

    return EXIT_CHECKS_FAILED if failed else EXIT_OK


def main() -> int:
    """CLI Argument Parser."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="DAT535 Silver layer: Bronze -> clean prices, weather and zone_hourly tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        choices=SILVER_INPUTS,
        default=SILVER_INPUT if SILVER_INPUT in SILVER_INPUTS else None,
        help=f"Bronze tree to clean (default from SILVER_INPUT: {SILVER_INPUT})",
    )
    parser.add_argument("--output-dir", type=Path, default=SILVER_DIR, help=f"Silver output (default {SILVER_DIR})")
    parser.add_argument(
        "--allow-synthetic",
        action="store_true",
        help="Also use mock weather files (_is_synthetic); never enabled in production",
    )
    args = parser.parse_args()

    if args.input is None:
        logger.error(f"SILVER_INPUT must be one of {SILVER_INPUTS}, got {SILVER_INPUT!r}")
        return EXIT_CONFIG_ERROR
    input_dir = Path(INPUT_DIRS[args.input])
    if not input_dir.is_dir():
        logger.error(f"Input directory not found: {input_dir}")
        return EXIT_CONFIG_ERROR

    return run_silver(input_dir, args.output_dir, args.input, allow_synthetic=args.allow_synthetic)


if __name__ == "__main__":
    sys.exit(main())
