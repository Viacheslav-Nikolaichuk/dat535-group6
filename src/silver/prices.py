"""
Cleans Bronze day-ahead prices into `silver/prices`: one row per bidding zone and UTC hour.

Rules (docs/plans/silver_layer.md, section 6), checked in this order:
    timestamp  time_start (with its UTC offset) -> UTC; if unreadable, time_end - 1 h (repaired)
    hourly     time_end - time_start is one hour and the hour is whole, else rejected
    right day  the Norwegian date of the hour is the file's date, else rejected
    duplicate  the same hour again in one file: the later copy is removed
    price      NOK/kWh present and within [-15, 20]; otherwise rebuilt as EUR x EXR (repaired),
               or rejected if EUR or EXR is unusable too

Every row of every checked file gets these checks, but only rows from the version in use
(`is_used`) reach the table.
"""

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.silver.quality import (
    ACTION_REJECTED,
    ACTION_REMOVED,
    ACTION_REPAIRED,
    collect_issues,
    issue_if,
    issues_to_log,
    raw_json,
)
from src.silver.reference import OSLO_TZ

SOURCE = "prices"
PRICE_MIN_NOK, PRICE_MAX_NOK = -15.0, 20.0  # NOK/kWh bounds from the project definition
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"  # e.g. 2024-01-15T01:00:00+01:00
# The API's NOK price is EUR x EXR rounded to 5 decimals; larger differences are counted.
NOK_EUR_TOLERANCE = 0.001

STATUS_KEPT, STATUS_REJECTED, STATUS_REMOVED = "kept", "rejected", "removed"


def _flag(condition) -> F.Column:
    """A condition that is never null (null comparisons count as false)."""
    return F.coalesce(condition, F.lit(False))


def clean_prices(records: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Clean exploded price records (bronze_reader.explode_records of the electricity files).

    Returns (prices table, issue-log rows, checked rows with their `status` for the scorecard).
    """
    fmt = F.lit(TIMESTAMP_FORMAT)
    rows = records.select(
        F.col("entity").alias("bidding_zone"),
        "file_date",
        "source_file",
        "_batch_id",
        "_ingested_at_utc",
        "is_used",
        "record_index",
        F.col("rec.NOK_per_kWh").alias("nok"),
        F.col("rec.EUR_per_kWh").alias("eur"),
        F.col("rec.EXR").alias("exr"),
        F.try_to_timestamp(F.col("rec.time_start"), fmt).alias("ts_start"),
        F.try_to_timestamp(F.col("rec.time_end"), fmt).alias("ts_end"),
        raw_json(F.col("rec")).alias("raw_value"),
    ).withColumn("utc_hour", F.coalesce("ts_start", F.col("ts_end") - F.expr("INTERVAL 1 HOUR")))

    placed = F.col("utc_hour").isNotNull()
    rows = rows.withColumn(
        "bad_resolution",
        placed & _flag(
            (F.col("ts_end").cast("long") - F.col("ts_start").cast("long") != 3600)
            | (F.date_trunc("hour", "utc_hour") != F.col("utc_hour"))
        ),
    ).withColumn(
        "wrong_day",
        placed & ~F.col("bad_resolution")
        & _flag(F.to_date(F.from_utc_timestamp("utc_hour", OSLO_TZ)) != F.col("file_date")),
    ).withColumn("in_place", placed & ~F.col("bad_resolution") & ~F.col("wrong_day"))

    # The first copy of an hour in a file wins; rows that are not in place never compete.
    first_copy = Window.partitionBy("source_file", "in_place", "utc_hour").orderBy("record_index")
    rows = rows.withColumn("duplicate", F.col("in_place") & (F.row_number().over(first_copy) > 1))

    nok_ok = _flag(F.col("nok").between(PRICE_MIN_NOK, PRICE_MAX_NOK))
    rebuilt = F.round(F.col("eur") * F.col("exr"), 5)
    rebuilt_ok = _flag(rebuilt.between(PRICE_MIN_NOK, PRICE_MAX_NOK))
    price_problem = F.col("in_place") & ~F.col("duplicate") & ~nok_ok
    price_action = F.when(rebuilt_ok, ACTION_REPAIRED).otherwise(ACTION_REJECTED)

    checked = rows.select(
        "*",
        nok_ok.alias("nok_ok"),
        F.when(nok_ok, F.col("nok")).when(rebuilt_ok, rebuilt).alias("price_nok_kwh"),
        _flag(nok_ok & (F.abs(F.col("nok") - rebuilt) > NOK_EUR_TOLERANCE)).alias("nok_eur_mismatch"),
        collect_issues(
            issue_if(
                F.col("ts_start").isNull(),
                "bad_timestamp",
                F.when(placed, ACTION_REPAIRED).otherwise(ACTION_REJECTED),
                "time_start",
            ),
            issue_if(F.col("bad_resolution"), "wrong_resolution", ACTION_REJECTED, "time_end"),
            issue_if(F.col("wrong_day"), "wrong_day", ACTION_REJECTED, "time_start"),
            issue_if(F.col("duplicate"), "duplicate", ACTION_REMOVED),
            issue_if(price_problem & F.col("nok").isNull(), "null_value", price_action, "NOK_per_kWh"),
            issue_if(price_problem & F.col("nok").isNotNull(), "out_of_range", price_action, "NOK_per_kWh"),
        ).alias("issues"),
        F.when(~F.col("in_place") | (price_problem & ~rebuilt_ok), STATUS_REJECTED)
        .when(F.col("duplicate"), STATUS_REMOVED)
        .otherwise(STATUS_KEPT)
        .alias("status"),
    )

    local_ts = F.from_utc_timestamp("utc_hour", OSLO_TZ)
    table = checked.filter(F.col("is_used") & (F.col("status") == STATUS_KEPT)).select(
        "bidding_zone",
        "utc_hour",
        F.to_date(local_ts).alias("local_date"),
        F.hour(local_ts).alias("local_hour"),
        "price_nok_kwh",
        F.col("eur").alias("price_eur_kwh"),
        F.col("exr").alias("eur_nok_rate"),
        (F.col("ts_start").isNull() | ~F.col("nok_ok")).alias("is_repaired"),
        F.col("source_file").alias("_source_file"),
        "_batch_id",
        "_ingested_at_utc",
    ).withColumn("year", F.year("local_date")).withColumn("month", F.month("local_date"))

    return table, issues_to_log(checked, SOURCE), checked
