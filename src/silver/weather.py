"""
Cleans Bronze weather observations into `silver/weather_hourly`: one row per station and UTC hour.

Records (one Frost reference time each, docs/plans/silver_layer.md section 7):
    timestamp  unreadable referenceTime -> the whole record is rejected (nothing to repair from)
    right day  the reference time lies on the file's UTC day, else rejected
    duplicate  the same reference time again in one file: the later copy is removed
Values, in every series (D7): null -> null_value; outside the bounds -> out_of_range; Frost
    quality code 5, 6 or 7 -> bad_quality (D10). The value is rejected; the hour stays empty.
Series: per element and hour one series is used: hourly (PT1H), no offset, on the full hour;
    temperature at 2 m, wind at 10 m (or the nearest level); then the lowest timeSeriesId.
Gap filling: a gap of 1-2 missing hours between two valid values gets the last value before the
    gap (flagged *_is_imputed). Longer gaps and hours after the last observation stay empty.
"""

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.silver.quality import (
    ACTION_REJECTED,
    ACTION_REMOVED,
    collect_issues,
    issue_if,
    issues_to_log,
    raw_json,
)
from src.silver.reference import OSLO_TZ

SOURCE = "weather"
TEMPERATURE, WIND = "air_temperature", "wind_speed"
TEMP_MIN_C, TEMP_MAX_C = -50.0, 45.0  # project definition bounds
WIND_MIN_MPS, WIND_MAX_MPS = 0.0, 75.0
PREFERRED_LEVEL_M = {TEMPERATURE: 2.0, WIND: 10.0}  # standard screen and mast heights
REJECTED_QUALITY_CODES = (5, 6, 7)  # Frost: "very uncertain" (5, 6) and "erroneous" (7)
MAX_FILL_GAP_HOURS = 2  # the project definition fills gaps shorter than 3 hours
TIMESTAMP_FORMATS = ("yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ssX")  # 2024-01-15T00:00:00.000Z

STATUS_KEPT, STATUS_REJECTED, STATUS_REMOVED = "kept", "rejected", "removed"


def _flag(condition) -> F.Column:
    """A condition that is never null (null comparisons count as false)."""
    return F.coalesce(condition, F.lit(False))


def _check_records(records: DataFrame) -> DataFrame:
    """Record-level checks: timestamp, day and duplicates."""
    ref_time = F.coalesce(*[F.try_to_timestamp(F.col("rec.referenceTime"), F.lit(f)) for f in TIMESTAMP_FORMATS])
    rows = records.select(
        F.col("entity").alias("station_id"),
        "file_date",
        "source_file",
        "_batch_id",
        "_ingested_at_utc",
        "is_used",
        "record_index",
        ref_time.alias("ref_time"),
        F.col("rec.observations").alias("observations"),
        raw_json(F.col("rec")).alias("raw_value"),
    )
    placed = F.col("ref_time").isNotNull()
    rows = rows.withColumn("wrong_day", placed & _flag(F.to_date("ref_time") != F.col("file_date")))
    rows = rows.withColumn("in_place", placed & ~F.col("wrong_day"))
    first_copy = Window.partitionBy("source_file", "in_place", "ref_time").orderBy("record_index")
    rows = rows.withColumn("duplicate", F.col("in_place") & (F.row_number().over(first_copy) > 1))
    return rows.select(
        "*",
        collect_issues(
            issue_if(~placed, "bad_timestamp", ACTION_REJECTED, "referenceTime"),
            issue_if(F.col("wrong_day"), "wrong_day", ACTION_REJECTED, "referenceTime"),
            issue_if(F.col("duplicate"), "duplicate", ACTION_REMOVED),
        ).alias("issues"),
        F.when(~F.col("in_place"), STATUS_REJECTED)
        .when(F.col("duplicate"), STATUS_REMOVED)
        .otherwise(STATUS_KEPT)
        .alias("status"),
    )


def _check_values(records: DataFrame) -> DataFrame:
    """One row per observation of every kept record, with value checks (all series)."""
    obs = records.filter(F.col("status") == STATUS_KEPT).select(
        "station_id",
        "source_file",
        "_batch_id",
        "_ingested_at_utc",
        "is_used",
        "record_index",
        "ref_time",
        F.posexplode("observations").alias("obs_index", "obs"),
    )
    element, value = F.col("obs.elementId"), F.col("obs.value")
    low = F.when(element == TEMPERATURE, TEMP_MIN_C).when(element == WIND, WIND_MIN_MPS)
    high = F.when(element == TEMPERATURE, TEMP_MAX_C).when(element == WIND, WIND_MAX_MPS)
    null_value = value.isNull()
    out_of_range = ~null_value & _flag(~value.between(low, high))
    bad_quality = ~null_value & ~out_of_range & _flag(F.col("obs.qualityCode").isin(*REJECTED_QUALITY_CODES))
    field = F.concat(F.lit("observations["), F.col("obs_index").cast("string"), F.lit("]."))
    return obs.select(
        "*",
        (~null_value & ~out_of_range & ~bad_quality).alias("valid"),
        collect_issues(
            issue_if(null_value, "null_value", ACTION_REJECTED, F.concat(field, F.lit("value"))),
            issue_if(out_of_range, "out_of_range", ACTION_REJECTED, F.concat(field, F.lit("value"))),
            issue_if(bad_quality, "bad_quality", ACTION_REJECTED, F.concat(field, F.lit("qualityCode"))),
        ).alias("issues"),
        raw_json(F.col("obs")).alias("raw_value"),
    )


def _hourly_series(observations: DataFrame) -> DataFrame:
    """One temperature and one wind value per station and hour, from the version in use."""
    element, level = F.col("obs.elementId"), F.col("obs.level.value")
    candidates = observations.filter(
        F.col("is_used")
        & element.isin(TEMPERATURE, WIND)
        & (F.col("obs.timeResolution") == "PT1H")
        & (F.coalesce(F.col("obs.timeOffset"), F.lit("PT0H")) == "PT0H")
        & (F.date_trunc("hour", "ref_time") == F.col("ref_time"))
    )
    preferred_level = F.when(element == TEMPERATURE, PREFERRED_LEVEL_M[TEMPERATURE]).otherwise(PREFERRED_LEVEL_M[WIND])
    best_first = Window.partitionBy("station_id", "ref_time", "obs.elementId").orderBy(
        F.abs(F.coalesce(level, F.lit(1000.0)) - preferred_level),
        F.col("obs.timeSeriesId").asc_nulls_last(),
        "obs_index",
    )
    chosen = candidates.withColumn("rank", F.row_number().over(best_first)).filter(F.col("rank") == 1)

    def pick(name: str, column) -> F.Column:
        return F.max(F.when(element == name, column))

    # A rejected value in the chosen series leaves the hour empty; no fallback to another level.
    return chosen.groupBy("station_id", F.col("ref_time").alias("utc_hour")).agg(
        pick(TEMPERATURE, F.when(F.col("valid"), F.col("obs.value"))).alias("temp_celsius"),
        pick(WIND, F.when(F.col("valid"), F.col("obs.value"))).alias("wind_speed_mps"),
        pick(TEMPERATURE, F.col("obs.qualityCode")).cast("int").alias("temp_quality_code"),
        pick(WIND, F.col("obs.qualityCode")).cast("int").alias("wind_quality_code"),
        F.first("source_file").alias("_source_file"),
        F.first("_batch_id").alias("_batch_id"),
        F.first("_ingested_at_utc").alias("_ingested_at_utc"),
    )


def _fill_short_gaps(hourly: DataFrame, column: str, flag_column: str) -> DataFrame:
    """Fill gaps of at most MAX_FILL_GAP_HOURS missing hours with the last value before the gap.

    The input has a row for every hour of each station, so row offsets are hours. Bounded frames
    (+-MAX_FILL_GAP_HOURS rows) are enough to find both edges of a short gap and stay cheap; a
    frame to the end of the station's history would be quadratic in Spark.
    """
    by_hour = Window.partitionBy("station_id").orderBy("utc_hour")
    before = by_hour.rowsBetween(-MAX_FILL_GAP_HOURS, -1)
    after = by_hour.rowsBetween(1, MAX_FILL_GAP_HOURS)
    valid_hour = F.when(F.col(column).isNotNull(), F.col("utc_hour"))
    prev_hour = F.last(valid_hour, ignorenulls=True).over(before)
    next_hour = F.first(valid_hour, ignorenulls=True).over(after)
    missing_hours = (next_hour.cast("long") - prev_hour.cast("long")) / 3600 - 1
    fill = F.col(column).isNull() & _flag(missing_hours <= MAX_FILL_GAP_HOURS)
    return (
        hourly.select("*", fill.alias(flag_column), F.last(column, ignorenulls=True).over(before).alias("_prev"))
        .withColumn(column, F.when(F.col(flag_column), F.col("_prev")).otherwise(F.col(column)))
        .drop("_prev")
    )


def clean_weather(records: DataFrame, dim_station: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Clean exploded weather records (bronze_reader.explode_records of the weather files).

    Returns (weather_hourly table, issue-log rows, checked records with their `status`).
    """
    checked_records = _check_records(records)
    observations = _check_values(checked_records)
    hourly = _hourly_series(observations)

    # An hourly grid per station from its first to its last observed hour: missing hours become
    # rows, so gaps can be filled, and nothing is ever filled after the last observation.
    span = hourly.groupBy("station_id").agg(F.min("utc_hour").alias("first"), F.max("utc_hour").alias("last"))
    grid = span.select("station_id", F.explode(F.sequence("first", "last", F.expr("INTERVAL 1 HOUR"))).alias("utc_hour"))
    filled = grid.join(hourly, ["station_id", "utc_hour"], "left")
    filled = _fill_short_gaps(filled, "temp_celsius", "temp_is_imputed")
    filled = _fill_short_gaps(filled, "wind_speed_mps", "wind_is_imputed")

    local_date = F.to_date(F.from_utc_timestamp("utc_hour", OSLO_TZ))
    table = (
        filled.filter(F.col("temp_celsius").isNotNull() | F.col("wind_speed_mps").isNotNull())
        .join(F.broadcast(dim_station.select("station_id", "bidding_zone")), "station_id")
        .select(
            "station_id",
            "bidding_zone",
            "utc_hour",
            local_date.alias("local_date"),
            "temp_celsius",
            "wind_speed_mps",
            "temp_quality_code",
            "wind_quality_code",
            "temp_is_imputed",
            "wind_is_imputed",
            "_source_file",
            "_batch_id",
            "_ingested_at_utc",
        )
        .withColumn("year", F.year("local_date"))
        .withColumn("month", F.month("local_date"))
    )
    issues = issues_to_log(checked_records, SOURCE).unionByName(issues_to_log(observations, SOURCE))
    return table, issues, checked_records
