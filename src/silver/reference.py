"""
Reference tables for the Silver layer.

  * dim_station: weather stations and the price zone each one represents, from
    config/zones_and_stations.json (stations were picked per zone by hand, so a lookup replaces
    the point-in-polygon mapping in the project definition).
  * dim_date: the Norwegian calendar: ISO weekday, weekend, public holidays, and the number of
    hours in each local day (23 or 25 on daylight-saving days).
"""

from datetime import date, timedelta
import json
from pathlib import Path
from typing import Dict, List, Tuple, Union

from dateutil.easter import easter
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from config.settings import ZONES_CONFIG_FILE

OSLO_TZ = "Europe/Oslo"

DIM_STATION_SCHEMA = StructType([
    StructField("station_id", StringType(), False),
    StructField("station_name", StringType()),
    StructField("bidding_zone", StringType(), False),
    StructField("zone_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("is_primary", BooleanType()),
])


def station_rows(config_path: Union[str, Path] = ZONES_CONFIG_FILE) -> List[Tuple]:
    """(station_id, station_name, bidding_zone, zone_name, lat, lon, is_primary) per station."""
    zones = json.loads(Path(config_path).read_text(encoding="utf-8")).get("zones", {})
    return [
        (s["id"], s.get("name"), zone, info.get("name"), float(s["lat"]), float(s["lon"]),
         s["id"] == info.get("primary_station"))
        for zone, info in sorted(zones.items())
        for s in info.get("stations", [])
    ]


def build_dim_station(spark: SparkSession, config_path: Union[str, Path] = ZONES_CONFIG_FILE) -> DataFrame:
    return spark.createDataFrame(station_rows(config_path), DIM_STATION_SCHEMA)


def norwegian_public_holidays(year: int) -> Dict[date, str]:
    """Norway's public holidays ("helligdager" and 1/17 May) for one year."""
    e = easter(year)
    days = [
        (date(year, 1, 1), "New Year's Day"),
        (e - timedelta(days=3), "Maundy Thursday"),
        (e - timedelta(days=2), "Good Friday"),
        (e, "Easter Sunday"),
        (e + timedelta(days=1), "Easter Monday"),
        (date(year, 5, 1), "Labour Day"),
        (date(year, 5, 17), "Constitution Day"),
        (e + timedelta(days=39), "Ascension Day"),
        (e + timedelta(days=49), "Whit Sunday"),
        (e + timedelta(days=50), "Whit Monday"),
        (date(year, 12, 25), "Christmas Day"),
        (date(year, 12, 26), "Boxing Day"),
    ]
    holidays: Dict[date, str] = {}
    for day, name in days:
        # Ascension Day or Whit Monday can fall on 1 or 17 May: keep both names.
        holidays[day] = f"{holidays[day]} / {name}" if day in holidays else name
    return holidays


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per Norwegian calendar day in [start, end]."""
    holidays = [
        (day, name)
        for year in range(start.year, end.year + 1)
        for day, name in norwegian_public_holidays(year).items()
        if start <= day <= end
    ]
    holiday_df = spark.createDataFrame(holidays, "local_date date, holiday_name string")
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{start.isoformat()}', DATE'{end.isoformat()}', INTERVAL 1 DAY)) AS local_date"
    )

    # Local midnight as a UTC instant; the distance to the next one is 23, 24 or 25 hours.
    midnight_utc = F.to_utc_timestamp(F.col("local_date").cast("timestamp"), OSLO_TZ)
    next_midnight_utc = F.to_utc_timestamp(F.date_add("local_date", 1).cast("timestamp"), OSLO_TZ)
    weekday = F.expr("weekday(local_date)") + 1  # ISO: 1 = Monday ... 7 = Sunday

    return days.join(F.broadcast(holiday_df), "local_date", "left").select(
        "local_date",
        F.year("local_date").alias("year"),
        F.month("local_date").alias("month"),
        F.dayofmonth("local_date").alias("day"),
        weekday.alias("weekday"),
        (weekday >= 6).alias("is_weekend"),
        F.col("holiday_name").isNotNull().alias("is_holiday"),
        "holiday_name",
        ((next_midnight_utc.cast("long") - midnight_utc.cast("long")) / 3600).cast("int").alias("hours_in_day"),
    )
