"""
Builds `silver/zone_hourly`: one row for every bidding zone and UTC hour. This is the table Gold reads.

  * A complete grid: every zone x every hour from the first to the last price hour. A missing
    source shows up as has_price / has_weather = false instead of as a silently absent row.
  * Zone weather: the mean of the zone's stations (NO2 averages Kjevik and Sola); station_count is
    the number of stations with a temperature in that hour. has_weather means "a temperature".
  * Heating degree hours: hdh = max(0, 17 - temp_celsius), empty when the temperature is unknown.
  * Left joins onto the grid. The project definition's inner join would drop tomorrow's prices,
    which have no weather yet.
  * Norwegian date, hour and calendar fields (weekday, weekend, public holiday) from dim_date.
"""

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.silver.reference import OSLO_TZ

HDH_BASE_C = 17.0  # Norwegian residential heating baseline (project definition)


def build_zone_hourly(
    prices: DataFrame,
    weather_hourly: DataFrame,
    dim_date: DataFrame,
    zones: Sequence[str],
) -> DataFrame:
    spark = prices.sparkSession

    zone_weather = weather_hourly.groupBy("bidding_zone", "utc_hour").agg(
        F.round(F.avg("temp_celsius"), 2).alias("temp_celsius"),
        F.round(F.avg("wind_speed_mps"), 2).alias("wind_speed_mps"),
        F.count("temp_celsius").cast("int").alias("station_count"),
        F.max(F.col("temp_is_imputed") | F.col("wind_is_imputed")).alias("weather_is_imputed"),
    )

    hours = prices.agg(F.min("utc_hour").alias("first"), F.max("utc_hour").alias("last")).select(
        F.explode(F.sequence("first", "last", F.expr("INTERVAL 1 HOUR"))).alias("utc_hour")
    )
    zone_list = spark.createDataFrame([(z,) for z in zones], "bidding_zone string")
    grid = hours.crossJoin(F.broadcast(zone_list))

    local_ts = F.from_utc_timestamp("utc_hour", OSLO_TZ)
    joined = (
        grid.join(prices.select("bidding_zone", "utc_hour", "price_nok_kwh", "price_eur_kwh"),
                  ["bidding_zone", "utc_hour"], "left")
        .join(zone_weather, ["bidding_zone", "utc_hour"], "left")
        .withColumn("local_date", F.to_date(local_ts))
        .withColumn("local_hour", F.hour(local_ts))
        .join(F.broadcast(dim_date.select("local_date", "weekday", "is_weekend", "is_holiday")), "local_date", "left")
    )

    temp = F.col("temp_celsius")
    # greatest() skips nulls, so the null case is explicit: no temperature means no hdh, not 0.
    hdh = F.when(temp.isNotNull(), F.round(F.greatest(F.lit(0.0), F.lit(HDH_BASE_C) - temp), 2))
    return joined.select(
        "bidding_zone",
        "utc_hour",
        "local_date",
        "local_hour",
        "weekday",
        "is_weekend",
        F.coalesce("is_holiday", F.lit(False)).alias("is_holiday"),
        "price_nok_kwh",
        "price_eur_kwh",
        "temp_celsius",
        "wind_speed_mps",
        hdh.alias("hdh"),
        F.coalesce("station_count", F.lit(0)).alias("station_count"),
        F.col("price_nok_kwh").isNotNull().alias("has_price"),
        temp.isNotNull().alias("has_weather"),
        F.coalesce("weather_is_imputed", F.lit(False)).alias("weather_is_imputed"),
        F.year("local_date").alias("year"),
        F.month("local_date").alias("month"),
    )
