"""
Gold tables for use case 2, the dashboard "Prices and weather" (docs/plans/use_cases.md, section 5.2).

Silver's zone_hourly has one row per zone and UTC hour. The dashboard needs daily and monthly figures
per zone, with prices and weather side by side, so Gold builds them once:

  * zone_daily: one row per zone and Norwegian day. Price level, volatility, peak vs off-peak,
    negative hours, weather, heating degree days, and how many hours had a price and a temperature.
  * price_weather_sensitivity: one row per zone and month. Monthly averages, and how closely the
    daily price followed temperature and wind (correlation and slope). Only complete days are used.
  * price_profile: the average price per Norwegian hour of the day, for workdays against weekends and
    holidays, and winter against summer.

Peak hours are 08-20 Norwegian time on workdays (not weekends, not public holidays). Prices exclude VAT;
business rules such as VAT and support schemes belong to the other use cases.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PEAK_START_HOUR = 8  # inclusive, Norwegian time
PEAK_END_HOUR = 20  # exclusive
WINTER_MONTHS = (10, 11, 12, 1, 2, 3)  # the heating season
MIN_FIT_DAYS = 10  # fewer complete days in a month give no correlation or slope

SEASON_WINTER = "winter"
SEASON_SUMMER = "summer"
DAY_WORKDAY = "workday"
DAY_WEEKEND_HOLIDAY = "weekend_holiday"


def _is_workday() -> Column:
    return ~F.col("is_weekend") & ~F.col("is_holiday")


def build_zone_daily(zone_hourly: DataFrame, dim_date: DataFrame) -> DataFrame:
    """One row per zone and Norwegian day, including days with missing hours (their counts show the gap)."""
    price = F.col("price_nok_kwh")
    hour = F.col("local_hour")
    is_peak = _is_workday() & (hour >= PEAK_START_HOUR) & (hour < PEAK_END_HOUR)

    daily = zone_hourly.groupBy("bidding_zone", "local_date").agg(
        F.round(F.avg(price), 5).alias("avg_price_nok_kwh"),
        F.min(price).alias("min_price_nok_kwh"),
        F.max(price).alias("max_price_nok_kwh"),
        F.round(F.stddev_samp(price), 5).alias("price_stddev_nok_kwh"),
        F.round(F.avg(F.when(is_peak, price)), 5).alias("peak_avg_price_nok_kwh"),
        F.round(F.avg(F.when(~is_peak, price)), 5).alias("offpeak_avg_price_nok_kwh"),
        F.count(F.when(price < 0, True)).cast("int").alias("negative_price_hours"),
        F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
        F.min("temp_celsius").alias("min_temp_celsius"),
        F.max("temp_celsius").alias("max_temp_celsius"),
        # Heating degree days from Silver's heating degree hours; covers only the hours with a temperature.
        F.round(F.sum("hdh") / 24, 2).alias("hdd"),
        F.round(F.avg("wind_speed_mps"), 2).alias("avg_wind_mps"),
        F.count(price).cast("int").alias("price_hours"),
        F.count("temp_celsius").cast("int").alias("weather_hours"),  # Silver: has_weather = a temperature
    )

    peak, offpeak = F.col("peak_avg_price_nok_kwh"), F.col("offpeak_avg_price_nok_kwh")
    calendar = dim_date.select("local_date", "weekday", "is_weekend", "is_holiday", "hours_in_day")
    complete = (F.col("price_hours") == F.col("hours_in_day")) & (F.col("weather_hours") == F.col("hours_in_day"))
    return (
        daily.join(F.broadcast(calendar), "local_date", "left")
        .select(
            "bidding_zone",
            "local_date",
            "weekday",
            "is_weekend",
            "is_holiday",
            "avg_price_nok_kwh",
            "min_price_nok_kwh",
            "max_price_nok_kwh",
            "price_stddev_nok_kwh",
            "peak_avg_price_nok_kwh",
            "offpeak_avg_price_nok_kwh",
            # Weekends and holidays have no peak hours; a ratio over an off-peak price near or below
            # zero means nothing, so both stay empty.
            F.when(offpeak > 0, F.round(peak / offpeak, 3)).alias("peak_offpeak_ratio"),
            "negative_price_hours",
            "avg_temp_celsius",
            "min_temp_celsius",
            "max_temp_celsius",
            "hdd",
            "avg_wind_mps",
            "price_hours",
            "weather_hours",
            "hours_in_day",
            F.coalesce(complete, F.lit(False)).alias("is_complete"),
            F.year("local_date").alias("year"),
            F.month("local_date").alias("month"),
        )
    )


def _fit_or_null(value: Column, days: Column) -> Column:
    """Empty when the month has too few days, or the statistic is undefined (no variance)."""
    return F.when((days >= MIN_FIT_DAYS) & ~F.isnan(value), value)


def build_price_weather_sensitivity(zone_hourly: DataFrame, zone_daily: DataFrame) -> DataFrame:
    """One row per zone and month: monthly averages, and how the daily price followed the weather.

    Averages are over hours, from zone_hourly. Correlations and slopes are over the month's complete
    days (every hour has a price and a temperature) from zone_daily; days with gaps are left out.
    The slope is a least-squares fit of the daily average price on the daily average temperature
    (or wind), reported in øre/kWh: "+4 øre per degree colder".
    """
    keys = ["bidding_zone", "year", "month"]
    price = F.col("price_nok_kwh")
    monthly = zone_hourly.groupBy(*keys).agg(
        F.round(F.avg(price), 5).alias("avg_price_nok_kwh"),
        F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
        F.round(F.sum("hdh") / 24, 1).alias("hdd"),
        F.round(F.avg("wind_speed_mps"), 2).alias("avg_wind_mps"),
        F.count(F.when(price < 0, True)).cast("int").alias("negative_price_hours"),
        F.count(price).cast("int").alias("price_hours"),
        F.count("temp_celsius").cast("int").alias("weather_hours"),
        F.count(F.lit(1)).cast("int").alias("hours"),
    )

    t, w, p = F.col("avg_temp_celsius"), F.col("avg_wind_mps"), F.col("avg_price_nok_kwh")
    fits = (
        zone_daily.filter(F.col("is_complete"))
        .groupBy(*keys)
        .agg(
            F.count(F.lit(1)).cast("int").alias("fit_days"),
            F.count(w).alias("_wind_days"),
            F.corr(t, p).alias("_corr_temp"),
            (F.covar_samp(t, p) / F.var_samp(t)).alias("_slope_temp"),
            F.corr(w, p).alias("_corr_wind"),
            (F.covar_samp(w, p) / F.var_samp(w)).alias("_slope_wind"),
        )
    )

    days, wind_days = F.col("fit_days"), F.col("_wind_days")
    return monthly.join(fits, keys, "left").select(
        *keys,
        "avg_price_nok_kwh",
        "avg_temp_celsius",
        "hdd",
        "avg_wind_mps",
        "negative_price_hours",
        "price_hours",
        "weather_hours",
        "hours",
        F.coalesce(days, F.lit(0)).alias("fit_days"),
        F.round(_fit_or_null(F.col("_corr_temp"), days), 3).alias("corr_temp_price"),
        # Price change per degree colder: the negative of the slope on temperature, in øre (NOK x 100).
        F.round(_fit_or_null(-F.col("_slope_temp") * 100, days), 2).alias("ore_per_degree_colder"),
        F.round(_fit_or_null(F.col("_corr_wind"), wind_days), 3).alias("corr_wind_price"),
        F.round(_fit_or_null(F.col("_slope_wind") * 100, wind_days), 2).alias("ore_per_mps_wind"),
    )


def build_price_profile(zone_hourly: DataFrame) -> DataFrame:
    """Average price per zone, year, season, day type and Norwegian hour of the day.

    On the autumn daylight saving day, 02:00 happens twice; both hours count towards hour 2.
    """
    season = F.when(F.col("month").isin(*WINTER_MONTHS), SEASON_WINTER).otherwise(SEASON_SUMMER)
    day_type = F.when(_is_workday(), DAY_WORKDAY).otherwise(DAY_WEEKEND_HOLIDAY)
    return (
        zone_hourly.filter(F.col("has_price"))
        .groupBy("bidding_zone", "year", season.alias("season"), day_type.alias("day_type"), "local_hour")
        .agg(
            F.round(F.avg("price_nok_kwh"), 5).alias("avg_price_nok_kwh"),
            F.count(F.lit(1)).cast("int").alias("n_hours"),
        )
    )
