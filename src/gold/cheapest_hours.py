"""
Gold tables for use case 1, the application "Cheapest hours tomorrow" (docs/plans/use_cases.md, section 5.1).

When should a household charge the car or run the dishwasher tomorrow, and what will it cost? Silver has
spot prices only. Gold adds VAT and the support schemes (config/price_rules.py) and finds the cheapest
blocks of hours in each day:

  * consumer_prices_hourly: one row per zone and hour from BILL_START. The spot price, VAT,
    strømstønad and the price after it, and the Norgespris price: what a household pays for energy.
  * cheapest_hours: one row per zone, Norwegian day and block length (1, 3 and 6 hours). The cheapest
    block, its average price, the same block starting at 17:00, and the saving per kWh moved.

Apps and smart chargers read both tables through the HTTP API (api/main.py, docs/api.md); the dashboard
page shows them to people.

Blocks are ranked by the price a household without Norgespris pays: the price after strømstønad (with
VAT only in a year without a known threshold). Strømstønad keeps the order of single hours, but it
flattens the expensive ones, so the cheapest block on average can differ from the cheapest on spot.
With Norgespris every hour costs the same: moving usage saves nothing on the energy price.

A day gets blocks only when every hour has a price (23-25 with daylight saving): use case 5.1 publishes
a zone's day only when it is complete. Blocks stay inside one Norwegian day.
"""

from datetime import date
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config.price_rules import (
    BILL_START,
    NORGESPRIS_FROM,
    NORGESPRIS_NOK,
    NORGESPRIS_TO,
    STROMSTOTTE_SHARE,
    STROMSTOTTE_THRESHOLD_NOK,
)
from config.settings import PRICE_ZONES
from src.gold.household_bill import vat_rate
from src.silver.reference import OSLO_TZ

BLOCK_HOURS = (1, 3, 6)  # e.g. a dishwasher, a washing machine, charging the car
EVENING_START_HOUR = 17  # the comparison: starting when people come home
PRICE_DECIMALS = 5


def build_consumer_prices_hourly(zone_hourly: DataFrame) -> DataFrame:
    """One row per zone and hour with a price, from BILL_START. NOK/kWh; all but spot include VAT.

    Strømstønad and the price after it are empty in a year without a known threshold, and the
    Norgespris price outside the dates the scheme runs.
    """
    spot = F.col("price_nok_kwh")
    with_vat = 1 + vat_rate()
    threshold = F.create_map(*[F.lit(v) for item in STROMSTOTTE_THRESHOLD_NOK.items() for v in item])[F.col("year")]
    # greatest() skips nulls, so a year without a known threshold has to stay empty explicitly.
    support = F.when(threshold.isNotNull(), STROMSTOTTE_SHARE * F.greatest(spot - threshold, F.lit(0.0)) * with_vat)
    norgespris = F.when(F.col("local_date").between(NORGESPRIS_FROM, NORGESPRIS_TO), NORGESPRIS_NOK * with_vat)

    return zone_hourly.filter(F.col("has_price") & (F.col("local_date") >= BILL_START)).select(
        "bidding_zone",
        "utc_hour",
        "local_date",
        "local_hour",
        F.round(spot, PRICE_DECIMALS).alias("spot_nok_kwh"),
        vat_rate().alias("vat_rate"),
        F.round(spot * with_vat, PRICE_DECIMALS).alias("price_with_vat_nok_kwh"),
        F.round(support, PRICE_DECIMALS).alias("stromstotte_nok_kwh"),
        F.round(spot * with_vat - support, PRICE_DECIMALS).alias("price_with_stromstotte_nok_kwh"),
        F.round(norgespris, PRICE_DECIMALS).alias("norgespris_nok_kwh"),
        "year",
        "month",
    )


def household_price():
    """What a household without Norgespris pays per kWh: after strømstønad, or with VAT when it's unknown."""
    return F.coalesce(F.col("price_with_stromstotte_nok_kwh"), F.col("price_with_vat_nok_kwh"))


def build_cheapest_hours(consumer_prices: DataFrame, dim_date: DataFrame) -> DataFrame:
    """One row per zone, complete Norwegian day and block length: the cheapest block of that many hours.

    Ties go to the earliest start. saving_nok_kwh is the evening block (starting 17:00) minus the
    cheapest block, per kWh: a 20 kWh car charge moved there saves 20 x saving_nok_kwh.
    """
    day = ["bidding_zone", "local_date"]
    keys = day + ["block_hours"]
    day_length = dim_date.select("local_date", "hours_in_day")
    complete_days = (
        consumer_prices.groupBy(*day).agg(F.count(F.lit(1)).alias("hours"))
        .join(F.broadcast(day_length), "local_date")
        .filter(F.col("hours") == F.col("hours_in_day"))
        .select(*day)
    )
    hours = consumer_prices.join(complete_days, day, "left_semi").withColumn("_price", household_price())

    by_hour = Window.partitionBy(*day).orderBy("utc_hour")
    candidates = reduce(DataFrame.unionByName, [
        hours.select(
            *day, "utc_hour", "local_hour", "year", "month",
            F.lit(n).alias("block_hours"),
            F.count(F.lit(1)).over(by_hour.rowsBetween(0, n - 1)).alias("_n"),
            F.avg("spot_nok_kwh").over(by_hour.rowsBetween(0, n - 1)).alias("_avg_spot"),
            F.avg("_price").over(by_hour.rowsBetween(0, n - 1)).alias("_avg_price"),
        ).filter(F.col("_n") == n)  # a block may not run past the end of the day
        for n in BLOCK_HOURS
    ])

    cheapest_first = Window.partitionBy(*keys).orderBy("_avg_price", "utc_hour")
    best = candidates.withColumn("_rank", F.row_number().over(cheapest_first)).filter(F.col("_rank") == 1)
    # The evening block. On the autumn daylight saving day 17:00 still happens once.
    evening = (
        candidates.filter(F.col("local_hour") == EVENING_START_HOUR)
        .groupBy(*keys).agg(F.min_by("_avg_price", "utc_hour").alias("_evening_price"))
    )

    end_utc = (F.col("utc_hour").cast("long") + F.col("block_hours") * 3600).cast("timestamp")
    local_clock = lambda ts: F.date_format(F.from_utc_timestamp(ts, OSLO_TZ), "HH:mm")  # noqa: E731
    return best.join(evening, keys, "left").select(
        *keys,
        F.col("utc_hour").alias("start_utc"),
        end_utc.alias("end_utc"),
        local_clock(F.col("utc_hour")).alias("start_time"),
        local_clock(end_utc).alias("end_time"),
        F.round("_avg_spot", PRICE_DECIMALS).alias("avg_spot_nok_kwh"),
        F.round("_avg_price", PRICE_DECIMALS).alias("avg_price_nok_kwh"),
        F.round("_evening_price", PRICE_DECIMALS).alias("evening_avg_price_nok_kwh"),
        F.round(F.col("_evening_price") - F.col("_avg_price"), PRICE_DECIMALS).alias("saving_nok_kwh"),
        "year",
        "month",
    )


def published_zones(cheapest: DataFrame, day: date) -> List[str]:
    """Zones whose day is complete in Gold (it has cheapest blocks), in zone order."""
    rows = cheapest.filter(F.col("local_date") == F.lit(day)).select("bidding_zone").distinct().collect()
    found = {r.bidding_zone for r in rows}
    return [zone for zone in PRICE_ZONES if zone in found]
