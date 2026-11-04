"""
Gold table for use case 3, the report "Monthly household bill" (docs/plans/use_cases.md, section 5.3).

What did a typical household in each zone pay for electricity in a month, with strømstønad, with
Norgespris, and with no support at all? Silver has spot prices only. Gold adds a modelled household,
VAT and the support schemes (config/price_rules.py, from use_cases.md section 9), prices every hour,
and sums the hours of each Norwegian month:

  * household_bill_monthly: one row per zone, month and scheme. kWh used, cost, average price per kWh,
    support received, and whether the month is final (every hour of the month has a price).

The household: we have no consumption data, so every hour uses a base load plus heating that grows as
it gets colder, kWh = 0.6 + 0.1 x hdh (Silver's heating degree hours). An hour without a temperature
uses the zone's average hdh for that month. Costs are the energy price including VAT only: the grid
tariff and the supplier's fees are the same under every scheme, so they don't change which one wins.
Both schemes cover at most 5,000 kWh a month; the household stays below that (a Gold check), so the
cap is left out.
"""

from datetime import date
from typing import Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from config.price_rules import (
    BASE_LOAD_KWH,
    BILL_START,
    KWH_PER_HDH,
    NO_VAT_ZONES,
    NORGESPRIS_FROM,
    NORGESPRIS_NOK,
    NORGESPRIS_TO,
    SCHEME_NONE,
    SCHEME_NORGESPRIS,
    SCHEME_STROMSTOTTE,
    SCHEMES,
    STROMSTOTTE_SHARE,
    STROMSTOTTE_THRESHOLD_NOK,
    VAT,
)


def vat_rate() -> Column:
    return F.when(F.col("bidding_zone").isin(*NO_VAT_ZONES), 0.0).otherwise(VAT)


def bill_days(local_date: Column, bill_until: Optional[date]) -> Column:
    """The days the bill covers: from BILL_START, and before bill_until when it is given."""
    days = local_date >= BILL_START
    return days if bill_until is None else days & (local_date < F.lit(bill_until))


def build_household_bill_monthly(
    zone_hourly: DataFrame, dim_date: DataFrame, bill_until: Optional[date] = None
) -> DataFrame:
    """One row per zone, Norwegian month and scheme, from BILL_START.

    support_nok is what the scheme saved compared with paying the spot price with VAT. For Norgespris
    it is negative when the spot price was below the Norgespris price: the household then paid more.
    A scheme that didn't exist in a month (Norgespris before October 2025) has no row for it.

    bill_until (exclusive) is the day Gold runs. Silver also has today's and, after the day-ahead run,
    tomorrow's prices, but no weather for them yet: billing them would use estimated heating and make
    a month final before it is over. With the cap, month to date runs through yesterday, and a month
    becomes final with the nightly run on the 1st.
    """
    keys = ["bidding_zone", "year", "month"]
    spot = F.col("price_nok_kwh")
    with_vat = 1 + vat_rate()

    # The household. An hour without a temperature uses the zone's average heating need that month;
    # an hour without a price can't be billed, so it gets no kWh either.
    hdh = F.coalesce(F.col("hdh"), F.avg("hdh").over(Window.partitionBy(*keys)))
    kwh = F.when(F.col("has_price"), BASE_LOAD_KWH + KWH_PER_HDH * hdh)

    threshold = F.create_map(*[F.lit(v) for item in STROMSTOTTE_THRESHOLD_NOK.items() for v in item])[F.col("year")]
    # greatest() skips nulls, so a year without a known threshold has to stay empty explicitly.
    support = F.when(threshold.isNotNull(), STROMSTOTTE_SHARE * F.greatest(spot - threshold, F.lit(0.0)) * with_vat)
    norgespris = F.when(F.col("local_date").between(NORGESPRIS_FROM, NORGESPRIS_TO), NORGESPRIS_NOK * with_vat)

    hourly = zone_hourly.filter(bill_days(F.col("local_date"), bill_until)).select(
        *keys,
        "has_price",
        kwh.alias("kwh"),
        F.col("hdh").isNull().alias("temp_missing"),
        (spot * with_vat).alias("price_with_vat"),
        support.alias("support_per_kwh"),
        norgespris.alias("norgespris_per_kwh"),
    )

    # Every scheme is priced hour by hour. A sum over nothing but empty values stays empty, so a
    # scheme that didn't apply that month gets no cost.
    no_support = F.sum(F.col("kwh") * F.col("price_with_vat"))
    monthly = hourly.groupBy(*keys).agg(
        F.sum("kwh").alias("kwh"),
        no_support.alias(SCHEME_NONE),
        (no_support - F.sum(F.col("kwh") * F.col("support_per_kwh"))).alias(SCHEME_STROMSTOTTE),
        F.sum(F.col("kwh") * F.col("norgespris_per_kwh")).alias(SCHEME_NORGESPRIS),
        F.count(F.when(F.col("has_price"), True)).cast("int").alias("price_hours"),
        F.count("kwh").cast("int").alias("billed_hours"),
        F.count(F.when(F.col("kwh").isNotNull() & F.col("temp_missing"), True)).cast("int").alias("temp_filled_hours"),
    )

    month_hours = dim_date.groupBy("year", "month").agg(F.sum("hours_in_day").cast("int").alias("hours_in_month"))
    cost, without_support = F.col("cost_nok"), F.col("_cost_without_support")
    return (
        monthly.withColumn("_cost_without_support", F.col(SCHEME_NONE))
        .unpivot([*keys, "kwh", "_cost_without_support", "price_hours", "billed_hours", "temp_filled_hours"],
                 list(SCHEMES), "scheme", "cost_nok")
        # Every month keeps its no_support row, even before it has a cost, so its hours are counted.
        .filter((F.col("scheme") == SCHEME_NONE) | cost.isNotNull())
        .join(F.broadcast(month_hours), ["year", "month"], "left")
        .select(
            *keys,
            "scheme",
            F.round("kwh", 2).alias("kwh"),
            F.round(cost, 2).alias("cost_nok"),
            F.round(cost / F.col("kwh"), 4).alias("avg_price_nok_kwh"),
            F.round(without_support - cost, 2).alias("support_nok"),
            "price_hours",
            "billed_hours",
            "temp_filled_hours",
            "hours_in_month",
            F.coalesce(F.col("billed_hours") == F.col("hours_in_month"), F.lit(False)).alias("is_final"),
        )
    )
