"""
Unit tests for the use case 1 Gold tables (src/gold/cheapest_hours.py) and the household bill's cap at
the day Gold runs (src/gold/household_bill.py), on a hand-built zone_hourly.
"""

from datetime import date, timedelta
import random

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from src.gold.cheapest_hours import (  # noqa: E402
    BLOCK_HOURS,
    build_cheapest_hours,
    build_consumer_prices_hourly,
    published_zones,
)
from src.gold.dashboard import build_zone_daily  # noqa: E402
from src.gold.household_bill import build_household_bill_monthly  # noqa: E402
from src.gold.run_gold import cheapest_hours_checks, household_bill_checks  # noqa: E402
from src.silver.reference import build_dim_date  # noqa: E402
from test_gold_dashboard import ZONE_HOURLY_SCHEMA, hourly  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def dim_date(spark):
    return build_dim_date(spark, date(2023, 12, 1), date(2027, 12, 31))


def _tables(spark, dim_date, rows):
    zone_hourly = spark.createDataFrame(rows, ZONE_HOURLY_SCHEMA)
    consumer = build_consumer_prices_hourly(zone_hourly)
    return zone_hourly, consumer, build_cheapest_hours(consumer, dim_date)


def test_consumer_prices_follow_the_worked_example(spark, dim_date):
    rows = (
        hourly("NO1", "2026-01-15", price=lambda h: 1.0)
        + hourly("NO4", "2026-01-15", price=lambda h: 1.0)  # no VAT in the north
        + hourly("NO1", "2023-12-31", price=lambda h: 1.0)  # before the rules: left out
        + hourly("NO1", "2027-01-05", price=lambda h: 1.0)  # no known threshold, Norgespris over
    )
    _, consumer, _ = _tables(spark, dim_date, rows)
    at = {(r.bidding_zone, r.local_date.isoformat()): r for r in consumer.filter(F.col("local_hour") == 9).collect()}

    no1 = at[("NO1", "2026-01-15")]
    assert (no1.spot_nok_kwh, no1.vat_rate, no1.price_with_vat_nok_kwh) == (1.0, 0.25, 1.25)
    assert no1.stromstotte_nok_kwh == round(0.9 * (1.0 - 0.77) * 1.25, 5)
    assert no1.price_with_stromstotte_nok_kwh == round(1.25 - 0.9 * 0.23 * 1.25, 5)  # 0.99, use_cases.md section 9
    assert no1.norgespris_nok_kwh == 0.5

    no4 = at[("NO4", "2026-01-15")]
    assert (no4.vat_rate, no4.price_with_vat_nok_kwh, no4.norgespris_nok_kwh) == (0.0, 1.0, 0.4)
    assert no4.price_with_stromstotte_nok_kwh == round(1.0 - 0.9 * 0.23, 5)

    assert ("NO1", "2023-12-31") not in at
    later = at[("NO1", "2027-01-05")]
    assert later.stromstotte_nok_kwh is None and later.price_with_stromstotte_nok_kwh is None
    assert later.norgespris_nok_kwh is None and later.price_with_vat_nok_kwh == 1.25


def _ranking_day(h):
    """Spot 2.00 all day, except 0, 3, 0 at 02-04 and 0.90 at 10-12.

    On spot, 10:00-13:00 is the cheapest 3 hours (0.90 against 1.00). After strømstønad the 3.00 hour
    costs barely more than a 2.00 one, so 02:00-05:00 is far cheaper for a household (0.41 against 0.98).
    """
    return {2: 0.0, 3: 3.0, 4: 0.0, 10: 0.9, 11: 0.9, 12: 0.9}.get(h, 2.0)


def _household(spot, threshold=0.77, vat=0.25):
    return spot * (1 + vat) - 0.9 * max(0.0, spot - threshold) * (1 + vat)


def test_blocks_are_ranked_by_what_a_household_pays(spark, dim_date):
    _, _, cheapest = _tables(spark, dim_date, hourly("NO1", "2026-01-15", price=_ranking_day))
    blocks = {r.block_hours: r for r in cheapest.collect()}

    three = blocks[3]
    assert (three.start_time, three.end_time) == ("02:00", "05:00")
    expected = (_household(0.0) + _household(3.0) + _household(0.0)) / 3
    assert three.avg_price_nok_kwh == pytest.approx(expected, abs=1e-5)
    assert three.avg_spot_nok_kwh == 1.0  # dearer on spot than 10:00-13:00, cheaper for the household
    assert three.evening_avg_price_nok_kwh == pytest.approx(_household(2.0), abs=1e-5)
    assert three.saving_nok_kwh == pytest.approx(_household(2.0) - expected, abs=1e-5)

    assert (blocks[1].start_time, blocks[1].end_time) == ("02:00", "03:00")  # tie with 04:00: the earliest wins
    assert (blocks[6].end_utc - blocks[6].start_utc) == timedelta(hours=6)


def _brute_force(prices, n):
    """The cheapest n-hour block by plain search: (start index, average)."""
    averages = [(sum(prices[i:i + n]) / n, i) for i in range(len(prices) - n + 1)]
    best_avg, best_i = min(averages)  # ties: the earliest start, as in Gold
    return best_i, best_avg


def test_cheapest_blocks_match_a_brute_force_search(spark, dim_date):
    rng = random.Random(42)
    days = ["2026-01-12", "2026-01-13", "2025-06-02"]
    zones = ["NO1", "NO2", "NO4"]
    prices = {(z, d): [round(rng.uniform(-0.1, 3.0), 3) for _ in range(24)] for z in zones for d in days}
    rows = [row for (z, d), p in prices.items() for row in hourly(z, d, price=lambda h, p=p: p[h])]
    _, consumer, cheapest = _tables(spark, dim_date, rows)

    household, utc_hours = {}, {}
    for r in consumer.orderBy("utc_hour").collect():
        key = (r.bidding_zone, r.local_date.isoformat())
        household.setdefault(key, []).append(r.price_with_stromstotte_nok_kwh)
        utc_hours.setdefault(key, []).append(r.utc_hour)

    found = {(r.bidding_zone, r.local_date.isoformat(), r.block_hours): r for r in cheapest.collect()}
    assert len(found) == len(prices) * len(BLOCK_HOURS)
    for (zone, day), day_prices in household.items():
        for n in BLOCK_HOURS:
            start, avg = _brute_force(day_prices, n)
            block = found[(zone, day, n)]
            # Compared in UTC: the test helper's hours are one hour off Norwegian summer time.
            assert block.start_utc == utc_hours[(zone, day)][start], (zone, day, n)
            assert block.avg_price_nok_kwh == pytest.approx(avg, abs=1e-5)
            assert block.saving_nok_kwh >= 0


def test_only_complete_days_get_blocks(spark, dim_date):
    spring = [0, 1] + list(range(3, 24))  # 29 March 2026: 02:00 never happens
    rows = (
        hourly("NO1", "2026-03-29", local_hours=spring)
        + hourly("NO2", "2026-01-15", local_hours=range(20))  # four hours missing
    )
    _, consumer, cheapest = _tables(spark, dim_date, rows)

    assert consumer.count() == 23 + 20
    assert sorted((r.bidding_zone, r.block_hours) for r in cheapest.collect()) == [("NO1", 1), ("NO1", 3), ("NO1", 6)]


def test_a_zone_is_published_when_its_day_is_complete(spark, dim_date):
    rows = (
        hourly("NO1", "2026-11-04", price=_ranking_day)
        + hourly("NO5", "2026-11-04")
        + hourly("NO3", "2026-11-04", local_hours=range(12))  # half published: not yet
        + hourly("NO2", "2026-11-03")  # another day
    )
    _, _, cheapest = _tables(spark, dim_date, rows)

    assert published_zones(cheapest, date(2026, 11, 4)) == ["NO1", "NO5"]
    assert published_zones(cheapest, date(2026, 11, 5)) == []


def test_the_checks_pass_and_catch_a_missing_block(spark, dim_date):
    rows = hourly("NO1", "2026-01-15", price=_ranking_day) + hourly("NO2", "2026-01-15")
    zone_hourly, consumer, cheapest = _tables(spark, dim_date, rows)
    daily = build_zone_daily(zone_hourly, dim_date)

    assert all(c.passed for c in cheapest_hours_checks(consumer, cheapest, daily))
    failed = [c.name for c in cheapest_hours_checks(consumer, cheapest.filter("block_hours != 6"), daily)
              if not c.passed]
    assert failed == ["cheapest_hours.blocks_per_complete_day"]


def test_the_household_bill_stops_before_the_day_gold_runs(spark, dim_date):
    rows = [r for day in ("2026-01-14", "2026-01-15", "2026-01-16") for r in hourly("NO1", day)]
    zone_hourly = spark.createDataFrame(rows, ZONE_HOURLY_SCHEMA)
    daily = build_zone_daily(zone_hourly, dim_date)

    capped = build_household_bill_monthly(zone_hourly, dim_date, bill_until=date(2026, 1, 15))
    uncapped = build_household_bill_monthly(zone_hourly, dim_date)

    def hours(bill):
        return bill.filter(F.col("scheme") == "no_support").first().price_hours
    assert (hours(capped), hours(uncapped)) == (24, 72)  # today (15th) and tomorrow (16th) are left out
    assert all(c.passed for c in household_bill_checks(capped, daily, bill_until=date(2026, 1, 15)))
    mismatch = [c for c in household_bill_checks(capped, daily) if c.name.endswith("price_hours_match_zone_daily")]
    assert not mismatch[0].passed  # the check has to use the same cap as the bill
