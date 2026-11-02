"""
Unit tests for the use case 3 Gold table (src/gold/household_bill.py), on a hand-built zone_hourly.
The expected bills are worked out by hand from the price rules in docs/plans/use_cases.md, section 9.
"""

from datetime import date, timedelta

import pytest

pytest.importorskip("pyspark")

from src.gold.dashboard import build_zone_daily  # noqa: E402
from src.gold.household_bill import build_household_bill_monthly  # noqa: E402
from src.gold.run_gold import household_bill_checks  # noqa: E402
from src.silver.reference import build_dim_date  # noqa: E402
from test_gold_dashboard import ZONE_HOURLY_SCHEMA, hourly  # noqa: E402

pytestmark = pytest.mark.spark

AUTUMN_2024 = date(2024, 10, 27)  # 02:00 happens twice
AUTUMN_HOURS = [0, 1, 2, 2] + list(range(3, 24))


def _bill(spark, rows):
    zone_hourly = spark.createDataFrame(rows, ZONE_HOURLY_SCHEMA)
    dim_date = build_dim_date(spark, date(2023, 1, 1), date(2027, 12, 31))
    return zone_hourly, dim_date, build_household_bill_monthly(zone_hourly, dim_date)


def _by_key(bill):
    return {(r.bidding_zone, r.year, r.month, r.scheme): r for r in bill.collect()}


def _month(zone, year, month, price=lambda h: 0.5, temp=lambda h: 10.0, gap_day=None):
    """zone_hourly rows for a whole Norwegian month; gap_day loses its price at 05:00."""
    rows, day = [], date(year, month, 1)
    while day.month == month:
        day_price = (lambda h: None if h == 5 else price(h)) if day == gap_day else price
        rows += hourly(zone, day.isoformat(), price=day_price, temp=temp,
                       local_hours=AUTUMN_HOURS if day == AUTUMN_2024 else None)
        day += timedelta(days=1)
    return rows


def test_the_worked_example_for_each_scheme_with_and_without_vat(spark):
    # Spot 1.00 NOK and 17 °C (no heating): 0.6 kWh an hour, 14.4 kWh in the day.
    rows = (hourly("NO1", "2026-01-15", price=lambda h: 1.0, temp=lambda h: 17.0)
            + hourly("NO4", "2026-01-15", price=lambda h: 1.0, temp=lambda h: 17.0))
    _, _, bill = _bill(spark, rows)
    at = _by_key(bill)

    none, stotte, norges = (at[("NO1", 2026, 1, s)] for s in ("no_support", "stromstotte", "norgespris"))
    assert none.kwh == pytest.approx(14.4) and stotte.kwh == none.kwh == norges.kwh
    assert (none.cost_nok, none.avg_price_nok_kwh, none.support_nok) == (18.0, 1.25, 0.0)  # 25 % VAT
    # 0.9 x (1.00 - 0.77) x 1.25 = 0.25875 NOK/kWh of support: the household pays 0.99 per kWh.
    assert stotte.support_nok == pytest.approx(round(14.4 * 0.25875, 2))
    assert stotte.cost_nok == pytest.approx(round(14.4 * (1.25 - 0.25875), 2))
    assert stotte.avg_price_nok_kwh == pytest.approx(0.99125, abs=1e-4)
    assert (norges.cost_nok, norges.avg_price_nok_kwh, norges.support_nok) == (7.2, 0.5, 10.8)  # 0.40 + VAT

    # NO4 pays no VAT: 1.00 per kWh, support 0.9 x 0.23 = 0.207, Norgespris 0.40.
    no4 = {s: at[("NO4", 2026, 1, s)] for s in ("no_support", "stromstotte", "norgespris")}
    assert no4["no_support"].cost_nok == pytest.approx(14.4)
    assert no4["stromstotte"].cost_nok == pytest.approx(round(14.4 * (1 - 0.207), 2))
    assert no4["norgespris"].cost_nok == pytest.approx(round(14.4 * 0.40, 2))

    assert (none.price_hours, none.billed_hours, none.temp_filled_hours) == (24, 24, 0)
    assert (none.hours_in_month, none.is_final) == (744, False)  # one day of January: month to date


def test_each_scheme_appears_only_in_the_months_it_existed(spark):
    rows = (
        hourly("NO1", "2023-12-15")  # before the bill starts
        + hourly("NO1", "2025-09-15", price=lambda h: 0.5)  # below the threshold: no support
        + hourly("NO1", "2025-10-15", price=lambda h: 0.3, temp=lambda h: 17.0)  # Norgespris begins
        + hourly("NO1", "2027-01-15")  # no known threshold, Norgespris over
    )
    _, _, bill = _bill(spark, rows)
    at = _by_key(bill)

    schemes = {}
    for zone, year, month, scheme in at:
        schemes.setdefault((year, month), set()).add(scheme)
    assert schemes == {
        (2025, 9): {"no_support", "stromstotte"},
        (2025, 10): {"no_support", "stromstotte", "norgespris"},
        (2027, 1): {"no_support"},
    }

    september = at[("NO1", 2025, 9, "stromstotte")]
    assert september.support_nok == 0.0 and september.cost_nok == at[("NO1", 2025, 9, "no_support")].cost_nok

    # Spot 0.30 is below Norgespris' 0.40: the household paid more with it, so the "support" is negative.
    october = at[("NO1", 2025, 10, "norgespris")]
    assert at[("NO1", 2025, 10, "no_support")].cost_nok == pytest.approx(14.4 * 0.3 * 1.25)
    assert october.cost_nok == pytest.approx(7.2) and october.support_nok == pytest.approx(5.4 - 7.2)


def test_heating_and_hours_without_a_temperature_or_a_price(spark):
    rows = (
        hourly("NO1", "2024-01-15", price=lambda h: 1.0, temp=lambda h: 7.0)  # hdh 10: 1.6 kWh an hour
        + hourly("NO1", "2024-01-16", price=lambda h: 1.0, temp=lambda h: None if h < 12 else 17.0)
        + hourly("NO2", "2024-01-15", temp=lambda h: None)  # no temperature all month
        + hourly("NO3", "2024-01-15", price=lambda h: None if h == 0 else 1.0)
    )
    _, _, bill = _bill(spark, rows)
    at = _by_key(bill)

    # The 12 hours without a temperature use January's average: (24 x 10 + 12 x 0) / 36 hdh.
    no1 = at[("NO1", 2024, 1, "no_support")]
    filled = 12 * (0.6 + 0.1 * 240 / 36)
    assert no1.kwh == pytest.approx(24 * 1.6 + 12 * 0.6 + filled)
    assert no1.cost_nok == pytest.approx(round((24 * 1.6 + 12 * 0.6 + filled) * 1.25, 2))
    assert (no1.price_hours, no1.billed_hours, no1.temp_filled_hours) == (48, 48, 12)

    # Without any temperature that month, no hour can be billed; the month still counts its hours.
    assert ("NO2", 2024, 1, "stromstotte") not in at
    no2 = at[("NO2", 2024, 1, "no_support")]
    assert (no2.kwh, no2.cost_nok, no2.price_hours, no2.billed_hours) == (None, None, 24, 0)

    no3 = at[("NO3", 2024, 1, "no_support")]
    assert (no3.price_hours, no3.billed_hours) == (23, 23)
    assert no3.kwh == pytest.approx(23 * 1.8)  # 5 °C: hdh 12, so 0.6 + 1.2 kWh an hour


def test_a_month_is_final_when_every_hour_has_a_price(spark):
    rows = (
        _month("NO1", 2024, 10)  # the autumn daylight saving day has 25 hours
        + _month("NO2", 2024, 10, gap_day=date(2024, 10, 10))
        + hourly("NO1", "2024-03-15")  # the spring day has 23 hours
    )
    _, _, bill = _bill(spark, rows)
    at = _by_key(bill)

    full = at[("NO1", 2024, 10, "no_support")]
    assert (full.price_hours, full.billed_hours, full.hours_in_month, full.is_final) == (745, 745, 745, True)
    assert at[("NO1", 2024, 10, "stromstotte")].is_final
    gap = at[("NO2", 2024, 10, "no_support")]
    assert (gap.price_hours, gap.hours_in_month, gap.is_final) == (744, 745, False)
    assert at[("NO1", 2024, 3, "no_support")].hours_in_month == 743


def test_the_checks_pass_and_catch_a_lost_month(spark):
    rows = (hourly("NO1", "2025-10-15", price=lambda h: 1.0)
            + hourly("NO4", "2025-11-15", price=lambda h: 1.0))
    zone_hourly, dim_date, bill = _bill(spark, rows)
    daily = build_zone_daily(zone_hourly, dim_date)

    checks = household_bill_checks(bill, daily)
    assert [c.name for c in checks if not c.passed] == []

    checks = household_bill_checks(bill.filter("bidding_zone != 'NO4'"), daily)
    assert [c.name for c in checks if not c.passed] == ["household_bill_monthly.price_hours_match_zone_daily"]
