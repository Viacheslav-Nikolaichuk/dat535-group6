"""
Unit tests for the Silver reference tables (src/silver/reference.py).
"""

from datetime import date

import pytest

pytest.importorskip("pyspark")

from src.silver.reference import build_dim_date, build_dim_station, norwegian_public_holidays  # noqa: E402

pytestmark = pytest.mark.spark


def test_norwegian_public_holidays_2026():
    holidays = norwegian_public_holidays(2026)  # Easter Sunday is 5 April 2026
    assert len(holidays) == 12
    assert holidays[date(2026, 4, 2)] == "Maundy Thursday"
    assert holidays[date(2026, 4, 3)] == "Good Friday"
    assert holidays[date(2026, 5, 14)] == "Ascension Day"
    assert holidays[date(2026, 5, 25)] == "Whit Monday"
    assert holidays[date(2026, 5, 17)] == "Constitution Day"


def test_holidays_on_the_same_day_keep_both_names():
    # 2008: Easter Sunday was 23 March, so Ascension Day (Easter + 39) fell on 1 May, Labour Day.
    assert norwegian_public_holidays(2008)[date(2008, 5, 1)] == "Labour Day / Ascension Day"


def test_dim_date_hours_weekdays_and_holidays(spark):
    rows = {r.local_date: r for r in build_dim_date(spark, date(2026, 3, 28), date(2026, 10, 26)).collect()}

    assert len(rows) == (date(2026, 10, 26) - date(2026, 3, 28)).days + 1
    assert rows[date(2026, 3, 29)].hours_in_day == 23  # clocks go forward
    assert rows[date(2026, 10, 25)].hours_in_day == 25  # clocks go back
    assert rows[date(2026, 6, 1)].hours_in_day == 24

    monday, sunday = rows[date(2026, 10, 26)], rows[date(2026, 10, 25)]
    assert (monday.weekday, monday.is_weekend) == (1, False)
    assert (sunday.weekday, sunday.is_weekend) == (7, True)

    assert rows[date(2026, 5, 17)].is_holiday and rows[date(2026, 5, 17)].holiday_name == "Constitution Day"
    assert not rows[date(2026, 5, 18)].is_holiday and rows[date(2026, 5, 18)].holiday_name is None


def test_dim_station_maps_every_configured_station(spark):
    rows = {r.station_id: r for r in build_dim_station(spark).collect()}

    assert len(rows) == 6
    assert {r.bidding_zone for r in rows.values()} == {"NO1", "NO2", "NO3", "NO4", "NO5"}
    assert rows["SN39040"].bidding_zone == rows["SN44560"].bidding_zone == "NO2"
    assert rows["SN39040"].is_primary and not rows["SN44560"].is_primary
    assert rows["SN18700"].station_name == "Oslo - Blindern"
