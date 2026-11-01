"""
Unit tests for the use case 2 Gold tables (src/gold/dashboard.py), on a hand-built zone_hourly.
"""

from datetime import date, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from src.gold.dashboard import (  # noqa: E402
    build_price_profile,
    build_price_weather_sensitivity,
    build_zone_daily,
)
from src.silver.reference import build_dim_date  # noqa: E402

pytestmark = pytest.mark.spark

ZONE_HOURLY_SCHEMA = (
    "bidding_zone string, utc_hour timestamp, local_date date, local_hour int, weekday int, "
    "is_weekend boolean, is_holiday boolean, price_nok_kwh double, temp_celsius double, "
    "wind_speed_mps double, hdh double, has_price boolean, has_weather boolean, year int, month int"
)


def hourly(zone, day, price=lambda h: 1.0, temp=lambda h: 5.0, wind=lambda h: 3.0,
           local_hours=None, is_holiday=False):
    """zone_hourly rows for one Norwegian day. The UTC hour only needs to be unique per zone."""
    d = date.fromisoformat(day)
    first = datetime(d.year, d.month, d.day) - timedelta(hours=1)
    rows = []
    for i, h in enumerate(local_hours if local_hours is not None else range(24)):
        p, t = price(h), temp(h)
        rows.append((
            zone, first + timedelta(hours=i), d, h, d.isoweekday(), d.isoweekday() >= 6, is_holiday,
            p, t, wind(h), None if t is None else max(0.0, 17.0 - t), p is not None, t is not None,
            d.year, d.month,
        ))
    return rows


def _tables(spark, rows):
    zone_hourly = spark.createDataFrame(rows, ZONE_HOURLY_SCHEMA)
    dim_date = build_dim_date(spark, date(2024, 1, 1), date(2024, 12, 31))
    daily = build_zone_daily(zone_hourly, dim_date)
    return zone_hourly, daily


def _peak_price(h):
    if h == 3:
        return -0.1  # one negative hour at night
    return 2.0 if 8 <= h < 20 else 1.0


def test_zone_daily_prices_peak_weather_and_hour_counts(spark):
    rows = (
        hourly("NO1", "2024-01-15", price=_peak_price, temp=lambda h: 7.0)  # a Monday
        + hourly("NO1", "2024-01-20", price=_peak_price)  # a Saturday: no peak hours
        + hourly("NO2", "2024-01-15", price=lambda h: None if h < 2 else 1.0,
                 temp=lambda h: None if h == 5 else 5.0)
    )
    _, daily = _tables(spark, rows)
    at = {(r.bidding_zone, r.local_date.isoformat()): r for r in daily.collect()}

    monday = at[("NO1", "2024-01-15")]
    assert monday.avg_price_nok_kwh == round((12 * 2.0 + 11 * 1.0 - 0.1) / 24, 5)
    assert (monday.min_price_nok_kwh, monday.max_price_nok_kwh) == (-0.1, 2.0)
    assert monday.peak_avg_price_nok_kwh == 2.0
    assert monday.offpeak_avg_price_nok_kwh == round((11 * 1.0 - 0.1) / 12, 5)
    assert monday.peak_offpeak_ratio == round(2.0 / round(10.9 / 12, 5), 3)
    assert monday.negative_price_hours == 1
    assert (monday.avg_temp_celsius, monday.hdd, monday.avg_wind_mps) == (7.0, 10.0, 3.0)
    assert (monday.price_hours, monday.weather_hours, monday.hours_in_day, monday.is_complete) == (24, 24, 24, True)
    assert (monday.weekday, monday.is_weekend, monday.year, monday.month) == (1, False, 2024, 1)

    saturday = at[("NO1", "2024-01-20")]
    assert saturday.peak_avg_price_nok_kwh is None and saturday.peak_offpeak_ratio is None
    assert saturday.is_weekend

    gaps = at[("NO2", "2024-01-15")]
    assert (gaps.price_hours, gaps.weather_hours, gaps.is_complete) == (22, 23, False)
    assert gaps.negative_price_hours == 0


def test_daylight_saving_day_has_25_hours_and_is_complete(spark):
    autumn = [0, 1, 2, 2] + list(range(3, 24))  # 02:00 happens twice on 27 October 2024
    _, daily = _tables(spark, hourly("NO1", "2024-10-27", local_hours=autumn))

    [row] = daily.collect()
    assert (row.price_hours, row.weather_hours, row.hours_in_day, row.is_complete) == (25, 25, 25, True)


def test_sensitivity_slope_correlation_and_too_few_days(spark):
    rows = []
    for i in range(20):  # 20 complete January days, 1 degree warmer each day, price falls 5 øre per degree
        t = -10.0 + i
        rows += hourly("NO1", (date(2024, 1, 1) + timedelta(days=i)).isoformat(),
                       price=lambda h, t=t: round(2.0 - 0.05 * t, 5), temp=lambda h, t=t: t)
    for i in range(5):  # only 5 days in February
        t = float(i)
        rows += hourly("NO2", (date(2024, 2, 1) + timedelta(days=i)).isoformat(),
                       price=lambda h, t=t: 1.0 + t, temp=lambda h, t=t: t)
    zone_hourly, daily = _tables(spark, rows)

    at = {(r.bidding_zone, r.month): r for r in build_price_weather_sensitivity(zone_hourly, daily).collect()}

    january = at[("NO1", 1)]
    assert january.fit_days == 20
    assert january.corr_temp_price == -1.0
    assert january.ore_per_degree_colder == 5.0
    assert january.corr_wind_price is None and january.ore_per_mps_wind is None  # constant wind: no fit
    assert january.avg_temp_celsius == -0.5
    assert (january.price_hours, january.weather_hours, january.hours) == (480, 480, 480)

    february = at[("NO2", 2)]
    assert february.fit_days == 5
    assert february.corr_temp_price is None and february.ore_per_degree_colder is None


def test_incomplete_days_are_left_out_of_the_fit(spark):
    rows = []
    for i in range(12):
        t = float(i)
        rows += hourly("NO1", (date(2024, 3, 1) + timedelta(days=i)).isoformat(),
                       price=lambda h, t=t: 1.0 + 0.1 * t, temp=lambda h, t=t: t)
    rows += hourly("NO1", "2024-03-20", price=lambda h: 99.0 if h else None, temp=lambda h: 0.0)  # a gap
    zone_hourly, daily = _tables(spark, rows)

    [march] = build_price_weather_sensitivity(zone_hourly, daily).collect()
    assert march.fit_days == 12
    assert march.ore_per_degree_colder == -10.0  # warmer days are dearer here; the 99.0 outlier is ignored


def test_price_profile_splits_season_and_day_type(spark):
    rows = (
        hourly("NO1", "2024-01-15", price=lambda h: float(h))  # winter workday
        + hourly("NO1", "2024-01-20", price=lambda h: 100.0 + h)  # winter weekend
        + hourly("NO1", "2024-07-15", price=lambda h: 200.0 + h)  # summer workday
        + hourly("NO1", "2024-05-17", price=lambda h: 300.0 + h, is_holiday=True)  # Constitution Day, a Friday
    )
    zone_hourly = spark.createDataFrame(rows, ZONE_HOURLY_SCHEMA)

    profile = {(r.season, r.day_type, r.local_hour): r for r in build_price_profile(zone_hourly).collect()}

    assert len(profile) == 4 * 24
    assert profile[("winter", "workday", 9)].avg_price_nok_kwh == 9.0
    assert profile[("winter", "weekend_holiday", 9)].avg_price_nok_kwh == 109.0
    assert profile[("summer", "workday", 9)].avg_price_nok_kwh == 209.0
    assert profile[("summer", "weekend_holiday", 9)].avg_price_nok_kwh == 309.0
    assert all(r.n_hours == 1 for r in profile.values())
