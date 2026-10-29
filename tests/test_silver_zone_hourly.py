"""
Unit tests for building silver/zone_hourly (src/silver/zone_hourly.py).
"""

from datetime import date, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from config.settings import PRICE_ZONES  # noqa: E402
from conftest import default_temp, weather_records  # noqa: E402
from src.silver.bronze_reader import DOMAIN_PRICES, DOMAIN_WEATHER, explode_records, read_bronze_files  # noqa: E402
from src.silver.prices import clean_prices  # noqa: E402
from src.silver.reference import build_dim_date, build_dim_station  # noqa: E402
from src.silver.weather import clean_weather  # noqa: E402
from src.silver.zone_hourly import build_zone_hourly  # noqa: E402

pytestmark = pytest.mark.spark

STATIONS = {"SN18700": "NO1", "SN39040": "NO2", "SN44560": "NO2", "SN68860": "NO3", "SN90450": "NO4", "SN50540": "NO5"}


def _build(spark, bronze):
    prices = clean_prices(explode_records(read_bronze_files(spark, bronze.root, DOMAIN_PRICES)))[0]
    weather = clean_weather(
        explode_records(read_bronze_files(spark, bronze.root, DOMAIN_WEATHER)), build_dim_station(spark)
    )[0]
    dim_date = build_dim_date(spark, date(2024, 1, 1), date(2024, 12, 31))
    return build_zone_hourly(prices, weather, dim_date, PRICE_ZONES)


def _station_temp(station: str):
    if station == "SN44560":
        return lambda h: default_temp(h) + 2.0  # Sola 2 degrees warmer than Kjevik
    if station == "SN68860":
        return lambda h: 20.0  # warm enough for hdh = 0
    return default_temp


def test_grid_weather_and_hdh_for_today_and_tomorrow(spark, bronze):
    for zone in PRICE_ZONES:
        bronze.prices(zone, "2024-01-15")
        bronze.prices(zone, "2024-01-16")  # "tomorrow": prices only
    for station in STATIONS:
        for day in ("2024-01-14", "2024-01-15"):
            bronze.weather(station, day, weather_records(day, station, temp=_station_temp(station)))

    rows = _build(spark, bronze).collect()

    assert len(rows) == 5 * 48  # every zone x every hour of the two Norwegian days
    assert all(r.has_price for r in rows)
    first_hour = datetime(2024, 1, 14, 23)  # 00:00 Norwegian time on 15 January
    assert sorted(r.utc_hour for r in rows if r.bidding_zone == "NO1") == [
        first_hour + timedelta(hours=i) for i in range(48)
    ]
    tomorrow = [r for r in rows if r.utc_hour >= datetime(2024, 1, 16)]
    assert tomorrow and not any(r.has_weather for r in tomorrow)
    assert all(r.hdh is None and r.station_count == 0 for r in tomorrow)

    at = {(r.bidding_zone, r.utc_hour): r for r in rows}
    no1, no2, no3 = (at[(z, datetime(2024, 1, 15, 10))] for z in ("NO1", "NO2", "NO3"))
    assert (no1.temp_celsius, no1.station_count, no1.hdh) == (default_temp(10), 1, round(17.0 - default_temp(10), 2))
    assert (no2.temp_celsius, no2.station_count) == (default_temp(10) + 1.0, 2)  # mean of Kjevik and Sola
    assert (no3.temp_celsius, no3.hdh) == (20.0, 0.0)
    assert (no1.local_date, no1.local_hour, no1.weekday, no1.is_weekend) == (date(2024, 1, 15), 11, 1, False)
    assert (no1.year, no1.month) == (2024, 1)


def test_missing_zones_are_rows_without_a_price(spark, bronze):
    bronze.prices("NO1", "2024-05-17")  # Constitution Day, a Friday

    table = _build(spark, bronze)

    assert table.count() == 5 * 24
    counts = {r.bidding_zone: r.n for r in table.groupBy("bidding_zone").agg(F.sum(F.col("has_price").cast("int")).alias("n")).collect()}
    assert counts == {"NO1": 24, "NO2": 0, "NO3": 0, "NO4": 0, "NO5": 0}
    row = table.filter(F.col("bidding_zone") == "NO2").first()
    assert (row.is_holiday, row.weekday, row.price_nok_kwh, row.has_weather) == (True, 5, None, False)


def test_no_prices_gives_an_empty_table(spark, bronze):
    bronze.weather("SN18700", "2024-01-15")
    assert _build(spark, bronze).count() == 0
