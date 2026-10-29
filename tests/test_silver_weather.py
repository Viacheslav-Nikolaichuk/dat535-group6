"""
Unit tests for cleaning weather into silver/weather_hourly (src/silver/weather.py).
"""

import copy
from datetime import date

import pytest

pytest.importorskip("pyspark")

from conftest import default_temp, default_wind, observation, weather_records  # noqa: E402
from src.silver.bronze_reader import DOMAIN_WEATHER, explode_records, read_bronze_files  # noqa: E402
from src.silver.reference import build_dim_station  # noqa: E402
from src.silver.weather import clean_weather  # noqa: E402

pytestmark = pytest.mark.spark


def _clean(spark, bronze):
    records = explode_records(read_bronze_files(spark, bronze.root, DOMAIN_WEATHER))
    return clean_weather(records, build_dim_station(spark))


def _by_hour(table):
    return {r.utc_hour.hour + 24 * (r.utc_hour.day - 15): r for r in table.collect()}


def _issues(issues):
    return {(i.issue, i.action, i.record_index, i.field) for i in issues.collect()}


def test_only_the_2m_hourly_temperature_and_10m_wind_are_used(spark, bronze):
    day = "2024-01-15"
    records = []
    for h in range(24):
        # Like a Blindern file from before we requested hourly data only: 10-minute series next to
        # the hourly ones, and temperature at 10 m listed before the 2 m one.
        records.append({"sourceId": "SN18700:0", "referenceTime": f"{day}T{h:02d}:00:00.000Z", "observations": [
            observation("air_temperature", default_temp(h) - 0.1, 10, quality=2),
            observation("air_temperature", default_temp(h), 2),
            observation("air_temperature", default_temp(h) + 20, 2, resolution="PT10M"),
            observation("wind_speed", default_wind(h) + 20, 10, resolution="PT10M"),
            observation("wind_speed", default_wind(h), 10),
        ]})
        # Like Sola: a 30-minute series reported 20 minutes past the hour.
        records.append({"sourceId": "SN18700:0", "referenceTime": f"{day}T{h:02d}:20:00.000Z", "observations": [
            observation("air_temperature", default_temp(h) + 25, 2, resolution="PT30M", offset="PT20M"),
        ]})
    bronze.weather("SN18700", day, records)

    table, issues, _ = _clean(spark, bronze)

    rows = _by_hour(table)
    assert sorted(rows) == list(range(24))
    for h, r in rows.items():
        assert (r.temp_celsius, r.wind_speed_mps) == (default_temp(h), default_wind(h))
        assert (r.temp_quality_code, r.wind_quality_code) == (0, 0)
        assert not r.temp_is_imputed and not r.wind_is_imputed
    assert issues.count() == 0


def test_bad_values_are_rejected_and_short_gaps_filled(spark, bronze):
    records = weather_records("2024-01-15")  # observations: [0] 2 m temp, [1] 10 m temp, [2] wind
    records[5]["observations"][0]["value"] = None
    records[8]["observations"][0]["value"] = 999.9
    records[12]["observations"][2]["qualityCode"] = 7  # Frost: "erroneous"
    records[15]["observations"][1]["value"] = None  # a series Silver doesn't use is checked too
    bronze.weather("SN18700", "2024-01-15", records)

    table, issues, _ = _clean(spark, bronze)

    assert _issues(issues) == {
        ("null_value", "rejected", 5, "observations[0].value"),
        ("out_of_range", "rejected", 8, "observations[0].value"),
        ("bad_quality", "rejected", 12, "observations[2].qualityCode"),
        ("null_value", "rejected", 15, "observations[1].value"),
    }
    rows = _by_hour(table)
    assert len(rows) == 24
    assert (rows[5].temp_celsius, rows[5].temp_is_imputed) == (default_temp(4), True)
    assert (rows[8].temp_celsius, rows[8].temp_is_imputed) == (default_temp(7), True)
    assert (rows[12].wind_speed_mps, rows[12].wind_is_imputed, rows[12].wind_quality_code) == (default_wind(11), True, 7)
    assert (rows[15].temp_celsius, rows[15].temp_is_imputed) == (default_temp(15), False)


def test_record_problems_are_rejected_or_removed(spark, bronze):
    records = weather_records("2024-01-15")
    records[3]["referenceTime"] = "not-a-timestamp"
    records.insert(7, copy.deepcopy(records[6]))  # record 6 twice; the copy sits at index 7
    records += weather_records("2024-01-16", hours=[5])  # a record from the next UTC day (index 25)
    bronze.weather("SN18700", "2024-01-15", records)

    table, issues, checked = _clean(spark, bronze)

    assert _issues(issues) == {
        ("bad_timestamp", "rejected", 3, "referenceTime"),
        ("duplicate", "removed", 7, None),
        ("wrong_day", "rejected", 25, "referenceTime"),
    }
    rows = _by_hour(table)
    assert sorted(rows) == list(range(24))  # hour 3 lost its record, then got filled from hour 2
    assert (rows[3].temp_celsius, rows[3].temp_is_imputed, rows[3].wind_is_imputed) == (default_temp(2), True, True)
    assert {r.status for r in checked.collect()} == {"kept", "rejected", "removed"}


def test_only_gaps_of_one_or_two_hours_are_filled(spark, bronze):
    hours = list(range(0, 10)) + [12, 13, 14] + [18, 19, 20, 21, 22]  # gaps: 10-11, 15-17, 23 (+ next day)
    bronze.weather("SN18700", "2024-01-15", weather_records("2024-01-15", hours=hours))
    bronze.weather("SN18700", "2024-01-16", weather_records("2024-01-16", hours=range(1, 6)))  # 00 missing

    rows = _by_hour(_clean(spark, bronze)[0])

    assert rows[10].temp_celsius == rows[11].temp_celsius == default_temp(9)  # 2-hour gap: filled
    assert rows[10].temp_is_imputed and rows[11].wind_is_imputed
    assert not {15, 16, 17} & set(rows)  # 3-hour gap: stays empty
    assert rows[23].temp_is_imputed and rows[24].temp_is_imputed  # 23:00 and 00:00 across the day boundary
    assert max(rows) == 24 + 5  # nothing after the last observation (2024-01-16 05:00)
    assert len(rows) == 10 + 2 + 3 + 5 + 2 + 5


def test_stations_get_their_zone_and_norwegian_date(spark, bronze):
    bronze.weather("SN44560", "2024-01-15", weather_records("2024-01-15", station="SN44560"))

    rows = _by_hour(_clean(spark, bronze)[0])

    assert {r.bidding_zone for r in rows.values()} == {"NO2"}
    assert rows[0].local_date == date(2024, 1, 15)
    assert rows[23].local_date == date(2024, 1, 16)  # 23:00 UTC is midnight in Norway (UTC+1)
    assert (rows[23].year, rows[23].month) == (2024, 1)
