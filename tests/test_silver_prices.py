"""
Unit tests for cleaning prices into silver/prices (src/silver/prices.py).
"""

import copy
from datetime import date

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from conftest import price_records  # noqa: E402
from src.silver.bronze_reader import DOMAIN_PRICES, explode_records, read_bronze_files  # noqa: E402
from src.silver.prices import clean_prices  # noqa: E402

pytestmark = pytest.mark.spark


def _clean(spark, bronze):
    return clean_prices(explode_records(read_bronze_files(spark, bronze.root, DOMAIN_PRICES)))


def _issues(issues):
    return {(i.issue, i.action, i.record_index, i.field) for i in issues.collect()}


def test_daylight_saving_days_have_23_and_25_hours(spark, bronze):
    bronze.prices("NO1", "2026-03-29")
    bronze.prices("NO1", "2026-10-25")
    table, issues, _ = _clean(spark, bronze)

    hours = {
        r.local_date.isoformat(): r.n
        for r in table.groupBy("local_date").agg(F.countDistinct("utc_hour").alias("n")).collect()
    }
    assert hours == {"2026-03-29": 23, "2026-10-25": 25}
    autumn = [r.local_hour for r in table.filter(F.col("local_date") == "2026-10-25").collect()]
    assert autumn.count(2) == 2  # 02:00 happens twice, so local_hour is never a key
    assert issues.count() == 0


def test_hours_are_converted_to_utc(spark, bronze):
    bronze.prices("NO1", "2024-01-15")
    first = _clean(spark, bronze)[0].orderBy("utc_hour").first()

    assert first.utc_hour.isoformat() == "2024-01-14T23:00:00"  # 00:00 Norwegian time = 23:00 UTC
    assert (first.local_date, first.local_hour) == (date(2024, 1, 15), 0)
    assert (first.year, first.month) == (2024, 1)
    assert first.price_nok_kwh == price_records("2024-01-15")[0]["NOK_per_kWh"]
    assert first.is_repaired is False


def test_damaged_rows_are_repaired_to_the_clean_values(spark, bronze):
    clean = price_records("2024-01-15")
    damaged = copy.deepcopy(clean)
    damaged[3]["NOK_per_kWh"] = None
    damaged[4]["NOK_per_kWh"] = 999.0
    damaged[5]["time_start"] = "not-a-timestamp"
    damaged.insert(7, dict(damaged[6]))  # record 6 twice; the copy sits at index 7
    bronze.prices("NO1", "2024-01-15", damaged)

    table, issues, _ = _clean(spark, bronze)

    assert _issues(issues) == {
        ("null_value", "repaired", 3, "NOK_per_kWh"),
        ("out_of_range", "repaired", 4, "NOK_per_kWh"),
        ("bad_timestamp", "repaired", 5, "time_start"),
        ("duplicate", "removed", 7, None),
    }
    rows = table.orderBy("utc_hour").collect()
    assert len(rows) == 24
    for row, original in zip(rows, clean):
        assert row.price_nok_kwh == pytest.approx(original["NOK_per_kWh"], abs=1e-5)
    assert [i for i, r in enumerate(rows) if r.is_repaired] == [3, 4, 5]


def test_rows_that_cannot_be_repaired_are_rejected(spark, bronze):
    records = price_records("2024-01-15")
    records[0]["NOK_per_kWh"] = records[0]["EUR_per_kWh"] = None  # nothing to rebuild from
    records[1]["time_start"] = records[1]["time_end"] = "not-a-timestamp"
    records[3]["time_end"] = "2024-01-15T03:30:00+01:00"  # a 30-minute price
    bronze.prices("NO1", "2024-01-15", records)

    table, issues, checked = _clean(spark, bronze)

    assert _issues(issues) == {
        ("null_value", "rejected", 0, "NOK_per_kWh"),
        ("bad_timestamp", "rejected", 1, "time_start"),
        ("wrong_resolution", "rejected", 3, "time_end"),
    }
    assert table.count() == 21
    assert checked.filter(F.col("status") == "rejected").count() == 3


def test_the_apis_label_for_the_first_2am_hour_in_autumn_is_accepted(spark, bronze):
    records = price_records("2024-10-27")
    records[2]["time_end"] = "2024-10-27T03:00:00+01:00"  # the API in 2022-2025; correct is 02:00+01:00
    bronze.prices("NO1", "2024-10-27", records)

    table, issues, _ = _clean(spark, bronze)

    assert issues.count() == 0
    assert table.count() == 25


def test_a_file_with_an_hour_of_another_day_is_rejected_whole(spark, bronze):
    # The API's 2024-10-27 file for NO1: no second 02:00, and the labels run into the next day.
    records = price_records("2024-10-27")
    records[2]["time_end"] = "2024-10-27T03:00:00+01:00"
    bronze.prices("NO1", "2024-10-27", records[:3] + records[4:] + price_records("2024-10-28")[:1])
    bronze.prices("NO2", "2024-10-27", records)

    table, issues, checked = _clean(spark, bronze)

    assert _issues(issues) == {("wrong_day", "rejected", 24, "time_start")} | {
        ("untrusted_file", "rejected", i, "time_start") for i in range(24)
    }
    assert {r.bidding_zone for r in table.collect()} == {"NO2"}
    assert checked.filter(F.col("status") == "rejected").count() == 25


def test_prices_whose_duration_cannot_be_checked_are_rejected(spark, bronze):
    records = price_records("2024-01-15")
    records[4]["time_end"] = "not-a-timestamp"  # start is fine, but the duration is unknown
    records[6]["time_start"] = "not-a-timestamp"
    records[6]["time_end"] = "2024-01-15T07:30:00+01:00"  # rebuilt start 06:30 is not a whole hour
    bronze.prices("NO1", "2024-01-15", records)

    table, issues, _ = _clean(spark, bronze)

    assert _issues(issues) == {
        ("wrong_resolution", "rejected", 4, "time_end"),
        ("bad_timestamp", "repaired", 6, "time_start"),  # rebuilt from time_end ...
        ("wrong_resolution", "rejected", 6, "time_end"),  # ... but the result is not a whole hour
    }
    assert table.count() == 22


def test_superseded_versions_are_checked_but_not_used(spark, bronze):
    old = price_records("2024-01-15", exr=11.0)
    old[0]["NOK_per_kWh"] = None
    bronze.prices("NO1", "2024-01-15", old, ingested_at="2026-01-01T00:00:00Z")
    bronze.prices("NO1", "2024-01-15", price_records("2024-01-15", exr=12.0), ingested_at="2026-01-02T00:00:00Z")

    table, issues, _ = _clean(spark, bronze)

    assert _issues(issues) == {("null_value", "repaired", 0, "NOK_per_kWh")}
    assert {r.eur_nok_rate for r in table.collect()} == {12.0}
