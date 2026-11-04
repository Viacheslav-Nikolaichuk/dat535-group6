"""
End-to-end tests for the Gold entrypoint (src/gold/run_gold.py): Bronze -> Silver -> Gold, the status
file, and what happens without Silver or when a check fails.
"""

from datetime import date
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from config.settings import PRICE_ZONES  # noqa: E402
from conftest import BronzeFactory, weather_records  # noqa: E402
from src.gold import run_gold as gold  # noqa: E402
from src.silver import run_silver as silver  # noqa: E402
from src.silver.quality import Check  # noqa: E402

pytestmark = pytest.mark.spark

STATIONS = ["SN18700", "SN39040", "SN44560", "SN50540", "SN68860", "SN90450"]
TABLES = ["consumer_prices_hourly", "cheapest_hours", "zone_daily", "price_weather_sensitivity", "price_profile",
          "household_bill_monthly"]


def _silver(spark, bronze: BronzeFactory, tmp_path: Path) -> Path:
    """Silver for two complete Norwegian days (15-16 January 2024). Weather starts on 14 January
    because 00:00 Norwegian time on the 15th is 23:00 UTC on the 14th."""
    for day in ("2024-01-15", "2024-01-16"):
        for zone in PRICE_ZONES:
            bronze.prices(zone, day)
    for day in ("2024-01-14", "2024-01-15", "2024-01-16"):
        for station in STATIONS:
            bronze.weather(station, day, weather_records(day, station))
    out = tmp_path / "silver"
    assert silver.run_silver(bronze.root, out, "bronze", spark=spark) == silver.EXIT_OK
    return out


def test_full_run_writes_tables_and_status(spark, bronze, tmp_path):
    silver_dir = _silver(spark, bronze, tmp_path)
    out = tmp_path / "gold"

    assert gold.run_gold(silver_dir, out, spark=spark) == gold.EXIT_OK

    daily = spark.read.parquet(str(out / "zone_daily"))
    assert daily.count() == 5 * 2
    assert daily.filter("is_complete").count() == 10
    assert spark.read.parquet(str(out / "price_weather_sensitivity")).count() == 5  # one month per zone
    assert spark.read.parquet(str(out / "price_profile")).count() == 5 * 24  # winter workdays only
    bill = spark.read.parquet(str(out / "household_bill_monthly"))
    assert bill.count() == 5 * 2  # January 2024: no support and strømstønad; Norgespris came in 2025
    assert bill.filter("is_final").count() == 0  # two days of January
    assert spark.read.parquet(str(out / "consumer_prices_hourly")).count() == 5 * 48
    assert spark.read.parquet(str(out / "cheapest_hours")).count() == 5 * 2 * 3  # zones x days x block lengths
    assert len(list((out / "zone_daily").glob("*.parquet"))) == 1  # one small file per table
    assert not (out / gold.STAGING_DIR).exists()

    status = json.loads((out / "_status" / "latest.json").read_text(encoding="utf-8"))
    assert status["status"] == "SUCCESS" and status["is_sample"] is False
    assert status["silver"]["run_id"] == gold.latest_silver_run(silver_dir)["run_id"]
    assert {name: t["rows"] for name, t in status["tables"].items()} == {
        "consumer_prices_hourly": 240, "cheapest_hours": 30,
        "zone_daily": 10, "price_weather_sensitivity": 5, "price_profile": 120, "household_bill_monthly": 10,
    }
    # Run today, 2024's data has no "tomorrow": no zone is published for it.
    assert status["day_ahead"]["published_zones"] == []
    assert (status["first_date"], status["last_date"]) == ("2024-01-15", "2024-01-16")
    assert status["freshness"]["data_through"] == "2024-01-16"
    assert status["freshness"]["complete_day_per_zone"]["NO4"] == "2024-01-16"
    assert all(c["passed"] for c in status["checks"])
    assert len(list((out / "_status" / "runs").glob("run__*.json"))) == 1


def test_run_the_day_before_publishes_tomorrow_and_caps_the_bill(spark, bronze, tmp_path):
    silver_dir = _silver(spark, bronze, tmp_path)
    out = tmp_path / "gold"

    # As if Gold ran on 15 January 2024 after the day-ahead run brought in the 16th.
    assert gold.run_gold(silver_dir, out, spark=spark, today=date(2024, 1, 15)) == gold.EXIT_OK

    blocks = spark.read.parquet(str(out / "cheapest_hours")).filter("local_date = '2024-01-16'")
    assert blocks.count() == 5 * 3  # every zone, 1-, 3- and 6-hour blocks
    status = json.loads((out / "_status" / "latest.json").read_text(encoding="utf-8"))
    assert status["day_ahead"] == {"delivery_day": "2024-01-16", "published_zones": PRICE_ZONES}
    # The bill covers the days before the 15th: none here, so it has no rows yet.
    assert spark.read.parquet(str(out / "household_bill_monthly")).count() == 0
    assert all(c["passed"] for c in status["checks"])


def test_without_silver_is_a_configuration_error(spark, tmp_path):
    assert gold.run_gold(tmp_path / "no_silver", tmp_path / "gold", spark=spark) == gold.EXIT_CONFIG_ERROR
    assert not (tmp_path / "gold").exists()


def test_a_failed_check_writes_nothing_and_keeps_the_previous_gold(spark, bronze, tmp_path, monkeypatch):
    silver_dir = _silver(spark, bronze, tmp_path)
    out = tmp_path / "gold"
    assert gold.run_gold(silver_dir, out, spark=spark) == gold.EXIT_OK
    latest_before = (out / "_status" / "latest.json").read_text(encoding="utf-8")

    monkeypatch.setattr(gold, "run_checks", lambda tables, silver_price_hours, **kwargs: [
        Check("zone_daily.duplicate_keys", True, False, 1.0, "forced")
    ])
    assert gold.run_gold(silver_dir, out, spark=spark) == gold.EXIT_CHECKS_FAILED

    assert spark.read.parquet(str(out / "zone_daily")).count() == 10  # previous Gold untouched
    assert (out / "_status" / "latest.json").read_text(encoding="utf-8") == latest_before
    runs = [json.loads(p.read_text(encoding="utf-8")) for p in (out / "_status" / "runs").glob("run__*.json")]
    assert sorted(r["status"] for r in runs) == ["FAILED", "SUCCESS"]
