"""
End-to-end tests for the Gold entrypoint (src/gold/run_gold.py): Bronze -> Silver -> Gold, the status
file, and what happens without Silver or when a check fails.
"""

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
TABLES = ["zone_daily", "price_weather_sensitivity", "price_profile"]


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
    assert len(list((out / "zone_daily").glob("*.parquet"))) == 1  # one small file per table
    assert not (out / gold.STAGING_DIR).exists()

    status = json.loads((out / "_status" / "latest.json").read_text(encoding="utf-8"))
    assert status["status"] == "SUCCESS" and status["is_sample"] is False
    assert status["silver"]["run_id"] == gold.latest_silver_run(silver_dir)["run_id"]
    assert {name: t["rows"] for name, t in status["tables"].items()} == {
        "zone_daily": 10, "price_weather_sensitivity": 5, "price_profile": 120,
    }
    assert (status["first_date"], status["last_date"]) == ("2024-01-15", "2024-01-16")
    assert status["freshness"]["data_through"] == "2024-01-16"
    assert status["freshness"]["complete_day_per_zone"]["NO4"] == "2024-01-16"
    assert all(c["passed"] for c in status["checks"])
    assert len(list((out / "_status" / "runs").glob("run__*.json"))) == 1


def test_without_silver_is_a_configuration_error(spark, tmp_path):
    assert gold.run_gold(tmp_path / "no_silver", tmp_path / "gold", spark=spark) == gold.EXIT_CONFIG_ERROR
    assert not (tmp_path / "gold").exists()


def test_a_failed_check_writes_nothing_and_keeps_the_previous_gold(spark, bronze, tmp_path, monkeypatch):
    silver_dir = _silver(spark, bronze, tmp_path)
    out = tmp_path / "gold"
    assert gold.run_gold(silver_dir, out, spark=spark) == gold.EXIT_OK
    latest_before = (out / "_status" / "latest.json").read_text(encoding="utf-8")

    monkeypatch.setattr(gold, "run_checks", lambda tables, silver_price_hours: [
        Check("zone_daily.duplicate_keys", True, False, 1.0, "forced")
    ])
    assert gold.run_gold(silver_dir, out, spark=spark) == gold.EXIT_CHECKS_FAILED

    assert spark.read.parquet(str(out / "zone_daily")).count() == 10  # previous Gold untouched
    assert (out / "_status" / "latest.json").read_text(encoding="utf-8") == latest_before
    runs = [json.loads(p.read_text(encoding="utf-8")) for p in (out / "_status" / "runs").glob("run__*.json")]
    assert sorted(r["status"] for r in runs) == ["FAILED", "SUCCESS"]
