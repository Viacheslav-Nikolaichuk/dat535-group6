"""
End-to-end tests for the Silver entrypoint (src/silver/run_silver.py): written tables, checks,
the run manifest, and cleaning a chaos copy compared with clean Bronze.
"""

import json
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyspark")

from config.settings import PRICE_ZONES  # noqa: E402
from conftest import BronzeFactory, weather_records  # noqa: E402
from src.ingestion.chaos_injector import run_chaos_injection  # noqa: E402
from src.silver import run_silver as silver  # noqa: E402

pytestmark = pytest.mark.spark

STATIONS = ["SN18700", "SN39040", "SN44560", "SN50540", "SN68860", "SN90450"]
TABLES = ["prices", "weather_hourly", "zone_hourly", "dim_date", "dim_station", "dq_issues", "dq_metrics"]


def _fill(bronze: BronzeFactory, days) -> None:
    for day in days:
        for zone in PRICE_ZONES:
            bronze.prices(zone, day)
        for station in STATIONS:
            bronze.weather(station, day, weather_records(day, station))


def _manifests(out: Path):
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted((out / "_runs").glob("run__*.json"))]


def test_full_run_writes_every_table_and_a_manifest(spark, bronze, tmp_path):
    _fill(bronze, ["2024-01-15", "2024-01-16"])
    out = tmp_path / "silver"

    assert silver.run_silver(bronze.root, out, "bronze", spark=spark) == silver.EXIT_OK

    for table in TABLES:
        assert spark.read.parquet(str(out / table)).count() >= 0  # every table exists and is readable
    zone_hourly = spark.read.parquet(str(out / "zone_hourly"))
    assert zone_hourly.count() == 5 * 48
    assert len(list((out / "zone_hourly" / "year=2024" / "month=1").glob("*.parquet"))) == 1  # one file per month

    [manifest] = _manifests(out)
    assert manifest["status"] == "SUCCESS" and manifest["chaos"] is None
    assert manifest["rows"]["zone_hourly"] == 240 and manifest["rows"]["prices"] == 240
    assert all(c["passed"] for c in manifest["checks"] if c["hard"])
    assert manifest["freshness"]["newest_price_hour_utc"]["NO1"] == "2024-01-16T22:00:00Z"
    assert manifest["freshness"]["newest_weather_hour_utc"]["NO2"] == "2024-01-16T23:00:00Z"
    assert spark.read.parquet(str(out / "dq_issues")).count() == 0  # clean input, empty but readable

    metrics = {(r.source, r.metric): r.value for r in spark.read.parquet(str(out / "dq_metrics")).collect()}
    assert metrics[("zone_hourly", "rows")] == 240
    assert metrics[("zone_hourly", "price_completeness_pct")] == 100.0
    assert metrics[("prices", "files_used")] == 10


def test_rerun_replaces_the_tables_and_appends_metrics(spark, bronze, tmp_path):
    _fill(bronze, ["2024-01-15"])
    out = tmp_path / "silver"

    for _ in range(2):
        assert silver.run_silver(bronze.root, out, "bronze", spark=spark) == silver.EXIT_OK

    assert spark.read.parquet(str(out / "zone_hourly")).count() == 5 * 24
    assert spark.read.parquet(str(out / "dq_metrics")).select("run_id").distinct().count() == 2
    assert len(_manifests(out)) == 2


def test_a_failed_hard_check_writes_nothing(spark, bronze, tmp_path, monkeypatch):
    _fill(bronze, ["2024-01-15"])
    build = silver.build_zone_hourly

    def with_duplicate_row(*args):
        table = build(*args)
        return table.unionByName(table.limit(1))

    monkeypatch.setattr(silver, "build_zone_hourly", with_duplicate_row)
    out = tmp_path / "silver"

    assert silver.run_silver(bronze.root, out, "bronze", spark=spark) == silver.EXIT_CHECKS_FAILED

    assert [p.name for p in out.iterdir()] == ["_runs"]  # only the manifest, no tables
    [manifest] = _manifests(out)
    assert manifest["status"] == "FAILED"
    assert {c["name"] for c in manifest["checks"] if c["hard"] and not c["passed"]} == {
        "zone_hourly.duplicate_keys",
        "zone_hourly.grid_rows",
    }


def test_chaos_copy_is_cleaned_and_every_injected_error_found(spark, tmp_path):
    clean = BronzeFactory(tmp_path / "bronze")
    _fill(clean, [f"2024-01-{d:02d}" for d in range(10, 16)])
    chaos_dir = tmp_path / "bronze_chaos"
    run_chaos_injection(clean.root, chaos_dir, rate=0.2, seed=7)

    assert silver.run_silver(chaos_dir, tmp_path / "silver_chaos", "bronze_chaos", spark=spark) == silver.EXIT_OK
    assert silver.run_silver(clean.root, tmp_path / "silver_clean", "bronze", spark=spark) == silver.EXIT_OK

    [manifest] = _manifests(tmp_path / "silver_chaos")
    chaos = manifest["chaos"]
    assert {"duplicate", "null_field", "out_of_range", "bad_timestamp", "corrupt_file"} <= set(chaos["recall"])
    assert chaos["found"] == chaos["events"] > 0
    assert chaos["recall_all"] == 1.0 and set(chaos["recall"].values()) == {1.0}
    assert chaos["unexplained_issues"] == 0

    # Only truncated price files (no older version to fall back to) leave hours without a price.
    events = json.loads(next((chaos_dir / "_manifests").glob("run__*.json")).read_text(encoding="utf-8"))["events"]
    lost_days = {
        (e["file"].split("/")[1][len("zone="):], e["file"].rsplit("/", 1)[1][:10].replace("_", "-"))
        for e in events if e["type"] == "corrupt_file" and e["file"].startswith("electricity/")
    }
    cols = ["bidding_zone", "utc_hour", "local_date", "price_nok_kwh", "temp_celsius", "station_count", "weather_is_imputed"]
    chaos_rows = spark.read.parquet(str(tmp_path / "silver_chaos" / "zone_hourly")).select(cols)
    clean_rows = {(r.bidding_zone, r.utc_hour): r for r in
                  spark.read.parquet(str(tmp_path / "silver_clean" / "zone_hourly")).select(cols).collect()}
    rows = chaos_rows.collect()
    assert len(rows) == len(clean_rows)
    for r in rows:
        ref = clean_rows[(r.bidding_zone, r.utc_hour)]
        if r.price_nok_kwh is None:
            assert (r.bidding_zone, r.local_date.isoformat()) in lost_days
        else:
            assert r.price_nok_kwh == pytest.approx(ref.price_nok_kwh, abs=1e-5)
        # Weather that was not gap-filled and comes from the same stations is untouched.
        if r.temp_celsius is not None and not r.weather_is_imputed and r.station_count == ref.station_count:
            assert r.temp_celsius == pytest.approx(ref.temp_celsius, abs=1e-9)


def test_missing_input_directory_is_a_configuration_error(monkeypatch, tmp_path):
    monkeypatch.setitem(silver.INPUT_DIRS, "bronze", tmp_path / "missing")
    monkeypatch.setattr(sys, "argv", ["run_silver.py", "--input", "bronze", "--output-dir", str(tmp_path / "out")])
    assert silver.main() == silver.EXIT_CONFIG_ERROR
