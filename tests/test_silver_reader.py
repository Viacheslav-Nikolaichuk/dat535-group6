"""
Unit tests for reading Bronze into Spark (src/silver/bronze_reader.py): lineage, truncated and
empty files, synthetic data, and the choice of one version per zone or station and day.
"""

from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from conftest import price_records  # noqa: E402
from src.silver.bronze_reader import (  # noqa: E402
    DOMAIN_PRICES,
    DOMAIN_WEATHER,
    explode_records,
    file_issues,
    read_bronze_files,
)

pytestmark = pytest.mark.spark

EARLY, LATE = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"


def _truncate(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    path.write_text(text[: len(text) // 2], encoding="utf-8")


def _status_by_file(files):
    return {r.source_file.rsplit("/", 1)[1]: r.status for r in files.collect()}


def test_lineage_columns_come_from_the_file(spark, bronze):
    bronze.prices("NO3", "2024-01-15")
    row = read_bronze_files(spark, bronze.root, DOMAIN_PRICES).collect()[0]

    assert row.entity == "NO3"
    assert row.file_date.isoformat() == "2024-01-15"
    assert row.source_file == "electricity/zone=NO3/year=2024/month=01/2024_01_15__20260101T000000Z_batch.json"
    assert row._batch_id == "batch"
    assert row._ingested_at_utc.isoformat() == "2026-01-01T00:00:00"
    assert row.status == "used" and row.is_used


def test_newest_readable_version_wins(spark, bronze):
    # A day with an older and a newer readable version: the newer one is used.
    old = bronze.prices("NO1", "2024-01-15", price_records("2024-01-15", exr=11.0), ingested_at=EARLY)
    new = bronze.prices("NO1", "2024-01-15", price_records("2024-01-15", exr=12.0), ingested_at=LATE)
    # A truncated newest version falls back to the older one.
    old_ok = bronze.prices("NO1", "2024-01-16", ingested_at=EARLY)
    new_bad = bronze.prices("NO1", "2024-01-16", price_records("2024-01-16", exr=12.0), ingested_at=LATE)
    _truncate(new_bad)
    # An empty newest version falls back to the older one too.
    old_full = bronze.prices("NO1", "2024-01-17", ingested_at=EARLY)
    new_empty = bronze.prices("NO1", "2024-01-17", records=[], ingested_at=LATE)
    # A day whose only version is truncated has nothing to use.
    only_bad = bronze.prices("NO1", "2024-01-18")
    _truncate(only_bad)

    status = _status_by_file(read_bronze_files(spark, bronze.root, DOMAIN_PRICES))

    assert status == {
        old.name: "superseded", new.name: "used",
        old_ok.name: "used", new_bad.name: "corrupt",
        old_full.name: "used", new_empty.name: "empty",
        only_bad.name: "corrupt",
    }


def test_file_issues_log_truncated_and_empty_files(spark, bronze):
    bad = bronze.prices("NO1", "2024-01-15")
    _truncate(bad)
    bronze.prices("NO2", "2024-01-15", records=[])
    bronze.prices("NO3", "2024-01-15")

    issues = file_issues(read_bronze_files(spark, bronze.root, DOMAIN_PRICES), "prices").collect()

    by_issue = {i.issue: i for i in issues}
    assert set(by_issue) == {"corrupt_file", "empty_file"}
    assert by_issue["corrupt_file"].action == "rejected"
    assert by_issue["corrupt_file"].source_file.endswith(bad.name)
    assert by_issue["corrupt_file"].raw_value.startswith("{")
    assert by_issue["empty_file"].action == "excluded"
    assert all(i.record_index is None for i in issues)


def test_synthetic_files_are_excluded_unless_allowed(spark, bronze):
    bronze.weather("SN18700", "2024-01-15", is_synthetic=True)

    assert _status_by_file(read_bronze_files(spark, bronze.root, DOMAIN_WEATHER)) == {
        "2024_01_15__20260101T000000Z_batch.json": "synthetic"
    }
    allowed = read_bronze_files(spark, bronze.root, DOMAIN_WEATHER, allow_synthetic=True)
    assert [r.status for r in allowed.collect()] == ["used"]


def test_explode_records_checks_superseded_versions_but_marks_only_used_rows(spark, bronze):
    bronze.prices("NO1", "2024-01-15", price_records("2024-01-15", exr=11.0), ingested_at=EARLY)
    bronze.prices("NO1", "2024-01-15", price_records("2024-01-15", exr=12.0), ingested_at=LATE)

    records = explode_records(read_bronze_files(spark, bronze.root, DOMAIN_PRICES)).collect()

    assert len(records) == 48
    used = [r for r in records if r.is_used]
    assert len(used) == 24 and {r.rec.EXR for r in used} == {12.0}
    assert sorted(r.record_index for r in used) == list(range(24))


def test_missing_domain_folder_gives_an_empty_frame(spark, tmp_path):
    files = read_bronze_files(spark, tmp_path / "nothing_here", DOMAIN_WEATHER)
    assert files.count() == 0
    assert explode_records(files).count() == 0
