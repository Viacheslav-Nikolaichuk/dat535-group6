"""
Unit tests for the day-ahead price ingestion (src/ingestion/ingest_daily.py --day-ahead): tomorrow on the
Norwegian clock, "not published yet" before ~13:00, and what the workflow is told afterwards.
"""

from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

import pytest

from config.settings import PRICE_ZONES
from conftest import price_records
from src.ingestion import ingest_daily
from src.ingestion.ingest_daily import day_ahead_date, hours_in_oslo_day, run_day_ahead

AFTERNOON = datetime(2026, 11, 3, 12, 30, tzinfo=timezone.utc)  # 13:30 in Oslo
TOMORROW = "2026-11-04"


class _FakeStrom:
    """Price API stand-in: zones in `published` have tomorrow's file, the others answer 404."""

    def __init__(self, published=PRICE_ZONES, hours=None, fail=()):
        self.published, self.hours, self.fail = set(published), hours, set(fail)
        self.calls = []

    def fetch_prices(self, date_str, zone):
        self.calls.append((date_str, zone))
        if zone in self.fail:
            raise RuntimeError("HTTP 500")
        if zone not in self.published:
            raise FileNotFoundError("not found (404)")
        records = price_records(date_str)
        return {"records": records[: self.hours] if self.hours else records, "source_uri": f"fake://{zone}"}


@pytest.fixture
def github_output(tmp_path, monkeypatch):
    path = tmp_path / "github_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(path))

    def read():
        lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
        return dict(line.split("=", 1) for line in lines)
    return read


def _price_files(root: Path):
    return sorted((root / "electricity").rglob("*.json")) if (root / "electricity").exists() else []


@pytest.mark.parametrize("now, expected", [
    (datetime(2026, 11, 3, 12, 30, tzinfo=timezone.utc), date(2026, 11, 4)),
    (datetime(2026, 11, 3, 22, 59, tzinfo=timezone.utc), date(2026, 11, 4)),  # 23:59 in Oslo
    (datetime(2026, 11, 3, 23, 30, tzinfo=timezone.utc), date(2026, 11, 5)),  # 00:30 in Oslo on the 4th
    (datetime(2026, 7, 1, 22, 30, tzinfo=timezone.utc), date(2026, 7, 3)),  # summer time: 00:30 on 2 July
])
def test_tomorrow_follows_the_norwegian_clock(now, expected):
    assert day_ahead_date(now) == expected


@pytest.mark.parametrize("day, hours", [(date(2026, 3, 29), 23), (date(2026, 10, 25), 25), (date(2026, 11, 4), 24)])
def test_hours_in_a_norwegian_day(day, hours):
    assert hours_in_oslo_day(day) == hours


def test_before_publication_nothing_is_written(tmp_path, github_output):
    rc = run_day_ahead(output_dir=tmp_path, strom_client=_FakeStrom(published=()), now_utc=AFTERNOON)

    assert rc == ingest_daily.EXIT_NOT_PUBLISHED
    assert _price_files(tmp_path) == []
    assert not (tmp_path / "_manifests").exists()  # retries before 13:00 leave no trace
    assert github_output() == {"delivery_day": TOMORROW, "published": "false", "changed": "false"}


def test_published_prices_are_written_once(tmp_path, github_output):
    strom = _FakeStrom()
    assert run_day_ahead(output_dir=tmp_path, strom_client=strom, now_utc=AFTERNOON) == ingest_daily.EXIT_OK

    files = _price_files(tmp_path)
    assert len(files) == 5 and all(f"{TOMORROW.replace('-', '_')}__" in f.name for f in files)
    assert not (tmp_path / "weather").exists()  # tomorrow has no weather
    assert {c[0] for c in strom.calls} == {TOMORROW}
    [manifest] = [json.loads(p.read_text(encoding="utf-8")) for p in (tmp_path / "_manifests").glob("run__*.json")]
    assert manifest["mode"] == "day_ahead" and manifest["status"] == "SUCCESS"
    assert len(manifest["written_files"]) == 5 and manifest["date_from"] == TOMORROW
    assert github_output()["changed"] == "true" and github_output()["published"] == "true"

    # A later retry the same afternoon finds the same files: nothing new, so Silver and Gold can be skipped.
    assert run_day_ahead(output_dir=tmp_path, strom_client=_FakeStrom(), now_utc=AFTERNOON) == ingest_daily.EXIT_OK
    assert len(_price_files(tmp_path)) == 5
    assert len(list((tmp_path / "_manifests").glob("run__*.json"))) == 1
    assert github_output()["changed"] == "false"  # the newest line wins: last value written


def test_some_zones_not_yet_published(tmp_path, github_output):
    rc = run_day_ahead(output_dir=tmp_path, strom_client=_FakeStrom(published=["NO1", "NO2"]), now_utc=AFTERNOON)

    assert rc == ingest_daily.EXIT_NOT_PUBLISHED
    assert len(_price_files(tmp_path)) == 2
    [manifest] = [json.loads(p.read_text(encoding="utf-8")) for p in (tmp_path / "_manifests").glob("run__*.json")]
    assert manifest["status"] == "NOT_PUBLISHED" and manifest["not_published"] == ["NO3", "NO4", "NO5"]
    assert github_output() == {"delivery_day": TOMORROW, "published": "false", "changed": "true"}


def test_a_day_with_missing_hours_is_not_published(tmp_path, github_output):
    rc = run_day_ahead(zones=["NO1"], output_dir=tmp_path, strom_client=_FakeStrom(hours=12), now_utc=AFTERNOON)

    assert rc == ingest_daily.EXIT_NOT_PUBLISHED
    assert len(_price_files(tmp_path)) == 1  # Bronze keeps what the API sent
    assert github_output()["published"] == "false"


def test_a_failed_request_is_an_error(tmp_path, github_output):
    rc = run_day_ahead(output_dir=tmp_path, strom_client=_FakeStrom(fail=["NO3"]), now_utc=AFTERNOON)

    assert rc == ingest_daily.EXIT_PARTIAL_FAILURE
    assert len(_price_files(tmp_path)) == 4
    assert github_output()["published"] == "false"


def test_day_ahead_cannot_be_combined_with_dates(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["ingest_daily.py", "--day-ahead", "--date", "2026-11-01"])
    with pytest.raises(SystemExit) as exc:
        ingest_daily.main()
    assert exc.value.code == 2
