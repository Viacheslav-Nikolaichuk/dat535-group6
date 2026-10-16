"""
Unit tests for the historical backfill wrapper (no network).
"""

from datetime import date
import json
from pathlib import Path

import pytest

from src.ingestion import ingest_daily
from src.ingestion.backfill_historical import run_backfill, split_by_month
from src.ingestion.frost_client import FrostClient
from tests.test_ingestion import SAMPLE_PRICES, _NoCredsFrost

TODAY = date(2026, 10, 15)


class _RecordingClients:
    """Fake Strømpris + Frost pair recording requested dates; `fail_dates` raise on prices."""

    def __init__(self, fail_dates=()):
        self.dates = []
        self.fail_dates = set(fail_dates)
        outer = self

        class _Frost(FrostClient):
            def __init__(self):
                super().__init__(client_id="fake")

            def fetch_observations(self, date_str, station_id, **kwargs):
                outer.dates.append(date_str)
                return {"records": SAMPLE_PRICES, "source_uri": "fake://", "is_synthetic": False}

        self.frost = _Frost()

    def fetch_prices(self, date_str, zone):
        self.dates.append(date_str)
        if date_str in self.fail_dates:
            raise ConnectionError("simulated outage")
        return {"records": SAMPLE_PRICES, "source_uri": "fake://"}


def _backfill(tmp_path, clients, start="2024-01-30", end="2024-03-02", **kwargs):
    return run_backfill(
        start, end, request_delay=0, output_dir=tmp_path,
        strom_client=clients, frost_client=clients.frost, today=TODAY, **kwargs,
    )


def _manifests(bronze_dir: Path):
    return [json.loads(p.read_text(encoding="utf-8")) for p in (bronze_dir / "_manifests").glob("run__*.json")]


def test_split_by_month():
    dates = ["2024-01-30", "2024-01-31", "2024-02-01", "2024-03-01"]
    assert split_by_month(dates) == [["2024-01-30", "2024-01-31"], ["2024-02-01"], ["2024-03-01"]]


def test_backfill_writes_one_run_and_manifest_per_month(tmp_path: Path):
    assert _backfill(tmp_path, _RecordingClients()) == ingest_daily.EXIT_OK

    ranges = sorted((m["date_from"], m["date_to"]) for m in _manifests(tmp_path))
    assert ranges == [("2024-01-30", "2024-01-31"), ("2024-02-01", "2024-02-29"), ("2024-03-01", "2024-03-02")]
    assert len({m["batch_id"] for m in _manifests(tmp_path)}) == 3
    # 33 days x (5 zones + 6 stations)
    assert sum(len(m["written_files"]) for m in _manifests(tmp_path)) == 33 * 11


def test_backfill_rerun_skips_completed_months(tmp_path: Path):
    _backfill(tmp_path, _RecordingClients())
    clients = _RecordingClients()
    assert _backfill(tmp_path, clients) == ingest_daily.EXIT_OK
    assert clients.dates == []
    assert len(_manifests(tmp_path)) == 3


def test_backfill_rerun_retries_only_failed_month_without_duplicates(tmp_path: Path):
    assert _backfill(tmp_path, _RecordingClients(fail_dates={"2024-02-10"})) == ingest_daily.EXIT_PARTIAL_FAILURE

    clients = _RecordingClients()
    assert _backfill(tmp_path, clients) == ingest_daily.EXIT_OK
    assert {d[:7] for d in clients.dates} == {"2024-02"}

    retry = [m for m in _manifests(tmp_path) if m["status"] == "SUCCESS" and m["date_from"] == "2024-02-01"][0]
    assert retry["files"]["created"] == 5  # the 5 zone prices that failed on 2024-02-10
    assert retry["files"]["unchanged"] == 29 * 11 - 5  # everything else is not rewritten


@pytest.mark.parametrize("end, ok", [("2026-10-11", True), ("2026-10-12", False), ("2026-10-14", False)])
def test_backfill_refuses_daily_lookback_window(tmp_path: Path, end, ok):
    clients = _RecordingClients()
    if ok:
        assert _backfill(tmp_path, clients, start="2026-10-11", end=end) == ingest_daily.EXIT_OK
    else:
        with pytest.raises(ValueError):
            _backfill(tmp_path, clients, start="2026-10-11", end=end)
        assert clients.dates == []


def test_backfill_stops_on_missing_credentials(tmp_path: Path):
    clients = _RecordingClients()
    rc = run_backfill(
        "2024-01-30", "2024-03-02", request_delay=0, output_dir=tmp_path,
        strom_client=clients, frost_client=_NoCredsFrost(), today=TODAY,
    )
    assert rc == ingest_daily.EXIT_CONFIG_ERROR
    assert clients.dates == []
    assert not (tmp_path / "_manifests").exists()
