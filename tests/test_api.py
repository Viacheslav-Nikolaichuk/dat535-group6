"""
Tests for the cheapest hours API (api/main.py), on sample Gold from scripts/make_sample_gold.py.
Skipped when FastAPI or its test client isn't installed (the VM runner's test step).
"""

from datetime import date, datetime, time, timedelta, timezone
import os
import subprocess
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pd = pytest.importorskip("pandas")

from fastapi.testclient import TestClient  # noqa: E402

from api import gold, main  # noqa: E402
from config.settings import PRICE_ZONES, PROJECT_ROOT  # noqa: E402
from dashboard import data  # noqa: E402

DEV_ENV = {**os.environ, "DAT535_ENV": "dev"}


def _sample(path, *args):
    subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "make_sample_gold.py"), "--output-dir", str(path),
                    *args], check=True, capture_output=True, env=DEV_ENV)
    return path


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    return _sample(tmp_path_factory.mktemp("gold_published"))


@pytest.fixture(scope="module")
def not_published(tmp_path_factory):
    return _sample(tmp_path_factory.mktemp("gold_not_published"), "--without-tomorrow")


def _clock(monkeypatch, gold, oslo_time: time):
    """Point the API at `gold` and stop its clock on the sample's "today" at `oslo_time` (Norwegian)."""
    monkeypatch.setenv("DAT535_GOLD_DIR", str(gold))
    tomorrow = date.fromisoformat(data.read_status(gold)["day_ahead"]["delivery_day"])
    today = tomorrow - timedelta(days=1)
    now = datetime.combine(today, oslo_time, tzinfo=data.OSLO).astimezone(timezone.utc)
    monkeypatch.setattr(main, "now_utc", lambda: now)
    return today, tomorrow


@pytest.fixture
def client():
    return TestClient(main.app)


def test_health_and_zones(client, published, monkeypatch):
    _, tomorrow = _clock(monkeypatch, published, time(13, 30))

    health = client.get("/api/v1/health").json()
    assert health["status"] == "ok" and health["is_sample"] is True
    assert health["tomorrow"] == {"date": tomorrow.isoformat(), "published_zones": PRICE_ZONES}
    zones = client.get("/api/v1/zones").json()
    assert [z["zone"] for z in zones] == PRICE_ZONES and zones[0]["name"]


def test_a_day_has_every_hour_and_the_cheapest_blocks(client, published, monkeypatch):
    _, tomorrow = _clock(monkeypatch, published, time(13, 30))

    response = client.get("/api/v1/cheapest-hours/NO2")  # tomorrow is the default
    assert response.status_code == 200
    day = response.json()
    assert (day["zone"], day["date"], day["timezone"], day["units"]) == ("NO2", tomorrow.isoformat(), "Europe/Oslo", "NOK/kWh")
    assert len(day["hours"]) == 24 and day["hours"][0]["local_time"] == "00:00"
    assert set(day["hours"][0]) == {"start_utc", "end_utc", "local_time", "spot_nok_kwh", "price_with_vat_nok_kwh",
                                    "price_with_stromstotte_nok_kwh", "norgespris_nok_kwh"}
    assert [b["block_hours"] for b in day["cheapest"]] == [1, 3, 6]

    # The same numbers as the Gold table.
    gold = data.read_table(published / "cheapest_hours")
    six = gold[(gold["bidding_zone"] == "NO2") & (gold["local_date"].dt.date == tomorrow) & (gold["block_hours"] == 6)].iloc[0]
    block = day["cheapest"][2]
    assert (block["start_time"], block["end_time"]) == (six["start_time"], six["end_time"])
    assert block["avg_price_nok_kwh"] == pytest.approx(six["avg_price_nok_kwh"])
    assert block["saving_vs_17_nok_kwh"] == pytest.approx(six["saving_nok_kwh"])


def test_today_and_a_date_in_the_history(client, published, monkeypatch):
    today, _ = _clock(monkeypatch, published, time(13, 30))

    assert client.get("/api/v1/cheapest-hours/NO1?day=today").json()["date"] == today.isoformat()
    assert client.get("/api/v1/cheapest-hours/NO4?day=2024-02-29").json()["date"] == "2024-02-29"
    missing = client.get("/api/v1/cheapest-hours/NO4?day=2023-06-01")  # before the price rules: not in Gold
    assert missing.status_code == 404 and "2023-06-01" in missing.json()["detail"]


def test_one_block_for_a_charger(client, published, monkeypatch):
    _, tomorrow = _clock(monkeypatch, published, time(13, 30))

    block = client.get("/api/v1/cheapest-hours/NO1/block?hours=6").json()
    assert block["zone"] == "NO1" and block["date"] == tomorrow.isoformat() and block["block_hours"] == 6
    assert block["end_utc"] > block["start_utc"] and block["saving_vs_17_nok_kwh"] >= 0
    assert client.get("/api/v1/cheapest-hours/NO1/block").json()["block_hours"] == 3  # the default
    wrong = client.get("/api/v1/cheapest-hours/NO1/block?hours=2")
    assert wrong.status_code == 422 and "1, 3, 6" in wrong.json()["detail"]
    assert client.get("/api/v1/cheapest-hours/NO1/block?hours=six").status_code == 422


def test_tomorrow_before_publication_says_when_to_retry(client, not_published, monkeypatch):
    _, tomorrow = _clock(monkeypatch, not_published, time(12, 0))

    response = client.get("/api/v1/cheapest-hours/NO1")
    assert response.status_code == 503 and response.headers["Retry-After"] == str(main.RETRY_AFTER_SECONDS)
    body = response.json()
    assert body["published"] is False and body["date"] == tomorrow.isoformat() and "13:00" in body["detail"]
    assert client.get("/api/v1/cheapest-hours/NO1/block").status_code == 503
    assert client.get("/api/v1/health").json()["tomorrow"]["published_zones"] == []
    assert client.get("/api/v1/cheapest-hours/NO1?day=today").status_code == 200  # today is still there


def test_after_the_deadline_tomorrow_is_late(client, not_published, monkeypatch):
    _clock(monkeypatch, not_published, time(15, 0))

    response = client.get("/api/v1/cheapest-hours/NO3")
    assert response.status_code == 503 and "late" in response.json()["detail"]


@pytest.mark.parametrize("url", [
    "/api/v1/cheapest-hours/NO9",
    "/api/v1/cheapest-hours/NO1?day=yesterday",
    "/api/v1/cheapest-hours/NO1?day=2026-13-40",
])
def test_bad_requests_are_explained(client, published, monkeypatch, url):
    _clock(monkeypatch, published, time(13, 30))
    response = client.get(url)
    assert response.status_code == 422 and isinstance(response.json()["detail"], str)


def test_without_gold_the_api_is_unavailable(client, tmp_path, monkeypatch):
    monkeypatch.setenv("DAT535_GOLD_DIR", str(tmp_path / "empty"))
    response = client.get("/api/v1/health")
    assert response.status_code == 503 and "Retry-After" in response.headers


def test_it_is_open_and_has_no_generated_docs(client, published, monkeypatch):
    _clock(monkeypatch, published, time(13, 30))

    assert client.get("/api/v1/zones").status_code == 200  # no key needed
    for url in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(url).status_code == 404
    index = client.get("/").json()
    assert index["name"] == "DAT535 cheapest hours API"
    assert "/api/v1/cheapest-hours/{zone}/block?hours=6&day=tomorrow" in index["endpoints"]


def test_the_functions_behind_the_page_give_the_same_answers(client, not_published, monkeypatch):
    """The dashboard's API page calls api/gold.py directly: its answers must equal the HTTP ones."""
    _clock(monkeypatch, not_published, time(12, 0))
    status, tables = main.gold.get()

    for url, answer in [
        ("/api/v1/cheapest-hours/NO2?day=today", gold.day_answer(tables, "NO2", "today", main.now_utc())),
        ("/api/v1/cheapest-hours/NO2/block?hours=6", gold.block_answer(tables, "NO2", 6, "tomorrow", main.now_utc())),
        ("/api/v1/health", gold.health_answer(status, tables, main.now_utc())),
    ]:
        response = client.get(url)
        assert (response.status_code, response.json()) == (answer.status, answer.body)
        assert response.headers.get("Retry-After") == answer.headers.get("Retry-After")
