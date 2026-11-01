"""
Unit tests for the dashboard's data helpers (dashboard/data.py) and the sample Gold generator.
Plain pandas, no Streamlit or Spark; skipped when pandas isn't installed (the VM runner).
"""

from datetime import date, datetime, timezone
import json
import subprocess
import sys

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from dashboard import data  # noqa: E402
from config.settings import PROJECT_ROOT  # noqa: E402

FIRST, LAST = date(2021, 11, 1), date(2026, 10, 30)


@pytest.mark.parametrize("preset, expected", [
    (data.PRESET_LAST_30, (date(2026, 10, 1), LAST)),
    (data.PRESET_LAST_90, (date(2026, 8, 2), LAST)),
    (data.PRESET_LAST_YEAR, (date(2025, 10, 31), LAST)),  # 365 days including the last
    ("2022", (date(2022, 1, 1), date(2022, 12, 31))),
    ("2021", (FIRST, date(2021, 12, 31))),  # clipped to the first day in Gold
    ("2026", (date(2026, 1, 1), LAST)),
    (data.PRESET_ALL, (FIRST, LAST)),
])
def test_range_bounds(preset, expected):
    assert data.range_bounds(preset, FIRST, LAST) == expected


def test_presets_list_every_year_newest_first():
    assert data.range_presets(FIRST, LAST) == [
        data.PRESET_LAST_30, data.PRESET_LAST_90, data.PRESET_LAST_YEAR,
        "2026", "2025", "2024", "2023", "2022", "2021", data.PRESET_ALL,
    ]


def test_long_ranges_are_drawn_per_month():
    assert not data.use_monthly(date(2025, 1, 1), date(2025, 12, 31))
    assert data.use_monthly(FIRST, LAST)


def _status(through: str, built_at: str = "2026-10-31T02:10:00Z"):
    return {"built_at_utc": built_at, "freshness": {"data_through": through}}


def test_data_is_on_time_after_the_nightly_run():
    now = datetime(2026, 10, 31, 6, 30, tzinfo=timezone.utc)  # 07:30 in Oslo
    assert data.freshness_warnings(_status("2026-10-30"), None, now) == []


def test_data_is_late_after_six_without_yesterday():
    now = datetime(2026, 10, 31, 6, 30, tzinfo=timezone.utc)
    [warning] = data.freshness_warnings(_status("2026-10-29"), None, now)
    assert "late" in warning and "2026-10-30" in warning


def test_before_six_the_day_before_yesterday_is_enough():
    now = datetime(2026, 10, 31, 3, 0, tzinfo=timezone.utc)  # 04:00 in Oslo, before the deadline
    assert data.freshness_warnings(_status("2026-10-29"), None, now) == []


def test_a_newer_failed_run_is_reported():
    now = datetime(2026, 10, 31, 6, 30, tzinfo=timezone.utc)
    failed = {"status": "FAILED", "built_at_utc": "2026-10-31T02:20:00Z"}
    [warning] = data.freshness_warnings(_status("2026-10-30"), failed, now)
    assert "failed" in warning


def _daily(rows):
    return pd.DataFrame(rows, columns=["bidding_zone", "local_date", "avg_price_nok_kwh"]).assign(
        local_date=lambda d: pd.to_datetime(d["local_date"]))


def test_price_series_and_spread_in_ore():
    daily = _daily([("NO2", "2026-10-01", 1.0), ("NO4", "2026-10-01", 0.25),
                    ("NO2", "2026-10-02", 0.5), ("NO4", "2026-10-02", 0.5)])
    series = data.price_series(daily, pd.DataFrame(), date(2026, 10, 1), date(2026, 10, 2))
    assert series["price_ore"].tolist() == [100.0, 50.0, 25.0, 50.0]
    assert data.spread(series)["spread_ore"].tolist() == [75.0, 0.0]


def test_fit_recovers_a_line_and_refuses_flat_data():
    x = pd.Series([0.0, 1.0, 2.0, 3.0])
    line = data.fit(x, 10 - 2 * x)
    assert line["slope"] == pytest.approx(-2) and line["corr"] == pytest.approx(-1) and line["n"] == 4
    assert data.fit(x, pd.Series([5.0] * 4)) is None


def test_profile_weights_years_by_their_hours():
    profile = pd.DataFrame([
        ("NO1", 2024, "winter", "workday", 8, 1.0, 100),
        ("NO1", 2025, "winter", "workday", 8, 2.0, 300),
        ("NO1", 2025, "summer", "workday", 8, 9.0, 100),
    ], columns=["bidding_zone", "year", "season", "day_type", "local_hour", "avg_price_nok_kwh", "n_hours"])
    [row] = data.profile_for(profile, "NO1", date(2024, 1, 1), date(2025, 12, 31), "winter").itertuples()
    assert row.price_ore == pytest.approx(175.0) and row.n_hours == 400


def test_sample_gold_is_readable_by_the_dashboard(tmp_path):
    out = tmp_path / "gold"
    subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "make_sample_gold.py"), "--output-dir", str(out)],
                   check=True, capture_output=True)

    status = data.read_status(out)
    assert status["is_sample"] is True and status["freshness"]["data_through"]
    daily = data.read_table(out / "zone_daily")
    assert set(daily["bidding_zone"]) == {"NO1", "NO2", "NO3", "NO4", "NO5"}
    assert daily["local_date"].min() == pd.Timestamp("2021-11-01")
    assert status["tables"]["zone_daily"]["rows"] == len(daily)
    summary = data.zone_summary(daily)
    assert summary["bidding_zone"].tolist() == ["NO1", "NO2", "NO3", "NO4", "NO5"]
    assert len(data.read_table(out / "price_profile")) == status["tables"]["price_profile"]["rows"]


def test_sample_generator_refuses_to_overwrite_real_gold(tmp_path):
    out = tmp_path / "gold"
    (out / "_status").mkdir(parents=True)
    (out / "_status" / "latest.json").write_text(json.dumps({"is_sample": False}), encoding="utf-8")
    result = subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "make_sample_gold.py"),
                             "--output-dir", str(out)], capture_output=True, text=True)
    assert result.returncode == 2 and "Refusing" in result.stdout
