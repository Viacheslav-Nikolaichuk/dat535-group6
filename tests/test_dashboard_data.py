"""
Unit tests for the dashboard's data helpers (dashboard/data.py) and the sample Gold generator.
Plain pandas, no Streamlit or Spark; skipped when pandas isn't installed (the VM runner).
"""

from datetime import date, datetime, timezone
import json
import os
import subprocess
import sys

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from dashboard import data  # noqa: E402
from config.settings import PROJECT_ROOT  # noqa: E402

FIRST, LAST = date(2021, 11, 1), date(2026, 10, 30)

# The generator refuses to run under prod, and the pipeline runs these tests with DAT535_ENV=prod on main.
DEV_ENV = {**os.environ, "DAT535_ENV": "dev"}


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


def _bill(rows):
    """household_bill_monthly rows: zone, year, month, scheme, kWh, cost, final."""
    df = pd.DataFrame(rows, columns=["bidding_zone", "year", "month", "scheme", "kwh", "cost_nok", "is_final"])
    return df.assign(billed_hours=744, hours_in_month=744)


BILL = _bill([
    ("NO1", 2025, 9, "no_support", 1000.0, 1500.0, True),
    ("NO1", 2025, 9, "stromstotte", 1000.0, 1300.0, True),
    ("NO1", 2025, 10, "no_support", 1200.0, 1500.0, True),
    ("NO1", 2025, 10, "stromstotte", 1200.0, 1400.0, True),
    ("NO1", 2025, 10, "norgespris", 1200.0, 600.0, True),
    ("NO4", 2025, 10, "no_support", 1500.0, 300.0, True),
    ("NO4", 2025, 10, "stromstotte", 1500.0, 300.0, True),
    ("NO4", 2025, 10, "norgespris", 1500.0, 600.0, True),
    ("NO1", 2025, 11, "no_support", 100.0, 150.0, False),  # month to date
    ("NO4", 2025, 11, "no_support", 100.0, 20.0, True),
])


def test_the_report_compares_the_schemes_per_zone():
    report = data.bill_for_month(BILL, 2025, 10)
    assert report["bidding_zone"].tolist() == ["NO1", "NO4"]
    assert report["norgespris_saving_nok"].tolist() == [800.0, -300.0]
    assert report["cheaper"].tolist() == ["Norgespris", "Strømstønad"]

    september = data.bill_for_month(BILL, 2025, 9)  # before Norgespris
    assert september["norgespris_nok"].isna().all() and september["cheaper"].tolist() == ["–"]


def test_bill_months_are_newest_first_and_final_only_in_every_zone():
    months = data.bill_months(BILL)
    assert list(zip(months["month"], months["is_final"], months["in_progress"])) == [
        (11, False, True), (10, True, False), (9, True, False)]

    # An older month that isn't final has hours without a price: incomplete, not month to date.
    months = data.bill_months(BILL.assign(is_final=BILL["month"] == 10))
    assert months["in_progress"].tolist() == [True, False, False] and not months["is_final"].iloc[2]


def test_bill_history_and_months_for_one_zone():
    history = data.bill_history(BILL, "NO1")
    assert len(history) == 6 and history["month_start"].min() == pd.Timestamp("2025-09-01")
    by_month = data.bill_by_month(history)
    assert by_month["month_start"].tolist() == [pd.Timestamp(f"2025-{m}-01") for m in (11, 10, 9)]
    assert by_month["norgespris_saving_nok"].tolist()[1] == 800.0


def test_household_kwh_per_year_needs_twelve_final_months():
    year = _bill([("NO1", 2025, m, "no_support", 1000.0 + m, 0.0, True) for m in range(1, 13)])
    assert data.household_kwh_per_year(year, "NO1") == 12000.0 + sum(range(1, 13))
    assert data.household_kwh_per_year(year.iloc[1:], "NO1") is None


def test_sample_gold_is_readable_by_the_dashboard(tmp_path):
    out = tmp_path / "gold"
    subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "make_sample_gold.py"), "--output-dir", str(out)],
                   check=True, capture_output=True, env=DEV_ENV)

    status = data.read_status(out)
    assert status["is_sample"] is True and status["freshness"]["data_through"]
    daily = data.read_table(out / "zone_daily")
    assert set(daily["bidding_zone"]) == {"NO1", "NO2", "NO3", "NO4", "NO5"}
    assert daily["local_date"].min() == pd.Timestamp("2021-11-01")
    assert status["tables"]["zone_daily"]["rows"] == len(daily)
    summary = data.zone_summary(daily)
    assert summary["bidding_zone"].tolist() == ["NO1", "NO2", "NO3", "NO4", "NO5"]
    assert len(data.read_table(out / "price_profile")) == status["tables"]["price_profile"]["rows"]

    bill = data.read_table(out / "household_bill_monthly")
    assert len(bill) == status["tables"]["household_bill_monthly"]["rows"]
    months = data.bill_months(bill)
    assert (months["year"].min(), months["month"].iloc[-1]) == (2024, 1)
    assert months["is_final"].iloc[1:].all()  # only the newest month can be month to date
    report = data.bill_for_month(bill, months["year"].iloc[1], months["month"].iloc[1])
    assert report["bidding_zone"].tolist() == ["NO1", "NO2", "NO3", "NO4", "NO5"]
    assert (report["kwh"] > 0).all() and data.household_kwh_per_year(bill, "NO1") > 0


def test_sample_generator_refuses_to_overwrite_real_gold(tmp_path):
    out = tmp_path / "gold"
    (out / "_status").mkdir(parents=True)
    (out / "_status" / "latest.json").write_text(json.dumps({"is_sample": False}), encoding="utf-8")
    result = subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "make_sample_gold.py"),
                             "--output-dir", str(out)], capture_output=True, text=True, env=DEV_ENV)
    assert result.returncode == 2 and "overwrite real Gold" in result.stdout
