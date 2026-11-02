"""
Reading Gold and preparing it for the pages. Plain pandas, no Streamlit, so it can be unit tested.

Gold lives in <data root>/<env>/gold/ (config/settings.py, which follows DAT535_ENV). DAT535_GOLD_DIR
overrides it, e.g. to look at a scratch Gold folder.
"""

from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dateutil import tz
import numpy as np
import pandas as pd

from config.price_rules import SCHEME_NONE, SCHEME_NORGESPRIS, SCHEME_STROMSTOTTE, SCHEMES
from config.settings import GOLD_DIR, PRICE_ZONES, ZONES_CONFIG_FILE

OSLO = tz.gettz("Europe/Oslo")

# The dashboard is promised by 06:00 Norwegian time with yesterday complete (use_cases.md, section 3).
READY_BY_HOUR = 6

# Longer ranges are drawn per month instead of per day.
MAX_DAILY_RANGE_DAYS = 400

PRESET_LAST_30 = "Last 30 days"
PRESET_LAST_90 = "Last 90 days"
PRESET_LAST_YEAR = "Last 12 months"
PRESET_ALL = "All"


def gold_dir() -> Path:
    return Path(os.environ.get("DAT535_GOLD_DIR") or GOLD_DIR)


def zone_names() -> Dict[str, str]:
    """NO1 -> "Oslo / Øst-Norge", from config/zones_and_stations.json."""
    zones = json.loads(Path(ZONES_CONFIG_FILE).read_text(encoding="utf-8"))["zones"]
    return {zone: zones.get(zone, {}).get("name", zone) for zone in PRICE_ZONES}


# ---------------------------------------------------------------------- #
# Reading Gold
# ---------------------------------------------------------------------- #
def _read_json(path: Path) -> Optional[Dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def read_status(gold: Path) -> Optional[Dict]:
    """gold/_status/latest.json: the newest successful Gold run, or None before the first one."""
    return _read_json(Path(gold) / "_status" / "latest.json")


def read_newest_run(gold: Path) -> Optional[Dict]:
    """The newest run file in gold/_status/runs/, successful or not."""
    runs = sorted((Path(gold) / "_status" / "runs").glob("run__*.json"), key=lambda p: p.stat().st_mtime)
    return _read_json(runs[-1]) if runs else None


def read_table(path: Path) -> pd.DataFrame:
    """One Gold table (a folder with Parquet files). Dates become pandas timestamps."""
    df = pd.read_parquet(path)
    if "local_date" in df.columns:
        df["local_date"] = pd.to_datetime(df["local_date"])
    return df


# ---------------------------------------------------------------------- #
# Freshness
# ---------------------------------------------------------------------- #
def to_oslo(iso_utc: Optional[str]) -> Optional[datetime]:
    if not iso_utc:
        return None
    return datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(OSLO)


def expected_data_through(now_utc: datetime) -> date:
    """The newest day Gold should be complete for: yesterday after 06:00, otherwise the day before."""
    now = now_utc.astimezone(OSLO)
    return now.date() - timedelta(days=1 if now.hour >= READY_BY_HOUR else 2)


def freshness_warnings(status: Optional[Dict], newest_run: Optional[Dict], now_utc: datetime) -> List[str]:
    """Plain-language warnings for the sidebar; empty when Gold is on time."""
    if status is None:
        return []
    warnings = []
    through = (status.get("freshness") or {}).get("data_through")
    expected = expected_data_through(now_utc)
    if through is None:
        warnings.append("No day is complete in every zone yet.")
    elif date.fromisoformat(through) < expected:
        warnings.append(
            f"Data is late: complete through {through}, but by {READY_BY_HOUR:02d}:00 it should reach {expected}. "
            "The nightly pipeline may have failed."
        )
    if (newest_run and newest_run.get("status") != "SUCCESS"
            and newest_run.get("built_at_utc", "") > status.get("built_at_utc", "")):
        warnings.append(
            f"The latest Gold run ({to_oslo(newest_run.get('built_at_utc')):%d %b %H:%M}) failed its checks; "
            "showing the previous successful run."
        )
    return warnings


# ---------------------------------------------------------------------- #
# Periods
# ---------------------------------------------------------------------- #
def range_presets(first: date, last: date) -> List[str]:
    years = [str(y) for y in range(last.year, first.year - 1, -1)]
    return [PRESET_LAST_30, PRESET_LAST_90, PRESET_LAST_YEAR, *years, PRESET_ALL]


def range_bounds(preset: str, first: date, last: date) -> Tuple[date, date]:
    """Inclusive date bounds for a preset. "Last ..." counts back from the newest day in Gold."""
    if preset == PRESET_LAST_30:
        start = last - timedelta(days=29)
    elif preset == PRESET_LAST_90:
        start = last - timedelta(days=89)
    elif preset == PRESET_LAST_YEAR:
        start = last - timedelta(days=364)
    elif preset.isdigit():
        year = int(preset)
        return max(first, date(year, 1, 1)), min(last, date(year, 12, 31))
    else:
        start = first
    return max(first, start), last


def in_range(df: pd.DataFrame, start: date, end: date, column: str = "local_date") -> pd.DataFrame:
    values = df[column].dt.date
    return df[(values >= start) & (values <= end)]


def months_in_range(monthly: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Rows of a (year, month) table whose month overlaps [start, end], with a `month_start` date column."""
    month_start = pd.to_datetime(dict(year=monthly["year"], month=monthly["month"], day=1))
    first_month = pd.Timestamp(start.year, start.month, 1)
    out = monthly.assign(month_start=month_start)
    return out[(out["month_start"] >= first_month) & (out["month_start"] <= pd.Timestamp(end))]


def use_monthly(start: date, end: date) -> bool:
    return (end - start).days + 1 > MAX_DAILY_RANGE_DAYS


# ---------------------------------------------------------------------- #
# Figures for the prices and weather page (use case 2)
# ---------------------------------------------------------------------- #
def price_series(daily: pd.DataFrame, monthly: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Average price per zone over time in øre/kWh: daily for short ranges, monthly for long ones.

    Columns: date, bidding_zone, price_ore.
    """
    if use_monthly(start, end):
        rows = months_in_range(monthly, start, end)
        return pd.DataFrame({
            "date": rows["month_start"],
            "bidding_zone": rows["bidding_zone"],
            "price_ore": rows["avg_price_nok_kwh"] * 100,
        }).sort_values(["bidding_zone", "date"], ignore_index=True)
    rows = in_range(daily, start, end)
    return pd.DataFrame({
        "date": rows["local_date"],
        "bidding_zone": rows["bidding_zone"],
        "price_ore": rows["avg_price_nok_kwh"] * 100,
    }).sort_values(["bidding_zone", "date"], ignore_index=True)


def spread(series: pd.DataFrame, high: str = "NO2", low: str = "NO4") -> pd.DataFrame:
    """Price difference between two zones (default south NO2 minus north NO4), øre/kWh."""
    wide = series.pivot_table(index="date", columns="bidding_zone", values="price_ore")
    if high not in wide or low not in wide:
        return pd.DataFrame(columns=["date", "spread_ore"])
    return (wide[high] - wide[low]).dropna().rename("spread_ore").reset_index()


def zone_summary(daily_in_range: pd.DataFrame) -> pd.DataFrame:
    """One row per zone for the chosen period. Prices are means of the daily figures, in øre/kWh."""
    g = daily_in_range.groupby("bidding_zone")
    summary = pd.DataFrame({
        "avg_price_ore": g["avg_price_nok_kwh"].mean() * 100,
        "min_price_ore": g["min_price_nok_kwh"].min() * 100,
        "max_price_ore": g["max_price_nok_kwh"].max() * 100,
        "volatility_ore": g["price_stddev_nok_kwh"].mean() * 100,
        "peak_offpeak_ratio": g["peak_offpeak_ratio"].mean(),
        "negative_price_hours": g["negative_price_hours"].sum(),
        "avg_temp_celsius": g["avg_temp_celsius"].mean(),
        "hdd": g["hdd"].sum(),
        "avg_wind_mps": g["avg_wind_mps"].mean(),
        "complete_days": g["is_complete"].sum(),
        "days": g.size(),
    })
    return summary.reindex([z for z in PRICE_ZONES if z in summary.index]).reset_index()


def fit(x: pd.Series, y: pd.Series) -> Optional[Dict[str, float]]:
    """Least-squares line through the points: slope, intercept, correlation and number of points."""
    mask = x.notna() & y.notna()
    x, y = x[mask].to_numpy(dtype=float), y[mask].to_numpy(dtype=float)
    if len(x) < 3 or np.ptp(x) == 0 or np.ptp(y) == 0:
        return None
    slope, intercept = np.polyfit(x, y, 1)
    return {"slope": float(slope), "intercept": float(intercept),
            "corr": float(np.corrcoef(x, y)[0, 1]), "n": int(len(x))}


def profile_for(profile: pd.DataFrame, zone: str, start: date, end: date, season: Optional[str]) -> pd.DataFrame:
    """Average price per hour of the day and day type, øre/kWh, over the years the period touches.

    Years are combined weighted by their number of hours. season None means both seasons.
    """
    rows = profile[(profile["bidding_zone"] == zone) & profile["year"].between(start.year, end.year)]
    if season:
        rows = rows[rows["season"] == season]
    weighted = rows.assign(total=rows["avg_price_nok_kwh"] * rows["n_hours"])
    out = weighted.groupby(["day_type", "local_hour"], as_index=False)[["total", "n_hours"]].sum()
    out["price_ore"] = out["total"] / out["n_hours"] * 100
    return out[["day_type", "local_hour", "price_ore", "n_hours"]]


# ---------------------------------------------------------------------- #
# Figures for the monthly household bill page (use case 3)
# ---------------------------------------------------------------------- #
SCHEME_LABELS = {SCHEME_NONE: "No support", SCHEME_STROMSTOTTE: "Strømstønad", SCHEME_NORGESPRIS: "Norgespris"}


def _with_month_start(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(month_start=pd.to_datetime(dict(year=df["year"], month=df["month"], day=1)))


def bill_months(bill: pd.DataFrame) -> pd.DataFrame:
    """Every month of household_bill_monthly, newest first: year, month, month_start, is_final.

    A month is final when it is final in every zone.
    """
    months = bill[bill["scheme"] == SCHEME_NONE].groupby(["year", "month"], as_index=False)["is_final"].all()
    return _with_month_start(months).sort_values("month_start", ascending=False, ignore_index=True)


def _bills_wide(rows: pd.DataFrame, index: str) -> pd.DataFrame:
    """One row per `index` value: kWh, the bill under each scheme, and which scheme was cheaper.

    norgespris_saving_nok is the strømstønad bill minus the Norgespris bill: above zero, Norgespris
    was cheaper. It is empty in months without Norgespris.
    """
    costs = rows.set_index([index, "scheme"])["cost_nok"].unstack().reindex(columns=list(SCHEMES))
    base = rows[rows["scheme"] == SCHEME_NONE].set_index(index)
    saving = costs[SCHEME_STROMSTOTTE] - costs[SCHEME_NORGESPRIS]
    cheaper = pd.Series(np.select(
        [saving > 0, saving < 0, saving == 0],
        [SCHEME_LABELS[SCHEME_NORGESPRIS], SCHEME_LABELS[SCHEME_STROMSTOTTE], "Same"], "–",
    ), index=saving.index)
    return pd.DataFrame({
        "kwh": base["kwh"],
        "no_support_nok": costs[SCHEME_NONE],
        "stromstotte_nok": costs[SCHEME_STROMSTOTTE],
        "norgespris_nok": costs[SCHEME_NORGESPRIS],
        "norgespris_saving_nok": saving.round(2),
        "cheaper": cheaper,
        "is_final": base["is_final"],
        "billed_hours": base["billed_hours"],
        "hours_in_month": base["hours_in_month"],
    })


def bill_for_month(bill: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """The report for one month: one row per zone (see _bills_wide)."""
    rows = bill[(bill["year"] == year) & (bill["month"] == month)]
    wide = _bills_wide(rows, "bidding_zone")
    return wide.reindex([z for z in PRICE_ZONES if z in wide.index]).rename_axis("bidding_zone").reset_index()


def bill_history(bill: pd.DataFrame, zone: str) -> pd.DataFrame:
    """One zone's bills, one row per month and scheme, oldest first (for the chart)."""
    rows = _with_month_start(bill[bill["bidding_zone"] == zone])
    return rows.sort_values(["scheme", "month_start"], ignore_index=True)


def bill_by_month(history: pd.DataFrame) -> pd.DataFrame:
    """One zone's bills, one row per month (see _bills_wide), newest first."""
    return _bills_wide(history, "month_start").sort_index(ascending=False).reset_index()


def household_kwh_per_year(bill: pd.DataFrame, zone: str) -> Optional[float]:
    """kWh the modelled household used in the zone's 12 newest final months, or None with fewer."""
    rows = bill[(bill["bidding_zone"] == zone) & (bill["scheme"] == SCHEME_NONE) & bill["is_final"]]
    newest = rows.sort_values(["year", "month"]).tail(12)
    return float(newest["kwh"].sum()) if len(newest) == 12 else None
