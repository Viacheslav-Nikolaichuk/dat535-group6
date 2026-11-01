"""
Writes SYNTHETIC Gold tables so the dashboard can be tried without the VM (Spark can't write files on
Windows). The numbers are invented: seasonal temperatures, prices that rise in the cold, a 2022 price
spike in the south and cheap power in the north. Nothing here is real data.

The tables have the same names and columns as src/gold/run_gold.py writes, and _status/latest.json
says "is_sample": true, so the dashboard shows a "sample data" warning.

Usage:
    python scripts/make_sample_gold.py                       # into <data root>/dev/gold
    python scripts/make_sample_gold.py --output-dir /tmp/gold_sample
    DAT535_GOLD_DIR=/tmp/gold_sample streamlit run dashboard/app.py
"""

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sys

from dateutil import easter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from config.settings import DAT535_ENV, GOLD_DIR, HISTORY_START  # noqa: E402

TABLES = ["zone_daily", "price_weather_sensitivity", "price_profile"]

# Invented zone character: mean temperature, seasonal swing, mean wind, price level (NOK/kWh),
# size of the 2022 spike, and how strongly price follows cold.
ZONES = {
    "NO1": dict(temp=6.5, swing=10.0, wind=3.5, level=0.55, spike=2.6, cold=0.035),
    "NO2": dict(temp=8.0, swing=8.0, wind=5.0, level=0.60, spike=3.2, cold=0.030),
    "NO3": dict(temp=5.5, swing=9.0, wind=4.0, level=0.30, spike=0.6, cold=0.025),
    "NO4": dict(temp=3.5, swing=9.0, wind=5.5, level=0.18, spike=0.2, cold=0.015),
    "NO5": dict(temp=8.0, swing=7.0, wind=4.5, level=0.55, spike=2.8, cold=0.030),
}
PEAK_START_HOUR, PEAK_END_HOUR, MIN_FIT_DAYS = 8, 20, 10
WINTER_MONTHS = (10, 11, 12, 1, 2, 3)


def holidays(years):
    days = set()
    for y in years:
        e = easter.easter(y)
        days |= {date(y, 1, 1), date(y, 5, 1), date(y, 5, 17), date(y, 12, 25), date(y, 12, 26)}
        days |= {e + timedelta(days=d) for d in (-3, -2, 0, 1, 39, 49, 50)}
    return days


def ar1(rng, n, sigma, phi=0.8):
    noise = rng.normal(0, sigma * np.sqrt(1 - phi ** 2), n)
    out = np.empty(n)
    out[0] = noise[0]
    for i in range(1, n):
        out[i] = phi * out[i - 1] + noise[i]
    return out


def make_hourly(start: date, end: date, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    days = pd.date_range(start, end, freq="D")
    n = len(days)
    season = np.cos(2 * np.pi * (days.day_of_year.to_numpy() - 15) / 365.25)  # 1 in mid-January
    spike = np.exp(-(((days - pd.Timestamp("2022-08-20")).days.to_numpy()) / 110.0) ** 2)
    weekday = days.day_of_week.to_numpy() + 1
    hol = holidays(range(start.year, end.year + 1))
    is_holiday = np.array([d.date() in hol for d in days])
    is_weekend = weekday >= 6
    workday = ~is_weekend & ~is_holiday
    hours = np.arange(24)

    frames = []
    for zone, z in ZONES.items():
        temp = z["temp"] - z["swing"] * season + ar1(rng, n, 3.0)
        wind = np.clip(z["wind"] + ar1(rng, n, 1.8, 0.6), 0.3, None)
        level = z["level"] * (1 + z["spike"] * spike + 0.25 * season)
        price = level * (1 + z["cold"] * (5 - temp)) - 0.02 * level * (wind - z["wind"]) + ar1(rng, n, 0.05 * z["level"])

        work_shape = 0.85 + 0.2 * np.exp(-((hours - 8) / 1.8) ** 2) + 0.25 * np.exp(-((hours - 18) / 2.2) ** 2)
        rest_shape = 0.9 + 0.12 * np.exp(-((hours - 19) / 2.5) ** 2)
        shape = np.where(workday[:, None], work_shape, rest_shape)
        solar_dip = 0.35 * np.clip(-season, 0, None)[:, None] * np.exp(-((hours - 13) / 2.5) ** 2)
        hourly_price = price[:, None] * (shape - solar_dip) + rng.normal(0, 0.03 * z["level"], (n, 24))
        hourly_temp = temp[:, None] + 2.5 * np.sin(2 * np.pi * (hours - 9) / 24) + rng.normal(0, 0.4, (n, 24))
        hourly_wind = np.clip(wind[:, None] + rng.normal(0, 0.8, (n, 24)), 0, None)

        hourly_temp[rng.random((n, 24)) < 0.002] = np.nan  # a few missing weather hours
        frames.append(pd.DataFrame({
            "bidding_zone": zone,
            "local_date": np.repeat(days, 24),
            "local_hour": np.tile(hours, n),
            "weekday": np.repeat(weekday, 24),
            "is_weekend": np.repeat(is_weekend, 24),
            "is_holiday": np.repeat(is_holiday, 24),
            "price_nok_kwh": hourly_price.ravel().round(5),
            "temp_celsius": hourly_temp.ravel().round(1),
            "wind_speed_mps": hourly_wind.ravel().round(1),
        }))
    df = pd.concat(frames, ignore_index=True)
    df.loc[df["temp_celsius"].isna(), "wind_speed_mps"] = np.nan
    df["hdh"] = (17.0 - df["temp_celsius"]).clip(lower=0)
    df["year"], df["month"] = df["local_date"].dt.year, df["local_date"].dt.month
    return df


def zone_daily(h: pd.DataFrame) -> pd.DataFrame:
    """The same columns and rules as src/gold/dashboard.build_zone_daily."""
    workday = ~h["is_weekend"] & ~h["is_holiday"]
    peak = workday & (h["local_hour"] >= PEAK_START_HOUR) & (h["local_hour"] < PEAK_END_HOUR)
    h = h.assign(peak_price=h["price_nok_kwh"].where(peak), offpeak_price=h["price_nok_kwh"].where(~peak),
                 negative=h["price_nok_kwh"] < 0)
    g = h.groupby(["bidding_zone", "local_date"], as_index=False)
    d = g.agg(
        weekday=("weekday", "first"), is_weekend=("is_weekend", "first"), is_holiday=("is_holiday", "first"),
        avg_price_nok_kwh=("price_nok_kwh", "mean"), min_price_nok_kwh=("price_nok_kwh", "min"),
        max_price_nok_kwh=("price_nok_kwh", "max"), price_stddev_nok_kwh=("price_nok_kwh", "std"),
        peak_avg_price_nok_kwh=("peak_price", "mean"), offpeak_avg_price_nok_kwh=("offpeak_price", "mean"),
        negative_price_hours=("negative", "sum"), avg_temp_celsius=("temp_celsius", "mean"),
        min_temp_celsius=("temp_celsius", "min"), max_temp_celsius=("temp_celsius", "max"),
        hdh=("hdh", "sum"), avg_wind_mps=("wind_speed_mps", "mean"),
        price_hours=("price_nok_kwh", "count"), weather_hours=("temp_celsius", "count"),
    )
    d["peak_offpeak_ratio"] = (d["peak_avg_price_nok_kwh"] / d["offpeak_avg_price_nok_kwh"]).where(
        d["offpeak_avg_price_nok_kwh"] > 0).round(3)
    d["hdd"] = (d.pop("hdh") / 24).round(2)
    d["hours_in_day"] = 24
    d["is_complete"] = (d["price_hours"] == 24) & (d["weather_hours"] == 24)
    d["year"], d["month"] = d["local_date"].dt.year, d["local_date"].dt.month
    for c in ("avg_price_nok_kwh", "price_stddev_nok_kwh", "peak_avg_price_nok_kwh", "offpeak_avg_price_nok_kwh"):
        d[c] = d[c].round(5)
    for c in ("avg_temp_celsius", "avg_wind_mps"):
        d[c] = d[c].round(2)
    return d


def _fit(x: pd.Series, y: pd.Series):
    mask = x.notna() & y.notna()
    x, y = x[mask].to_numpy(float), y[mask].to_numpy(float)
    if len(x) < MIN_FIT_DAYS or np.ptp(x) == 0 or np.ptp(y) == 0:
        return None, None
    return round(float(np.corrcoef(x, y)[0, 1]), 3), float(np.polyfit(x, y, 1)[0])


def sensitivity(h: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    """The same columns and rules as src/gold/dashboard.build_price_weather_sensitivity."""
    keys = ["bidding_zone", "year", "month"]
    m = h.assign(negative=h["price_nok_kwh"] < 0).groupby(keys, as_index=False).agg(
        avg_price_nok_kwh=("price_nok_kwh", "mean"), avg_temp_celsius=("temp_celsius", "mean"),
        hdh=("hdh", "sum"), avg_wind_mps=("wind_speed_mps", "mean"), negative_price_hours=("negative", "sum"),
        price_hours=("price_nok_kwh", "count"), weather_hours=("temp_celsius", "count"),
        hours=("local_hour", "size"),
    )
    m["hdd"] = (m.pop("hdh") / 24).round(1)
    fits = []
    for key, days in daily[daily["is_complete"]].groupby(keys):
        corr_t, slope_t = _fit(days["avg_temp_celsius"], days["avg_price_nok_kwh"])
        corr_w, slope_w = _fit(days["avg_wind_mps"], days["avg_price_nok_kwh"])
        fits.append((*key, len(days), corr_t, None if slope_t is None else round(-slope_t * 100, 2),
                     corr_w, None if slope_w is None else round(slope_w * 100, 2)))
    fit_df = pd.DataFrame(fits, columns=keys + ["fit_days", "corr_temp_price", "ore_per_degree_colder",
                                                "corr_wind_price", "ore_per_mps_wind"])
    out = m.merge(fit_df, on=keys, how="left")
    out["fit_days"] = out["fit_days"].fillna(0).astype("int32")
    out["avg_price_nok_kwh"] = out["avg_price_nok_kwh"].round(5)
    return out


def profile(h: pd.DataFrame) -> pd.DataFrame:
    """The same columns and rules as src/gold/dashboard.build_price_profile."""
    h = h.assign(
        season=np.where(h["month"].isin(WINTER_MONTHS), "winter", "summer"),
        day_type=np.where(~h["is_weekend"] & ~h["is_holiday"], "workday", "weekend_holiday"),
    )
    p = h.groupby(["bidding_zone", "year", "season", "day_type", "local_hour"], as_index=False).agg(
        avg_price_nok_kwh=("price_nok_kwh", "mean"), n_hours=("price_nok_kwh", "count"))
    p["avg_price_nok_kwh"] = p["avg_price_nok_kwh"].round(5)
    return p


def write(out: Path, tables, start: date, end: date) -> None:
    for name, df in tables.items():
        df = df.copy()
        if "local_date" in df.columns:
            df["local_date"] = df["local_date"].dt.date
        for c in df.columns:
            if df[c].dtype == "int64":
                df[c] = df[c].astype("int32")
        target = out / name
        shutil.rmtree(target, ignore_errors=True)
        target.mkdir(parents=True)
        df.to_parquet(target / "part-00000-sample.parquet", index=False)

    daily = tables["zone_daily"]
    complete = daily[daily["is_complete"]].groupby("bidding_zone")["local_date"].max().dt.date
    prices = daily[daily["price_hours"] == daily["hours_in_day"]].groupby("bidding_zone")["local_date"].max().dt.date
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status = {
        "run_id": "sample",
        "status": "SUCCESS",
        "environment": "sample",
        "started_at_utc": now,
        "built_at_utc": now,
        "silver": {"run_id": None, "finished_at_utc": None, "input": "synthetic"},
        "tables": {name: {"rows": len(df)} for name, df in tables.items()},
        "first_date": start.isoformat(),
        "last_date": end.isoformat(),
        "freshness": {
            "complete_day_per_zone": {z: d.isoformat() for z, d in complete.items()},
            "price_day_per_zone": {z: d.isoformat() for z, d in prices.items()},
            "data_through": min(complete).isoformat(),
            "prices_through": min(prices).isoformat(),
        },
        "use_cases": {"prices_weather": TABLES},
        "checks": [],
        "is_sample": True,
    }
    (out / "_status").mkdir(parents=True, exist_ok=True)
    (out / "_status" / "latest.json").write_text(json.dumps(status, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Write synthetic Gold tables for trying the dashboard")
    parser.add_argument("--output-dir", type=Path, default=GOLD_DIR, help=f"default {GOLD_DIR}")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    if DAT535_ENV == "prod":
        print("Refusing to write sample data with DAT535_ENV=prod.")
        return 2
    latest = args.output_dir / "_status" / "latest.json"
    if latest.exists() and not json.loads(latest.read_text(encoding="utf-8")).get("is_sample"):
        print(f"Refusing to overwrite real Gold in {args.output_dir}; pass another --output-dir.")
        return 2

    start = date.fromisoformat(HISTORY_START)
    end = (datetime.now(timezone.utc) + timedelta(hours=1)).date() - timedelta(days=1)  # yesterday, Norway
    hourly = make_hourly(start, end, args.seed)
    daily = zone_daily(hourly)
    tables = {"zone_daily": daily, "price_weather_sensitivity": sensitivity(hourly, daily),
              "price_profile": profile(hourly)}
    write(args.output_dir, tables, start, end)
    print(f"Wrote sample Gold ({len(daily)} zone-days, {start} to {end}) to {args.output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
