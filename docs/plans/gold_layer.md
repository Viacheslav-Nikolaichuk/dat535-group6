# Gold layer and dashboard

**Status:**
- Use case 2 (dashboard: prices and weather) is built and runs nightly in prod since 2 Nov 2026.
- Use case 3 (report: monthly household bill) is built on branch `feat/gold-household-bill` (2 Nov 2026). All tests pass in a Linux container (Python 3.11, Spark 3.5.0). It has not run on the VM yet.
- Use cases 1 and 4 are planned, each with a placeholder page in the dashboard. The Data health page is planned too.

**Reads:** Silver's `zone_hourly` and `dim_date` ([silver_layer.md](silver_layer.md), section 4). The use cases themselves are in [use_cases.md](use_cases.md).

## 1. What Gold is for

Silver answers "what does the data say, once cleaned?". Gold answers "what does this consumer need?". Every Gold table is shaped for one use case: its grain, its units and its time zone are the ones the page or program reads. Gold contains no cleaning. It trusts Silver's checks and adds its own checks on its own tables.

## 2. Decisions

| # | Decision | Why |
|---|---|---|
| G1 | Gold **rebuilds everything** from the newest successful Silver on every run. | Same as Silver (D3). Gold is small: thousands of rows, and seconds to build. |
| G2 | Each Gold table is **one Parquet file**, not partitioned. | The dashboard reads whole tables with pandas, and one file is the fastest to read. |
| G3 | Tables are written to `gold/_staging/` and **swapped into place** only after every table was written and every hard check passed. | A failed or crashed run leaves the previous Gold in place, so the dashboard never shows half-written data. This reuses Silver's `write_table` and `publish`. |
| G4 | A **status file**, `gold/_status/latest.json`, describes the newest successful run. | It answers the guide's question "how will consumers know it is ready?" (section 6). |
| G5 | The dashboard is **Streamlit**, reading Gold directly from disk on the VM. | Python only, with no build step and no separate API. Each use case is one page module. |
| G6 | The dashboard binds to **127.0.0.1** and is opened through an **SSH tunnel**. | Nothing is public, and the OpenStack security group needs no change. |
| G7 | Correlations and slopes use **complete days only** (every hour has a price and a temperature). | A day with gaps would bias its average. The days stay in `zone_daily`, and their hour counts show the gap ([use_cases.md](use_cases.md), 5.2). |
| G8 | The price rules and the household live in **`config/price_rules.py`**, plain Python. | Gold, the sample generator and the dashboard's assumptions text share one copy, and the dashboard needs no Spark to read it. |
| G9 | In the household bill, an hour **without a temperature uses the zone's average `hdh` that month**. | Otherwise one missing weather hour would keep a month preliminary for good. A month is final when every hour has a price; `temp_filled_hours` shows how many hours were filled. |

## 3. Tables

### Use case 2: prices and weather

Prices are in NOK/kWh excluding VAT, as in Silver. The dashboard shows øre/kWh (NOK × 100). Dates are Norwegian local dates.

| Table | One row per | Main columns |
|---|---|---|
| `zone_daily` | zone and Norwegian day | Average, minimum and maximum price, `price_stddev_nok_kwh` (volatility), `peak_avg_price_nok_kwh`, `offpeak_avg_price_nok_kwh`, `peak_offpeak_ratio`, `negative_price_hours`, average/min/max temperature, `hdd`, `avg_wind_mps`, `price_hours`, `weather_hours`, `hours_in_day`, `is_complete`, weekday, weekend, holiday |
| `price_weather_sensitivity` | zone and month | Monthly averages over hours (price, temperature, wind, HDD, negative hours, hour counts), plus `fit_days`, `corr_temp_price`, `ore_per_degree_colder`, `corr_wind_price` and `ore_per_mps_wind` |
| `price_profile` | zone, year, season, day type and Norwegian hour | `avg_price_nok_kwh`, `n_hours` |

Rules:
- **Peak hours** are 08:00–20:00 Norwegian time on workdays, meaning not weekends and not public holidays. The ratio is empty on days without peak hours, and when the off-peak price is zero or below, where a ratio means nothing.
- **Heating degree days:** `hdd = sum(hdh) / 24` from Silver's heating degree hours. Only hours with a temperature count.
- **Slope:** a least-squares fit of the daily average price on the daily average temperature (or wind) over the month's complete days, computed as `covar_samp / var_samp`. `ore_per_degree_colder` is the negative of the temperature slope, in øre: "+4 øre per degree colder". It is empty when a month has fewer than 10 complete days, or the weather didn't vary.
- **Seasons:** winter is October to March (the heating season), summer is April to September. **Day types:** `workday` and `weekend_holiday`.
- On the autumn daylight saving day, 02:00 happens twice. Both hours count towards hour 2 in `price_profile`, and the day has 25 hours in `zone_daily`.

### Use case 3: monthly household bill

| Table | One row per | Main columns |
|---|---|---|
| `household_bill_monthly` | zone, Norwegian month and scheme (`no_support`, `stromstotte`, `norgespris`), from January 2024 | `kwh`, `cost_nok`, `avg_price_nok_kwh`, `support_nok`, `price_hours`, `billed_hours`, `temp_filled_hours`, `hours_in_month`, `is_final` |

Rules (the values are in `config/price_rules.py`, from [use_cases.md](use_cases.md), section 9):
- **Household:** every hour uses `0.6 + 0.1 × hdh` kWh. An hour without a temperature uses the zone's average `hdh` that month (G9). An hour without a price is not billed. If a zone has no temperature at all in a month, nothing can be billed yet: that month keeps its `no_support` row with its hours and no cost.
- **Every scheme is priced hour by hour**, then summed per month:
  - No support: spot × (1 + VAT).
  - Strømstønad: the same, minus 0.9 × max(0, spot − threshold) × (1 + VAT).
  - Norgespris: 0.40 × (1 + VAT).
  - VAT is 25 %, 0 % in NO4.
- **A scheme only has rows in months when it existed:**
  - Norgespris from October 2025 to December 2026.
  - Strømstønad in the years with a known threshold (2024–2026).
- **`support_nok`** is the no-support bill minus this scheme's bill. For Norgespris it is negative when the spot price was below 0.40: the household then paid more.
- **`is_final`:** every hour of the month was billed (`billed_hours = hours_in_month`, from `dim_date`: 743 in March, 745 in October). The month in progress is month to date. It becomes final with the run that brings in its last day, due by 06:00 on the 1st.
- **Left out:** the grid tariff and the supplier's fees, which are the same under every scheme. The 5,000 kWh monthly cap is left out too; a check confirms the household stays below it.

## 4. Checks before writing

All checks are hard: a failure writes nothing, exits with code 1, and the dashboard keeps the previous Gold.

| Check | Why |
|---|---|
| No duplicate or empty keys in any table | Every row is one zone and day (or month, or hour) |
| `price_hours` and `weather_hours` ≤ `hours_in_day`, which is 23, 24 or 25 | A day can't have more hours than the clock gives it |
| `zone_daily` is not empty | An empty Silver must not replace a good Gold |
| Σ `price_hours` in `zone_daily` equals Silver's count of `has_price` | No hour is lost or counted twice between Silver and Gold |
| Correlations lie in [−1, 1] | A sanity check on the statistics |
| Household bill: `price_hours` ≤ `hours_in_month` and `billed_hours` ≤ `price_hours` | A month can't have more hours than the calendar gives it |
| Household bill: each zone-month's `price_hours` equals the sum in `zone_daily` | `zone_daily` already matches Silver, so no hour is lost or counted twice in the bill either |
| Household bill: every Norgespris bill equals kWh × 0.40 × (1 + VAT) | The hourly pricing and the VAT per zone add up |
| Household bill: no month above 5,000 kWh | The support cap we leave out never applies |

The tests recompute the figures from hand-made hourly data. For example, a price that falls exactly 5 øre per degree must give a slope of 5.0 and a correlation of −1. For the bill, the worked example in [use_cases.md](use_cases.md), section 9 (NO1, 2026, spot 1.00 NOK: 1.25 with no support, 0.99 with strømstønad, 0.50 with Norgespris) is a test, and so are NO4 without VAT and the 743- and 745-hour months.

## 5. Freshness and readiness

The nightly pipeline runs Bronze → chaos copy → Silver → **Gold** (`pipeline.yml`, job `gold`). Gold runs only when Silver succeeded, and the whole run takes about 2 minutes.

GitHub starts scheduled runs late. With the cron at 02:00 UTC, the first scheduled runs started 5.5–7 hours late (07:35–09:14 UTC, 30 Oct to 2 Nov 2026), so Gold was ready at about 08:40–10:20 Norwegian time instead of by 06:00. GitHub says scheduled runs are delayed most at the start of each hour, so the cron now runs at 02:17 UTC. We can't control the rest of the delay. While the data is late, the dashboard says so (below).

`gold/_status/latest.json` (newest successful run) and `gold/_status/runs/run__<id>.json` (every run, including failed ones) hold:
- `built_at_utc`, the Silver run Gold was built from, and rows per table;
- `freshness.data_through`: the newest day that is complete (price and weather) in **every** zone. `freshness.prices_through` is the same for prices only;
- the check results.

The dashboard's sidebar reads them on every page:
- "Gold updated" (when the last run finished) and "complete through" (how far the data reaches);
- **Data is late:** after 06:00 Norwegian time, `data_through` should be yesterday. If it is older, the nightly run probably failed, and the dashboard says so instead of quietly showing old numbers;
- **The last Gold run failed:** the newest run file is newer than `latest.json` and did not succeed;
- **Sample data:** the tables came from `scripts/make_sample_gold.py`.

The dashboard caches the tables and uses `built_at_utc` as part of the cache key. A new Gold run therefore shows up on the next page load, without restarting Streamlit.

## 6. The dashboard

```text
dashboard/
├── app.py            # entry point: builds the menu from registry.py and the sidebar status
├── registry.py       # the use cases: question, user, deadline, planned tables, status, page module
├── data.py           # reading Gold and preparing figures (pandas only; unit tested)
├── ui.py             # cached loading and the sidebar status (Streamlit)
├── charts.py         # Altair charts in one style: fixed zone colours, one y-axis, tooltips
└── views/
    ├── overview.py        # home page: one card per use case
    ├── prices_weather.py  # use case 2
    ├── household_bill.py  # use case 3
    └── planned.py         # placeholder for use cases that aren't built yet
```

**The use case 2 page:**
- **What happened:**
  - the average price per zone in the chosen period;
  - price by zone over time, daily or monthly for periods over 400 days;
  - the south-north gap (NO2 − NO4);
  - a zone summary: volatility, peak/off-peak, negative hours, temperature, HDD, complete days.
- **Why**, for one zone:
  - daily temperature against price, and wind against price, each with the fitted line;
  - Gold's monthly sensitivity bars;
  - the price over the day, workdays against weekends, per season.
- Every chart has a table view or a legend. The zone colours pass the colour-blindness checks in light and dark mode.

**The use case 3 page:**
- **The report:**
  - opens on the newest final month (last month);
  - the bill in every zone under each scheme, and which scheme was cheaper;
  - one sentence answering "was Norgespris cheaper?";
  - a CSV download.
- **Month by month, for one zone:**
  - the bill under each scheme;
  - what Norgespris saved compared with strømstønad;
  - a table.
- **The assumptions**, with the household's yearly kWh in that zone.
- **Colours:** strømstønad and Norgespris reuse the workday/weekend colours (slots 6–7), and "no support" is the muted grey reference line. All pairs pass the same checks in both modes.

**Adding a use case:**
1. Build its Gold tables in `src/gold/<use_case>.py` and add them to `run_gold.py`, with key columns in `TABLE_KEYS` and the use case in `USE_CASE_TABLES`.
2. Write `dashboard/views/<name>.py` with a `render()` function. Load the tables with `ui.load_tables([...])`.
3. In `dashboard/registry.py`, set the entry's `view` to `"dashboard.views.<name>"` and its `status` to `READY`. The menu and the overview pick it up.

## 7. What is left

- ✅ The first run on the VM: Gold is built nightly in prod, and the dashboard runs through the tunnel.
- ⏳ Use case 3 on the VM: the `gold` job on `dev`, then `main`.
- ⏳ Use cases 1 and 4, and the Data health page ([use_cases.md](use_cases.md)).
- ⏳ A note in the report on how a correlation is not a cause: hydropower reserves, gas prices and exports move prices too.
