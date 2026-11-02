# Gold use cases

**Status:** use cases 2 (dashboard) and 3 (report) are built: Gold tables, checks, status file and a Streamlit page each ([gold_layer.md](gold_layer.md)). Use cases 1 and 4 are still proposals, with placeholder pages in the dashboard.

**Data:** two sources only: electricity prices (hvakosterstrommen.no) and weather (MET Frost). Elhub is out of scope; that work is archived on the branch `feat/elhub-bronze`.

## 1. What we were asked

The professor's feedback on our Gold layer:

1. Gold should have **different types of use cases**, not just more aggregates of the Silver data.
2. An ML model is welcome, but **it only has to work**. Its accuracy is not what counts.
3. Data must be **ready before users need it**. His example, roughly: *if the board updates once a day, it must not update later than I need it.*

The course guide ([Data_Engineering_Project_Definition_Guide_v3.pdf](../Data_Engineering_Project_Definition_Guide_v3.pdf), section 5) defines Gold as "a curated dataset optimized for a defined **dashboard, application, report, or analytical model**". We use these four consumers as our four types, with one use case each.

The guide also asks six questions about Gold. Each use case in section 5 answers them under the same headings:

| Guide question | Heading in section 5 |
|---|---|
| Which analytical question or consumer need will Gold support? | Question |
| Why should the Gold structure differ from Silver? | Why Gold |
| Which dimensions, measures, aggregations, or features are required? | Gold output |
| How often should Gold refresh, and how will consumers know it is ready? | Freshness |
| What should happen when upstream data is incomplete or fails checks? | If data is missing |
| How will correctness and usefulness be demonstrated? | Proof |

## 2. Our data, briefly

- **Prices:**
  - Hourly day-ahead spot prices for zones NO1–NO5, in NOK and EUR per kWh, excluding VAT.
  - Tomorrow's prices are published around 13:00.
  - History is complete from 2021-11-01.
- **Weather:**
  - Hourly air temperature and wind speed from 6 stations, one or two per zone.
  - A day's weather is complete after midnight.
- **Silver (planned in the project definition):** one clean row per zone and UTC hour, with `price_nok_kwh`, `temp_celsius`, `wind_speed_mps` and heating degree hours (`hdh` = max(0, 17 °C − temperature)).

Every Gold use case starts from that Silver data.

## 3. The four use cases at a glance

**What and for whom:**

| # | Type (guide) | Use case | Question it answers | User | Kind of analysis |
|---|---|---|---|---|---|
| 1 | Application | Cheapest hours tomorrow | When should I charge the car or run the dishwasher tomorrow, and what will it cost? | Household, smart charger | Prescriptive: what to do |
| 2 | Dashboard | Prices and weather | How do prices differ between zones, and how much do cold and wind move them? | Analyst, us in the report | Descriptive and diagnostic: what happened, and why |
| 3 | Report | Monthly household bill | What did a typical household pay last month, and was Norgespris cheaper than strømstønad? | Household, journalist | What-if: compare payment schemes |
| 4 | Analytical model | Price outlook | Roughly what will tomorrow's prices be, before they are published? | Anyone who plans before 13:00 | Predictive |

**When it is needed:**

| # | Needed by | Ready by | Refresh |
|---|---|---|---|
| 1 | Late afternoon, when people come home and plug in the car | 14:30 | Daily, after prices are published |
| 2 | Start of the working day | 06:00 | Daily |
| 3 | First day of the next month | 06:00 on the 1st | Monthly (month-to-date daily) |
| 4 | Before prices are published (around 13:00) | 06:00 | Daily; scored at 14:30 |

All four share one more page, **Data health** (section 7).

**Priority:**
- Use cases 1, 2 and 4 are must-haves. Together they cover the professor's three points.
- Use case 3 comes next. It reuses use case 1's prices and Silver's heating degree hours, so it is cheap to add. If time runs short, it is the one to drop.

## 4. How Gold differs from Silver

Silver answers "what does the data say, once cleaned?". Gold answers "what does this user need?".

| | Silver | Gold |
|---|---|---|
| Grain | One row per zone and hour | Chosen per use case: blocks of hours, days, months, forecasts |
| Business rules | None (spot price excluding VAT) | VAT, strømstønad, Norgespris, peak hours |
| Shape | General and reusable | Shaped for one page or one program, and small |
| Time | UTC | Norwegian local time, as users read it |
| History | Corrected when a source corrects its data | Some tables only append: a forecast or a health check records what was true then |

Example:
- **Silver row:** `NO1, 2026-10-24 01:00 UTC, 0.57 NOK/kWh, 4.1 °C, 3.2 m/s`
- **Gold row (use case 1):** `NO1, 2026-10-24, cheapest 3 hours 03:00–06:00, 0.71 NOK/kWh including VAT`

## 5. Use cases

### 5.1 Application: cheapest hours tomorrow

**Question.** For each zone, the use case shows:
- what the household pays for each hour tomorrow;
- the cheapest blocks of, for example, 1, 3 and 6 hours (dishwasher, washing machine, car charging);
- the most expensive hours to avoid.

A smart charger or a home-automation system could read the same data as a small JSON file. That is the "application" consumer.

**Why Gold.**
- Silver has spot prices only. Gold adds VAT and the support schemes (section 9) and ranks the hours of each day.
- Strømstønad doesn't change the order of the hours; it only flattens prices above the threshold.
- With Norgespris every hour costs the same, so the page says that moving usage saves nothing on the energy price.

**Gold output.**
- `consumer_prices_hourly`, one row per zone and hour from 2024-01-01. It holds:
  - the spot price;
  - VAT;
  - strømstønad and the price after strømstønad;
  - the Norgespris price.
- `cheapest_hours`, one row per zone, day and block length. It holds the start and end time, the average price, and the saving compared with starting at 17:00 (for example, for a 20 kWh car charge).
- `cheapest_hours_tomorrow.json`: tomorrow only, for an app.

**Freshness.**
- Prices are published around 13:00. A day-ahead run at about 13:15 (retrying hourly) builds this by **14:30**, well before people plug in their cars in the evening.
- **Our pipeline can't do this yet.** The 02:00 UTC run only collects finished days, so tomorrow's prices arrive about a day and a half after they are published, when that day is already over. This is the professor's example almost exactly. The fix is in section 8.

**If data is missing.**
- A zone is published only when it has every hour of the day (23–25, depending on daylight saving).
- Until then, the page says "tomorrow's prices usually arrive around 13:00, last checked at 13:15" and keeps showing today's prices.

**Proof.**
- **Tests:**
  - The cheapest-block search is compared with a brute-force search.
  - The price formulas are checked against the worked example in section 9.
- **Usefulness:** the saving for a 20 kWh car charge.

### 5.2 Dashboard: prices and weather

**Question.** How do prices develop in each zone, how far apart are the north and the south, and how much do cold and wind move the price? Examples include:
- the 2022 price crisis in the south;
- winter against summer;
- weekdays against weekends.

**Why Gold.** Silver is hourly. The dashboard needs daily and monthly figures per zone, with prices and weather side by side, already summarised. Building them once in Gold keeps the dashboard fast and the numbers consistent. This use case continues the Gold assets in our project definition: heating degree days, volatility, price sensitivity.

**Gold output.**
- `zone_daily`: one row per zone and Norwegian local day, with:
  - average, minimum and maximum price;
  - volatility (standard deviation);
  - ratio of peak to off-peak prices (peak = 08–20 on weekdays);
  - number of hours with a negative price;
  - mean temperature, heating degree days and mean wind;
  - how many hours had price and weather data.
- `price_weather_sensitivity`: one row per zone and month. It holds the correlation and slope between daily temperature and daily price (for example, "+4 øre per degree colder"), and the same for wind.

**Freshness.**
- The 02:00 UTC run brings in yesterday's weather, and the dashboard is ready by **06:00**, before the working day.
- Nobody needs it sooner: a day's weather is only complete once the day is over.

**If data is missing.**
- Days with missing hours stay in the table, with their hour counts showing the gap.
- They are left out of the correlations.

**Proof.**
- Checked against Silver:
  - each day has 23, 24 or 25 hours;
  - daily averages recomputed from Silver match.
- Spark's `corr` is checked against pandas on a sample.

### 5.3 Report: monthly household bill

**Question.** What did a typical household in each zone pay for electricity last month, under each of these:
- strømstønad;
- Norgespris (from October 2025);
- no support at all?

Which scheme was cheaper, and how much support was paid?

**Why Gold.** It combines the price rules (section 9), an assumption about the household and monthly sums. None of that belongs in Silver. Unlike the dashboard, which keeps moving, a report gives one fixed, final number for each closed month.

**Household assumption** (to state in the report):
- We have no consumption data, so we model it.
- Every hour uses a base load plus heating that grows as it gets colder, for example `kWh = 0.6 + 0.1 × hdh`.
- That gives roughly 15,000 kWh a year in Oslo (to be checked on our data), close to the commonly quoted 16,000 kWh for a Norwegian household.
- It also makes cold, expensive hours count more, as they do in real bills.
- Both numbers are settings we can change.

**Gold output.** `household_bill_monthly`, one row per zone, month and scheme, holding:
- kWh used;
- cost in NOK;
- average price per kWh;
- support received;
- `is_final`. The current month appears as a month-to-date row with `is_final = false`.

The costs cover the energy price including VAT only. The grid tariff and the supplier's fees are the same under every scheme, so they don't change which scheme wins.

**Freshness.**
- The month-to-date row updates every day by 06:00.
- A month's final report is ready on the 1st of the next month by **06:00**.

**If data is missing.** A month with missing days stays `is_final = false` and is shown as preliminary.

**As built** ([gold_layer.md](gold_layer.md), section 3):
- The bill starts in January 2024, the first year the price rules in section 9 cover.
- The household is the one above, `kWh = 0.6 + 0.1 × hdh`. In NO1 that comes to about 13,800 kWh a year: 0.6 × 8,760 hours + 0.1 × 24 × the 3,555 heating degree days from 2 Nov 2025 to 1 Nov 2026. The page shows the exact figure for each zone.
- An hour without a temperature uses the zone's average `hdh` for that month. Otherwise one missing weather hour would keep a month preliminary for good.
- A month is final when every hour of it has a price.

**Proof.**
- Monthly sums equal the sum of the hourly rows.
- Norgespris cost equals kWh × 0.50 NOK (0.40 in NO4).
- Support never exceeds 90 % of the price above the threshold.

### 5.4 Analytical model: price outlook for tomorrow

**Question.** Roughly what will tomorrow's hourly prices be, before they are published around 13:00?
- In the real market, bids for the next day close at 12:00. A morning outlook is what traders and flexible consumers plan with.
- On our board, the outlook fills the "Tomorrow" page in the morning. The real prices replace it in the afternoon.

**Why Gold.** A model needs a feature table: one row per zone and hour, with the inputs and the right answer. That is a different shape from Silver. Gold also keeps a record of every forecast made.

**Gold output.**
- `price_features`: one row per zone and hour, with:
  - the price in the same hour yesterday and a week ago;
  - hour, weekday, month and a holiday flag;
  - temperature, `hdh` and wind on the last day with complete weather.
- `price_outlook`, append-only: when the forecast was made, zone, hour, predicted price, and a naive forecast ("same as today").
- `outlook_accuracy`: one row per zone and day, with the average error of the model and of the naive forecast.
- **The model:** Spark MLlib linear regression, retrained every morning (the data is small). We don't tune it.

**Freshness.**
- The outlook must exist **before 13:00**. A forecast that arrives after the real prices helps nobody, which is the professor's point in its purest form.
- The 02:00 UTC run builds it by 06:00.
- At 14:30 the real prices arrive, and the outlook is scored the same day.

**What "it works" means.**
- Every morning there is an outlook for all 5 zones and every hour, stored and later scored against the real prices and the naive forecast.
- We report the errors honestly and don't tune for accuracy.

**If data is missing.**
- A zone with missing features gets the naive forecast and a flag.
- If training fails, yesterday's model is used.

**Proof.**
- The accuracy table itself.
- A test that the features never use data from after the forecast time.

## 6. A day on the board

| Time (Norwegian) | Run | What updates | What users see |
|---|---|---|---|
| 03:00–04:00 (02:00 UTC) | Daily | Yesterday's weather and prices, then Silver, then Gold | By 06:00: the dashboard, the month-to-date bill, and the outlook for tomorrow |
| About 13:15, retrying hourly | Day-ahead | Tomorrow's prices, then Silver, then Gold | By 14:30: tomorrow's prices and cheapest hours; the morning outlook is scored |
| The 1st of the month | Daily | The month's last day | By 06:00: the final monthly bill report |

GitHub's scheduler sometimes starts runs late. The hourly retries in the afternoon cover that.

## 7. Data health and readiness (shared by all use cases)

The guide asks how consumers know Gold is ready, and what happens when upstream data fails. One answer covers all four use cases:

- **Publish only after checks pass.** Each Gold table is written to a temporary folder and replaces the live one only if its checks pass. Otherwise the previous version stays.
- **Status file.** Each run writes `gold/_status/latest.json`. For each table, it records the newest data the table contains, when it was built, and whether the checks passed. Every page shows "updated at …", and a warning when its data is late.
- **Data health page** (`pipeline_health`, append-only). It shows:
  - the last run per source;
  - deadlines met or missed;
  - rows Silver rejected;
  - how many of the injected chaos errors Silver caught.

  This also answers the guide's section 6: "Are run status, failures, rejected records, and quality results visible?"

## 8. What we need before building Gold

1. **A day-ahead price run** (use cases 1 and 4). It is already built on `feat/elhub-bronze`; we port only that part, without Elhub.
2. **The Silver layer for prices and weather** (project definition, sections 4 and 7), with one change:
   - The definition joins prices and weather with an *inner* join. Tomorrow's prices have no weather yet, so they would be dropped.
   - Silver must keep every price hour (a left join from prices) and leave weather empty where it isn't known yet.
3. **A price rules table** with the rules in section 9.
4. **A Gold step in the workflow**, after Silver, in both runs.
5. **History from 2021-11-01 in prod**, if it isn't there yet. Use cases 2 and 4 are better with the 2022 price crisis included.

## 9. Price rules

Confirmed on regjeringen.no on 22 Oct 2026: [Regjeringens strømtiltak](https://www.regjeringen.no/no/tema/energi/strom/regjeringens-stromtiltak/id2900232/) and [Spørsmål og svar om Norgespris](https://www.regjeringen.no/no/tema/energi/strom/sporsmal-og-svar-om-norgespris/id3089310/).

| Rule | Value |
|---|---|
| VAT | 25 %. 0 % in NO4: households in Nordland, Troms and Finnmark pay no VAT on electricity ([merverdiavgiftsloven § 6-6](https://www.skatteetaten.no/nn/rettskjelder/type/handboker/merverdiavgiftshandboken/gjeldende/M-6/M-6-6/)), and NO4 approximates that area. |
| Strømstønad | 90 % of each hour's spot price above a yearly threshold, excluding VAT: 73 øre (2024), 75 øre (2025), 77 øre (2026). VAT is added to the support ([the law](https://stortinget.no/no/Saker-og-publikasjoner/Vedtak/Beslutninger/Lovvedtak/2024-2025/vedtak-202425-135/), § 6 and § 8). Covers up to 5,000 kWh a month. |
| Norgespris | 40 øre/kWh excluding VAT from 1 Oct 2025 to 31 Dec 2026. Households have it instead of strømstønad, never both. Covers up to 5,000 kWh a month for homes. |

Formulas (NOK/kWh, spot price excluding VAT):

- price including VAT = spot × (1 + VAT)
- strømstønad = 0.9 × max(0, spot − threshold) × (1 + VAT)
- price with strømstønad = price including VAT − strømstønad
- Norgespris price = 0.40 × (1 + VAT)

**Example** (NO1, 2026, spot 1.00):
- Price including VAT: 1.25.
- Support: 0.9 × 0.23 × 1.25 = 0.26.
- With strømstønad the household pays 0.99.
- With Norgespris it pays 0.50.

A typical household stays well below 5,000 kWh a month, so we ignore the caps and say so.

## 10. Ideas we are not doing now

- **Alerts for negative prices and price spikes.** Could become part of use case 1.
- **Analysis of when the north and the south split into different prices.** Could become part of use case 2.
- **Sorting days into "day types" with clustering.** This would be a second ML model.
- **The effect of the EUR/NOK exchange rate on prices.** The price data already includes the rate.
- **Precipitation as a sign of how much hydropower is available.** The same Frost API offers it, but we would have to change Bronze.
- **Consumption and production data from Elhub.** Archived on `feat/elhub-bronze`.

## 11. Updates to the project definition (once we agree)

- **Section 5 (Gold)** lists two assets, `gold_daily_grid_economics` and `gold_price_sensitivity_features`.
  - They live on inside use case 2 (`zone_daily`, `price_weather_sensitivity`) and use case 4 (`price_features`).
  - Section 5 should be rewritten around the four use cases.
- **Section 1** should name the four consumers under "Who or what will use the result?".
- **Section 4** needs the left join from section 8.

## 12. Questions for the team

1. Do we agree on the four use cases, and on use case 3 being the one to drop if time runs short?
2. Is the household assumption in 5.3 good enough, or do we prefer a flat profile (the same kWh every hour)?
3. How far back does prod Bronze go today? Should we backfill from 2021-11-01?
