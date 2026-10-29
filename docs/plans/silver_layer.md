# Silver layer

**Status:** built on branch `feat/silver-layer` (28 Oct 2026) and tested in CI-like conditions; not yet run on the VM. Section 12 shows what is left.

**Data:** two sources only: electricity prices (hvakosterstrommen.no) and weather (MET Frost). The Gold use cases in [use_cases.md](use_cases.md) are built on the tables in section 4.

## 1. What Silver is for

Bronze keeps exactly what the APIs sent. Silver turns that into clean tables we can trust:
- one row per zone (or station) and hour, in UTC;
- typed columns with the unit in the name;
- no duplicates;
- a record of every problem found and what we did about it.

The course guide ([Data_Engineering_Project_Definition_Guide_v3.pdf](../Data_Engineering_Project_Definition_Guide_v3.pdf), section 4) calls Silver the **primary focus** and asks ten questions about it. Section 13 shows where this document answers each one.

Silver contains **no business rules**. VAT, strømstønad, Norgespris and peak hours belong to Gold ([use_cases.md](use_cases.md), section 9).

## 2. What Silver has to deal with

Checked on 28 Oct 2026 against the price API and our Bronze sample in `data/bronze`.

**Prices**
- One file per zone and **Norwegian** day, normally 24 hourly rows.
- Daylight saving days have 23 rows (2026-03-29) or 25 rows (2026-10-25).
- Each row has `NOK_per_kWh`, `EUR_per_kWh`, the exchange rate `EXR`, and `time_start` and `time_end` with their UTC offset.
- NOK is always EUR × EXR rounded to 5 decimals (0.06708 × 11.2765 = 0.75643). A damaged NOK price can be rebuilt exactly.

**Weather**
- One file per station and **UTC** day. Price files and weather files cover different days, so Silver joins on the UTC hour, never file by file.
- One record per reference time, each holding several observations. Our sample has more than one series for the same element and hour:
  - Blindern reports temperature at 2 m and at 10 m (−4.4 °C and −4.5 °C at 00:00 UTC on 15 Jan 2024);
  - files from before we requested only hourly data also contain 10-minute series;
  - Sola also reports 30-minute series, at 20 and 50 minutes past the hour.
- Stations sometimes miss hours.
- MET corrects data after quality control. Bronze then keeps a new version of that day.

**Chaos copy** (`bronze_chaos/`, about 1% of records damaged on purpose):
- duplicated records;
- null values;
- impossible values (999.0 NOK/kWh, 999.9 °C);
- unreadable timestamps (`not-a-timestamp`);
- truncated files, about 0.2% of all files.

Every injected error is listed in `bronze_chaos/_manifests/`, so we can check whether Silver found it.

**Size:** about 20,000 Bronze files for 2021-11 to today, which become about 220,000 price rows and 260,000 station-hours. That is small enough to rebuild Silver completely on every run.

## 3. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | The scheduled prod run cleans the **chaos copy**. A one-off run on clean Bronze, into a scratch folder, gives the comparison for the report. | The cleaning is tested and scored every day, and Gold's "if data is missing" rules get real use. The cost: a truncated file can't be repaired. With one version per day, that is about 4 zone-days of prices a year, plus a few station-days of weather. |
| D2 | **Parquet**, partitioned by `year` and `month` of the Norwegian date. | Same as in the labs. A month is a natural unit to rebuild. Delta Lake would add a dependency we don't need. |
| D3 | **Rebuild everything** on every run. A partial rebuild of single months (`--months`) comes later, and only if measurements show the full rebuild is too slow. | Simplest, and a rerun always gives the same result. |
| D4 | Spark DataFrame and SQL functions only, **no Python UDFs**. | Spark can optimise the whole query. The project definition promises this. |
| D5 | All times in **UTC**; Norwegian date and hour are extra columns. Never use the Norwegian hour as a key. | Daylight saving is handled once. On the autumn day, 02:00 Norwegian time happens twice. |
| D6 | When a day has several versions, the **newest readable, non-empty file** wins. If it is truncated, the previous version is used. Inside a file, the first copy of a duplicate wins. | One rule that is easy to explain, with no mixing of rows from different versions. |
| D7 | Silver checks every row of every file and **logs every problem** together with what it did: `rejected`, `repaired` or `removed`. Nothing disappears silently. | This is the dead-letter queue from the project definition, and it lets us score Silver against the chaos list. |
| D8 | **No business rules** in Silver. Calendar fields and heating degree hours are enrichment, so they belong here. | The guide's own example derives calendar fields in Silver. |
| D9 | Stations are mapped to zones with `config/zones_and_stations.json`, not with zone polygons. | We picked the stations for each zone by hand, so a lookup table is enough. |
| D10 | Weather values with Frost quality code **5, 6 or 7** are rejected; codes 0, 1, 2 and 4 are kept. | Frost itself calls 5 and 6 "very uncertain" and 7 "erroneous". The codes and our reasons are in section 7. |

## 4. Silver tables (what Gold reads)

Conventions for every table:
- **Location:** `<data root>/<env>/silver/<table>/`.
- **Partitions:** `year` and `month` of the Norwegian date, one file per month. The two reference tables and `dq_issues` (thousands of rows at most, even on the chaos copy) are single files.
- **Time:** `utc_hour` is the start of the hour in UTC. Anyone reading Silver must set `spark.sql.session.timeZone=UTC`, or the times display shifted.
- **Units** are part of the column names. Prices exclude VAT.
- **Lineage:** `prices` and `weather_hourly` keep `_source_file`, `_batch_id` and `_ingested_at_utc`, so every row can be traced back to its Bronze file.

| Table | One row per | Main columns |
|---|---|---|
| `prices` | zone and hour | `bidding_zone`, `utc_hour`, `local_date`, `local_hour`, `price_nok_kwh`, `price_eur_kwh`, `eur_nok_rate`, `is_repaired` |
| `weather_hourly` | station and hour | `station_id`, `bidding_zone`, `utc_hour`, `local_date`, `temp_celsius`, `wind_speed_mps`, `temp_quality_code`, `wind_quality_code`, `temp_is_imputed`, `wind_is_imputed` |
| **`zone_hourly`** | zone and hour | `bidding_zone`, `utc_hour`, `local_date`, `local_hour`, `weekday`, `is_weekend`, `is_holiday`, `price_nok_kwh`, `price_eur_kwh`, `temp_celsius`, `wind_speed_mps`, `hdh`, `station_count`, `has_price`, `has_weather`, `weather_is_imputed` |
| `dim_date` | Norwegian date | `local_date`, `year`, `month`, `day`, `weekday` (1 = Monday), `is_weekend`, `is_holiday`, `holiday_name`, `hours_in_day` (23, 24 or 25) |
| `dim_station` | station | `station_id`, `station_name`, `bidding_zone`, `zone_name`, `latitude`, `longitude`, `is_primary` |
| `dq_issues` | problem found | `run_id`, `source`, `issue`, `action`, `source_file`, `record_index`, `field`, `raw_value` (the original record, shortened), `detected_at_utc` |
| `dq_metrics` | run and metric | `run_id`, `source`, `metric`, `value`; appended on every run |

There is also one run manifest per run, `_runs/run__<run_id>.json`. It holds the status, the input, rows per table, the check results, and the newest price hour and weather hour per zone. Gold and the data health page read the newest manifest with status `SUCCESS` to know how fresh Silver is.

**`zone_hourly` is the table Gold reads.** It has a row for **every** zone and hour:
- from the first price hour (2021-11-01, if prod history goes back that far);
- to the last one (tomorrow, once the day-ahead run exists).

The row exists even when a source has no data. `has_price` and `has_weather` say what is there; `has_weather` means the zone has a temperature for that hour.

**Example** (real data):
- **Bronze:**
  - NO1 price file for 15 Jan 2024: `0.75643` NOK/kWh for `2024-01-15T01:00:00+01:00`;
  - Blindern weather file: six observations at `2024-01-15T00:00:00.000Z`, which are temperature at 2 m and 10 m, each hourly and every 10 minutes, and wind hourly and every 10 minutes.
- **Silver (`zone_hourly`):** `NO1, 2024-01-15 00:00 UTC (01:00 Norwegian time, Monday, not a holiday), 0.75643 NOK/kWh, −4.4 °C, 7.3 m/s, hdh 21.4, has_price, has_weather`

**What each use case reads:**

| Use case | Reads from Silver |
|---|---|
| 1. Cheapest hours | `price_nok_kwh` including tomorrow; `has_price` and `dim_date.hours_in_day` to tell whether a day is complete |
| 2. Dashboard | Price, temperature, wind and `hdh`; `weekday` for peak hours; `has_price` and `has_weather` for the hour counts |
| 3. Monthly bill | `hdh` and the price for every hour |
| 4. Price outlook | Earlier prices for the same hour, `weekday`, `is_holiday`, month, temperature, `hdh` and wind |
| Data health page | `dq_issues`, `dq_metrics` and the run manifests |

## 5. Reading Bronze

The same steps for both sources:
1. **Read with a fixed schema.** We give Spark the exact structure of our Bronze files instead of letting it guess. Folder names (`zone=NO1`, `year=2024`, ...) become columns.
2. **Catch truncated files.** In `PERMISSIVE` mode, Spark puts an unreadable file into a `_corrupt_record` column instead of failing. That file gets the issue `corrupt_file` with action `rejected`. The project definition names `badRecordsPath` for this, but that option only exists on Databricks.
3. **Keep lineage.** `_source_file` comes from `input_file_name()`. The file's date comes from its name, which also works for truncated files.
4. **Leave out test data.** Files marked `_is_synthetic` (mock weather) get the action `excluded` unless `--allow-synthetic` is given.
5. **Choose the version** (D6). Older versions are still checked, so their problems appear in the issue log, but their rows are not used.
6. **One row per record.** `posexplode(payload)` turns the list of records into rows and keeps each record's position (`record_index`). The chaos list uses the same position, so the two can be matched.

Notes for whoever builds it:
- **Cache the parsed files.** Spark refuses a query that uses only the `_corrupt_record` column, and caching also means Bronze is read only once.
- **Use `input_file_name()`, not `_metadata`.** Spark's built-in `_metadata` column is hidden by our envelope field of the same name.

## 6. Cleaning prices → `prices`

| Check | Rule | If it fails |
|---|---|---|
| Timestamp | `time_start` is read with its offset and converted to UTC (`try_to_timestamp`). | Use `time_end` minus one hour (`repaired`). If that fails too: `rejected`. |
| Hourly | `time_end` is readable, `time_end` − `time_start` is one hour, and the hour is whole. | `rejected` (`wrong_resolution`). A price whose duration can't be checked counts as a wrong one. This also protects us if the API ever switches to 15-minute prices. |
| Right day | The Norwegian date of the hour matches the file's date. | `rejected` (`wrong_day`) |
| Price | `NOK_per_kWh` is not null and lies between −15 and 20. | Rebuild it as EUR × EXR, rounded to 5 decimals (`repaired`). If EUR or EXR is unusable: `rejected`. |
| Duplicate | The same hour appears twice in one file. | The later copy is `removed`. |

`is_repaired` marks rows whose price or time was rebuilt. As an extra check, we count the rows where NOK and EUR × EXR differ by more than 0.001. We expect none.

## 7. Cleaning weather → `weather_hourly`

1. **One row per observation.** Each record is split into its observations. Silver keeps their position and writes the field as `observations[i].value`, the same way the chaos list does.
2. **Timestamp:**
   - If `referenceTime` can't be read, the whole record is `rejected`. There is no second timestamp to repair it from.
   - A time outside the file's UTC day is `rejected` (`wrong_day`).
3. **Duplicates.** If the same reference time appears twice in one file, the later copy is `removed`.
4. **Values.** Every observation is checked, including the series we don't use:
   - null → `null_value`;
   - temperature outside −50 to 45 °C, or wind outside 0 to 75 m/s → `out_of_range`;
   - Frost quality code 5, 6 or 7 → `bad_quality` (D10, table below).

   The value is rejected, and that hour stays empty until step 7.
5. **Pick one series** per element and hour:
   - hourly (`PT1H`) at the full hour, with no offset;
   - temperature at 2 m (standard screen height);
   - wind at 10 m (standard mast height);
   - if several still remain, the lowest `timeSeriesId`.
6. **Columns.** Temperature and wind become `temp_celsius` and `wind_speed_mps`. Frost's `qualityCode` is kept as a column (`temp_quality_code`, `wind_quality_code`).
7. **Fill short gaps.** The project definition fills gaps shorter than 3 hours:
   - a gap of 1 or 2 missing hours between two valid values gets the last value before the gap and is flagged `*_is_imputed`;
   - longer gaps stay empty;
   - nothing is filled after a station's last observation.

   This is built with window functions over an hourly grid per station. `last(..., ignorenulls=True)` finds the value before a gap, and `first(..., ignorenulls=True)` finds the value after it.

**Frost quality codes (D10).** Frost gives every value a `qualityCode`. These are the codes from [Frost's data clarifications](https://frost.met.no/dataclarifications.html), checked on 28 Oct 2026. There is no code 3.

| Code | Frost's description | Silver |
|---|---|---|
| 0 | OK, value is controlled and found O.K. | keep |
| 1 | OK, value is controlled and corrected, or value is missing and interpolated | keep |
| 2 | Slightly uncertain, value is not controlled | keep |
| 4 | Slightly uncertain, value is not corrected | keep |
| 5 | Very uncertain, value is not corrected | reject |
| 6 | Very uncertain, model data. Value is controlled and corrected, or value is missing and is interpolated - automatic | reject |
| 7 | Erroneous, value is not corrected | reject |

Why this split:
- **We follow Frost's own words:** keep what it calls "OK" or "slightly uncertain"; reject what it calls "very uncertain" or "erroneous".
- **Code 2 only means nobody has checked the value.** That is no sign of an error.
- **Code 6 is a model estimate, not a measurement.** We prefer a visible gap, or our own short gap fill, which is flagged, over an estimate that looks like a measurement.
- **It costs nothing today.** On 28 Oct 2026 we looked at all six stations for 27 Oct 2026 and 27 Oct 2025:
  - every value in the series Silver uses (2 m temperature, 10 m wind) had code 0;
  - only Blindern's 10 m temperature, which Silver doesn't use, had code 2.

  The rule matters only when MET flags a value. The scorecard counts how often that happens (`bad_quality`), so we can revisit the rule if it removes too much.

## 8. Building `zone_hourly`

1. **A full grid.** Every zone × every hour, from the first to the last price hour (a `sequence` of hours crossed with the 5 zones).
2. **Zone weather.**
   - Each station gets its zone from `dim_station`. This is a broadcast join: the small table is copied to every worker, so the big table isn't shuffled.
   - For each zone and hour, we take the mean temperature and wind of its stations. NO2 averages Kjevik and Sola.
   - `station_count` says how many stations had a temperature.
3. **Heating degree hours.** `hdh = max(0, 17 − temp_celsius)`, as in the project definition.
4. **Left joins.** Prices and zone weather are left-joined onto the grid. The project definition has an inner join, which would drop tomorrow's prices because they have no weather yet. With the grid, every price hour stays and every gap is visible.
5. **Norwegian time and calendar.** `local_date` and `local_hour` come from `from_utc_timestamp(utc_hour, 'Europe/Oslo')`. `weekday`, `is_weekend` and `is_holiday` come from `dim_date`.

**`dim_date`** is made in Python, from 2021-11-01 to the end of next year:
- **Fixed public holidays:** 1 January, 1 May, 17 May, 25 and 26 December.
- **Holidays that move with Easter:**
  - Maundy Thursday, Good Friday, Easter Sunday and Easter Monday;
  - Ascension Day (Easter + 39);
  - Whit Sunday and Whit Monday (Easter + 49 and + 50).

  Easter comes from `dateutil.easter`, which is already a dependency.
- **`hours_in_day`** comes from the Europe/Oslo time zone: 23 on the March changeover day, 25 on the October one, otherwise 24.

**`dim_station`** is read from `config/zones_and_stations.json`.

## 9. Proving the cleaning works

### 9.1 Checks before writing

Silver computes all tables first, then checks them, and writes only if the hard checks pass. If a hard check fails, nothing is written, the run exits with code 1, and Gold keeps using the previous Silver.

Writing is safe too:
- every table goes to `silver/_staging/` first;
- only when all of them are written are they swapped into place, with a few folder renames that take milliseconds;
- the run manifest is written last.

A crash while writing (a full disk, a killed job) therefore leaves the previous Silver complete.

| Hard checks (stop the run) | Soft checks (warning and metric) |
|---|---|
| No duplicate keys in any table | A finished day has fewer price hours than `hours_in_day` |
| No empty keys; every `utc_hour` is on the full hour | Weather is less than 99% complete for a zone |
| All values inside the limits after cleaning | More than 5% of a source's rows are rejected (the chaos rate is 1%) |
| `zone_hourly` has exactly zones × hours rows | NOK and EUR × EXR differ by more than 0.001 |
| `prices` and `weather_hourly` are not empty | |
| At most half of a source's records are rejected | |

The last two hard checks protect against a broken input, such as an empty `bronze_chaos/` or a renamed API field. Without them, empty tables would pass every other check and replace the good Silver.

### 9.2 Before-and-after scorecard

Every run appends these to `dq_metrics`:
- **In:** files read, truncated files, older versions skipped, records read.
- **Cleaning:** rows rejected, repaired and removed, per issue type; gap-filled hours.
- **Out:**
  - share of empty prices, temperatures and wind speeds, before and after;
  - duplicate keys, before and after (after must be 0);
  - rows written;
  - completeness per zone.

This is the "quantitative before-and-after scorecard" the project definition promises, and it feeds the data health page.

### 9.3 Scoring against the chaos list

The chaos injector writes down every error it makes. We match Silver's issue log against that list on file, record position and field.

| Injected error | Prices | Weather |
|---|---|---|
| Duplicate | `removed` | `removed` |
| Null value | `repaired` from EUR × EXR | `rejected`; the hour is gap-filled if the gap is short |
| Impossible value | `repaired` from EUR × EXR | `rejected`; the hour is gap-filled if the gap is short |
| Bad timestamp | `repaired` from `time_end` | Whole record `rejected` |
| Truncated file | `rejected`; the older version is used if there is one | Same |

- **Target: 100% found** for every error type. That is possible because Silver checks every file and every series (D7).
- **Health page:** the share found per type and the number of problems that are not on the chaos list go into `dq_metrics` (source `chaos`: `recall_<type>`, `unexplained_issues`) and the run manifest.
- **Clean baseline (once, for the report):** run Silver on clean Bronze into a scratch folder (`--input bronze --output-dir ...`). It should find almost nothing, and the repaired prices from the chaos run should equal the clean prices.

## 10. Performance: measure, then decide

The guide asks for tuning "supported by relevant evidence rather than optimization by assumption". We run each experiment on the full prod Bronze and keep the timings and `explain()` output for the report.

| Experiment | What we expect to see |
|---|---|
| Fixed schema vs. letting Spark guess | Guessing reads all files an extra time |
| Many small files | Opening files costs more than reading them; we compare the number of files and tasks with the read time |
| `spark.sql.shuffle.partitions` 200 vs. 8 | 200 tiny tasks are overhead for data this small |
| `broadcast()` for the station and date tables | `BroadcastHashJoin` in the plan, and no shuffle of the big table |
| Partitioned by month vs. not | Reading one month skips the other folders (`PartitionFilters` in the plan) |
| `cache()` of the parsed Bronze | Bronze is read once instead of twice |
| Time of a full rebuild | Decides whether we need `--months` (D3). The day-ahead run must finish Silver and Gold by 14:30. |

## 11. Code, tests and workflow

**Code**, in `src/silver/`:

| File | What it does |
|---|---|
| `run_silver.py` | Command line: `--input`, `--output-dir`, `--allow-synthetic` (later `--months`). Exit codes 0, 1 and 2, as in `ingest_daily.py`. |
| `spark_session.py` | One place for the Spark settings: local mode, UTC, adaptive execution, dynamic partition overwrite, 8 shuffle partitions |
| `bronze_reader.py` | Schemas, reading, truncated and synthetic files, lineage, version choice |
| `prices.py`, `weather.py`, `zone_hourly.py` | One file per table |
| `reference.py` | `dim_date` and `dim_station` |
| `quality.py` | Issue log, checks, scorecard, chaos score, run manifest |

**What we reuse from Bronze:**
- `BronzeWriter`, for test data and atomic manifest writes;
- the zones file `config/zones_and_stations.json` (read directly, because `load_stations_config` returns only station IDs);
- the chaos injector;
- `generate_batch_id`;
- the paths in `config/settings.py`, plus one new setting: `SILVER_INPUT`, default `bronze_chaos`.

**Runtime:**
- **Pin `pyspark==3.5.0`** in `requirements.txt`, the version on the VM. Without the pin, CI would install Spark 4, which needs a newer Java than the VM has.
- **CI uses Python 3.11 and Java 8**, like the VM. Today it uses Python 3.12.
- **Spark tests are marked `spark`.** CI runs all tests. The VM runs `pytest -m "not spark"` before ingesting, so ingestion isn't slowed down.
- **Windows:** Spark can't write files on Windows without extra Hadoop tools, so run the Spark tests in CI, on the VM or in WSL.

**Tests** (`tests/test_silver_*.py`): each test builds a tiny Bronze folder with the real `BronzeWriter`. The ingestion time is fixed, so the chaos copy is the same on every run; the same fix for the existing chaos tests is waiting on `feat/elhub-bronze`.

| Test | Expected |
|---|---|
| Daylight saving | 25 hours on 2026-10-25, 23 on 2026-03-29 |
| Versions | Truncated newest version → the older one is used. Truncated only version → logged, and the run continues. Empty newest version → the older one is used. |
| Price repairs | Repaired prices equal the clean ones |
| Weather series | Only the 2 m hourly temperature and the 10 m hourly wind reach Silver |
| Gap filling | A 2-hour gap is filled and flagged; a 3-hour gap stays empty; nothing is filled after the last observation |
| `zone_hourly` | Every zone and hour is present; tomorrow has a price but no weather; NO2 is the mean of its 2 stations; `hdh` is correct |
| `dim_date` | 2026 holidays are right (Good Friday 3 April, Ascension Day 14 May, Whit Monday 25 May); days with 23 and 25 hours |
| Checks | A broken table → exit code 1, and nothing is written |
| Chaos | A seeded chaos copy → 100% of the injected errors are found |

**Workflow:** a `silver` job in `pipeline.yml` that runs after `bronze_chaos`, with the same branch guard (main → prod, dev → dev). Its step summary shows the rows written, issues per type, the check results and the chaos recall.

## 12. Build order

One commit per step on `feat/silver-layer`. ✅ = done and covered by tests; ⏳ = still to do.

**Must**

0. ⏳ **Backfill prod history.** Checked 28 Oct 2026: prod Bronze has no history yet, because the backfill hasn't been run. Run **Actions → Historical Backfill** on `main`:
   - start at 2021-11-01, the first day with prices for all five zones (NO4 has none in October 2021);
   - first 2021-11-01 → 2021-12-31 as a short test, then one calendar year per run, then 2026 up to today minus 4 days;
   - about an hour per year; start runs during the day, so the VM's runner is free for the 02:00 UTC daily run.

   The next daily run then copies the history into `bronze_chaos/` by itself. Silver's code and tests don't wait for this; only the first full prod run does.
1. ✅ **Spark runtime:** pin pyspark, run CI on Python 3.11 and Java 8, add the `spark` test marker and fixture and `spark_session.py`. Bring over the chaos-test fix.
2. ✅ Reading Bronze, the issue log, `dim_station` and `dim_date`.
3. ✅ `prices`, with the daylight saving tests.
4. ✅ `weather_hourly`: series choice and gap filling.
5. ✅ `zone_hourly`.
6. ✅ Checks, scorecard, chaos score and run manifest.
7. **Workflow job.**
   - ✅ The `silver` job runs after `bronze_chaos` in `pipeline.yml`; the VM runs the tests without Spark.
   - ⏳ Merge into `dev` and do a first full run on the VM (dev environment).
   - ⏳ After the backfill, a run on prod, then the one-off run on clean Bronze.

**Should**

8. ⏳ Performance experiments (section 10), notes for the report, and the project definition updates (section 14). The README is already updated.
9. ⏳ `--months`, only if step 8 shows that the full rebuild is too slow for the 14:30 deadline.

## 13. The guide's Silver questions

| Guide question | Where we answer it |
|---|---|
| Data understanding | Sections 2, 6 and 7 |
| Data model (grain, keys, history, reference data) | Section 4, D6, D9 |
| Cleaning | Sections 6 and 7, D7 |
| Standardization | D5; section 4 (units and snake_case names) |
| Enrichment | Section 8 |
| Quality evidence | Section 9 |
| Implementation | D4; section 11 |
| Performance | Section 10 |
| Lifecycle | Sections 11 and 12 |
| Scalability | D2 and D3: month partitions allow partial rebuilds. Fixed schemas ignore new fields. A removed field turns into empty values, the records fail, and a run that loses most of a source's records stops before writing (section 9.1). |

## 14. Updates to the project definition (in step 8)

| The project definition says | We do instead |
|---|---|
| Delta Lake | Parquet (D2) |
| Zone polygons to map stations to zones | A lookup in `config/zones_and_stations.json` (D9) |
| `to_utc_timestamp()` for time zones | Read the UTC offset that is already in each timestamp (section 6) |
| `badRecordsPath` | `PERMISSIVE` mode with `_corrupt_record` (section 5) |
| Great Expectations | Our own checks in `quality.py` (section 9) |
| `pytest-spark` | A plain pytest fixture |
| Inner join of prices and weather | A grid of all zones and hours, with left joins (section 8) |
| Open-Meteo and precipitation | Dropped: two sources only |
| `silver/_rejected_records/` | `silver/dq_issues`; its rejected rows are the dead-letter queue |
| Partitions by zone and year or month | Year and month only |

## 15. Done when

- ⏳ A full prod run passes all hard checks, with 0 duplicate keys in every table.
- ✅ Daylight saving days have 23 or 25 hours in `prices` and `zone_hourly`.
- ✅ 100% of the injected chaos errors are found, and the repaired prices equal the clean ones. ⏳ Still to confirm on prod data.
- ✅ Every run writes a run manifest and scorecard rows, and the step summary shows them.
- ⏳ The time of a full rebuild is measured and written down.

**Tested so far** (28 Oct 2026, in a Linux container with Python 3.11, Java 17 and Spark 3.5.0):
- All 92 tests pass, 34 of them Spark tests. This includes the fixes from Copilot's review of PR #7: no empty or mostly rejected publishes, staged writes, and an unreadable `time_end`.
- A smoke run of the real command lines on a week of generated Bronze around the October DST change:
  - 71 files, 65 injected errors: **all 65 found**, and no issue that isn't on the chaos list;
  - `zone_hourly` has exactly 5 zones × 169 hours;
  - one truncated price file shows up as a soft warning (one incomplete zone-day), as intended.

## 16. Answered questions

1. **Should Silver reject weather values by Frost quality code?** Yes: codes 5, 6 and 7 (D10, section 7).
2. **How far back does prod Bronze go?** Not back to 2021 yet: the backfill hasn't been run (step 0). This also answers question 3 in [use_cases.md](use_cases.md).
