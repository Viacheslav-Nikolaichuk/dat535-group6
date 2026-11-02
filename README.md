# DAT535 Data Engineering: Regional Power Grid Economics and Microclimate Dynamics

An automated, end-to-end Data Lakehouse architecture using **PySpark**, **Delta Lake/Parquet**, and **GitHub Actions** running on a self-hosted VM runner to ingest, clean, harmonize, and model Norwegian regional electricity spot prices alongside localized meteorological observations across bidding zones `NO1`–`NO5`.

---

## Repository Structure

```text
.
├── .github/
│   └── workflows/
│       ├── ci.yml                         # All unit tests incl. Spark on GitHub-hosted runners (push to dev, PRs to dev/main)
│       ├── pipeline.yml                   # Bronze -> chaos copy -> Silver -> Gold on the VM runner (cron 02:17 UTC on main = prod; manual on dev = dev)
│       └── backfill.yml                   # Manual historical Bronze backfill on the VM runner (main = prod, dev = dev)
├── config/
│   ├── settings.py                        # dev/prod environment, data paths, env loader, and API base settings
│   ├── price_rules.py                     # VAT, strømstønad, Norgespris and the modelled household (use case 3)
│   └── zones_and_stations.json            # Mapping of bidding zones NO1-NO5 to MET Norway weather stations
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── strompris_client.py            # Hva Koster Strømmen API extractor
│   │   ├── frost_client.py                # MET Norway Frost API extractor (auth & error handling)
│   │   ├── bronze_writer.py               # Metadata audit envelopes & versioned, checksum-deduplicated JSON storage
│   │   ├── ingest_daily.py                # Ingestion entrypoint CLI (daily, lookback catch-up, backfill)
│   │   └── backfill_historical.py         # Historical backfill CLI: month-by-month, resumable, outside the daily window
│   │   └── chaos_injector.py              # Builds bronze_chaos/: a Bronze copy with ~1% injected errors
│   ├── silver/
│   │   ├── run_silver.py                  # Silver entrypoint CLI: rebuilds every Silver table, checks before writing
│   │   ├── spark_session.py               # SparkSession factory (UTC, dynamic partition overwrite, AQE)
│   │   ├── bronze_reader.py               # Explicit Bronze schemas, truncated files, lineage, version choice
│   │   ├── prices.py                      # Bronze prices -> silver/prices (repairs from EUR x EXR, DST-safe UTC)
│   │   ├── weather.py                     # Bronze weather -> silver/weather_hourly (series choice, gap filling)
│   │   ├── zone_hourly.py                 # silver/zone_hourly: every zone x hour, prices + zone weather + HDH
│   │   ├── reference.py                   # dim_station (config) and dim_date (holidays, hours per local day)
│   │   └── quality.py                     # Issue log (DLQ), checks, scorecard, chaos score, run manifest
│   ├── gold/
│   │   ├── run_gold.py                    # Gold entrypoint CLI: rebuilds the Gold tables, checks, status file
│   │   ├── dashboard.py                   # Use case 2 tables: zone_daily, price_weather_sensitivity, price_profile
│   │   └── household_bill.py              # Use case 3 table: household_bill_monthly
│   └── utils/
│       ├── hashing.py                     # Batch UUID4, ISO-8601 UTC, and SHA-256 payload checksums
│       └── http.py                        # Shared requests.Session with retry/backoff & User-Agent
├── dashboard/                             # Streamlit dashboard over Gold (see Dashboard below)
│   ├── app.py                             # Entry point: menu from registry.py, sidebar data status
│   ├── registry.py                        # The use cases: question, user, deadline, status, page module
│   ├── data.py / ui.py / charts.py        # Reading Gold (pandas), cached loading, Altair charts
│   └── views/                             # One page per use case (prices_weather.py, household_bill.py) + overview and placeholder
├── deploy/
│   └── dat535-dashboard.service           # systemd unit that keeps the dashboard running on the VM
├── scripts/
│   ├── test_api_connections.py            # Diagnostic smoke-test for API connectivity & credentials
│   └── make_sample_gold.py                # Synthetic Gold tables for trying the dashboard without the VM
├── tests/
│   ├── test_ingestion.py                  # PyTest suite for hashing, envelopes, versioning, CLI dates, and clients
│   └── test_backfill.py                   # PyTest suite for the historical backfill (monthly runs, resume, date guard)
│   └── test_chaos.py                      # PyTest suite for chaos injection (rates, determinism, Bronze untouched)
│   └── test_settings.py                   # PyTest suite for dev/prod environment and data path resolution
│   └── conftest.py                        # Shared fixtures: local SparkSession, Bronze builders shaped like the APIs
│   └── test_silver_*.py                   # Spark tests (marker `spark`): reader, prices, weather, zone_hourly, end-to-end runs
│   └── test_gold_*.py                     # Spark tests: use case 2 and 3 tables, Bronze -> Silver -> Gold end-to-end
│   └── test_dashboard_data.py             # Dashboard helpers and the sample generator (pandas, no Spark)
├── labs/                                  # Archived course laboratory materials (Lab 1, 2, 3)
│   ├── lab1_1_setup_guide.md
│   ├── lab1_install-spark.sh
│   ├── lab2_pipeline.py
│   ├── lab2_spark_fundamentals.ipynb
│   ├── lab3_advanced_spark.ipynb
│   ├── lab3_pipeline.py
│   ├── run_pipeline.py
│   ├── project_cicd_guide.md
│   └── workflows/                         # Disabled lab workflows (dat535-dev, dat535-prod, pipeline)
├── docs/                                  # Project guides & documentation
│   ├── Data_Engineering_Project_Definition_Guide_v3.pdf
│   └── plans/
│       ├── silver_layer.md                # Silver layer design (tables Gold reads, cleaning rules, quality evidence)
│       ├── gold_layer.md                  # Gold layer and dashboard design (use case 2 tables, checks, freshness)
│       └── use_cases.md                   # Proposed Gold use cases (dashboard, application, report, ML model)
├── dat535_data_engineering_project_definition.md # Official project specification & architecture
├── pyproject.toml                         # Pytest configuration (import path, test discovery)
├── requirements.txt                       # Project Python dependencies
├── requirements-dashboard.txt             # Dashboard dependencies (Streamlit, pandas, pyarrow)
└── .env.example                           # Template for local environment variables & API keys
```

---

## Data Sources & Architecture

### 1. Electricity Spot Prices (Strømpris API)
* **Provider**: `hvakosterstrommen.no`
* **Format**: Hourly spot prices per bidding zone (`NO1`, `NO2`, `NO3`, `NO4`, `NO5`).
* **Authentication**: Free, open REST API (no key required).
* **Schedule**: Day-ahead auction prices for tomorrow are published daily around **13:00 CET**.

### 2. Meteorological Observations (MET Norway Frost API)
* **Provider**: `frost.met.no`
* **Format**: Hourly surface observations (`air_temperature`, `wind_speed`) from official meteorological stations. Requests use `timeresolutions=PT1H`; without it some stations also return 1-minute and 10-minute series (~2 MB/day instead of ~25 KB).
* **Authentication**: Free instant registration requiring a `Client ID` from [Frost Credentials](https://frost.met.no/auth/requestCredentials.html).
* **Nature of Data**: Empirical physical sensor observations (available up to the current hour; not predictive forecasts).

### 3. Medallion Storage Tiers
* **Bronze Layer**: Immutable, append-only raw JSON payloads wrapped in an audit envelope containing `_batch_id`, `_ingestion_timestamp_utc`, `_source_uri`, `_payload_checksum` (SHA-256), `_is_synthetic`, and `_schema_version`.
  * Electricity: `/bronze/electricity/zone={zone}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}__{ingestTs}_{batch}.json`
  * Weather: `/bronze/weather/station={station_id}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}__{ingestTs}_{batch}.json`
  * **Versioning**: files are never overwritten. On re-ingestion the new payload checksum is compared with the latest version for that date: identical → skipped (`unchanged`); different → written as a new version (`new_version`, e.g. MET Norway QC revisions). Silver keeps the latest version per key via `_ingestion_timestamp_utc`. Legacy `{YYYY}_{MM}_{DD}.json` files are treated as the first version.
  * **Day boundaries**: a price file for date `D` covers the Norwegian local day (23 or 25 hours on DST days); a weather file covers the UTC day. Silver must join on the UTC hour across files, not per file.
  * **Atomic writes**: files are written to a hidden `.{name}.tmp` file and then renamed, so a crashed run never leaves a partial file that Spark would read (Spark skips names starting with `.` or `_`).
  * **Run manifests**: every non-dry run writes `/bronze/_manifests/run__{batch_id}.json` with record counts, errors, and `written_files` (paths relative to `/bronze`, with `created`/`new_version` status). Silver can use it to process only new files instead of rescanning Bronze.
* **Bronze Chaos Copy** (`/bronze_chaos/`): the APIs return very clean data, so for Silver cleaning/DLQ exercises `chaos_injector.py` mirrors Bronze into a separate tree with ~1% of all records damaged. `/bronze/` is only read, never modified.
  * **Error types**: `duplicate` (record emitted twice), `null_field` (price / observation value → `null`), `out_of_range` (`999.0` NOK/kWh, `999.9` °C), `bad_timestamp` (`"not-a-timestamp"`), and `corrupt_file` (file truncated mid-JSON). File corruption takes 20% of the budget, record errors 80%, so the expected share of damaged records stays ~1% overall.
  * **Tamper evidence**: `_metadata` is copied unchanged, so a modified payload no longer matches its `_payload_checksum`.
  * **Ground truth**: every run writes `/bronze_chaos/_manifests/run__{batch_id}.json` with the rate, seed, counts per error type, and every injected error (`file`, `type`, record `index`, `field`, `original` value). The data files carry no markers, so Silver must detect errors itself; its DLQ/dedup counts can then be scored against the union of these manifests.
  * **Reproducible & incremental**: each file's errors are seeded from `(seed, relative path)`, so daily runs and historical backfills damage a file identically. Files already in `/bronze_chaos/` are skipped; delete the folder to rebuild with a different `--rate`/`--seed`.
* **Silver Layer**: PySpark cleans the chaos copy (by default) into typed, deduplicated UTC tables: `prices`, `weather_hourly` and `zone_hourly` (every zone × hour, the table Gold reads), plus `dim_date`, `dim_station`, the issue log `dq_issues` (DLQ), the scorecard `dq_metrics` and a run manifest. See [Silver Layer](#silver-layer) below and the design in [docs/plans/silver_layer.md](docs/plans/silver_layer.md).
* **Gold Layer**: tables shaped for one consumer each, read by the Streamlit dashboard. Use case 2 (prices and weather) is built; use cases 1, 3 and 4 are planned. See [Gold Layer & Dashboard](#gold-layer--dashboard) below, [docs/plans/gold_layer.md](docs/plans/gold_layer.md) and [docs/plans/use_cases.md](docs/plans/use_cases.md).

---

## Getting Started

### 1. Configure Credentials

Create a local `.env` file (copied from `.env.example`):

```bash
cp .env.example .env
```

Add your MET Norway Client ID into `.env`:

```env
FROST_CLIENT_ID=your_frost_client_id_here
DAT535_ENV=dev
DAT535_DATA_DIR=./data
```

*(Note: `.env` is listed in `.gitignore` and is never committed to git).* Local runs then write to `./data/dev/` (see [Environments](#environments)).

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Diagnostic Smoke Test

Verify network connectivity, latency, credentials, and API response parsing:

```bash
python scripts/test_api_connections.py
```

### 4. Run Unit Tests

Execute the automated test suite:

```bash
pytest -v                    # everything (Spark tests need Java 8/11/17 and pyspark 3.5.0)
pytest -v -m "not spark"     # without the Spark tests
```

The same suite runs automatically in `.github/workflows/ci.yml` on every push to `dev` and every pull request to `dev`/`main`, on Python 3.11 with Java 8 like the VM. It uses GitHub-hosted runners, so the self-hosted VM runner is used only for jobs that touch real data.

* **Spark tests** are marked `spark` and are skipped automatically when `pyspark` is not installed. They start a local SparkSession and build small Bronze trees with the real `BronzeWriter`.
* **On Windows**, Spark cannot write files without Hadoop's `winutils`, so run the Spark tests in CI, on the VM, in WSL, or in a Linux container (e.g. `python:3.11` with a Java 17 JRE and `pip install -r requirements.txt`).

---

## Environments

Development and production are separated by the `DAT535_ENV` variable (`dev` or `prod`, default `dev`). Each environment has its own data tree under the data root (`DAT535_DATA_DIR`), so a dev run can never overwrite production data:

```text
~/spark-project-data/            # data root on the VM (DAT535_DATA_DIR)
├── prod/   bronze/  bronze_chaos/  silver/  gold/
└── dev/    bronze/  ...
```

| Environment | Who runs it | Git branch | Data directory |
|---|---|---|---|
| `dev` | Local runs, manual VM runs | `dev` (and local feature branches) | `<data root>/dev/` |
| `prod` | Daily schedule + manual runs | `main` only | `<data root>/prod/` |

* **Default is `dev`**: running a script without `DAT535_ENV` (locally or over SSH on the VM) never touches prod. An unknown value fails at startup.
* **Branch → environment**: `pipeline.yml` and `backfill.yml` derive `DAT535_ENV` from the branch they run on (`main` → `prod`, `dev` → `dev`). A *Guard branch* step fails any other branch before it can write data. The cron schedule only fires on `main`, so scheduled runs are always prod.
* **GitHub environments**: jobs run in the `prod` or `dev` GitHub environment. `prod` is restricted to the `main` branch under **Settings** → **Environments** → `prod` → **Deployment branches**.
* **Dev → prod flow**: develop on a feature branch → PR to `dev` (CI tests) → try it on the VM with **Run workflow** on branch `dev` (writes to `dev/`) → PR `dev` → `main` → the next scheduled run uses it on `prod/`.
* **Logs**: every run logs `Environment: <env>` and its data directory, and the step summary shows the environment and branch.

---

## Daily Ingestion Pipeline

### Running Manually

```bash
# 1. Ingest yesterday's complete 24h data (default)
python src/ingestion/ingest_daily.py

# 2. Ingest a specific historical date
python src/ingestion/ingest_daily.py --date 2024-01-15

# 3. Lookback catch-up: re-check the last 3 days (catches missed runs & MET revisions)
python src/ingestion/ingest_daily.py --lookback-days 3

# 4. Historical backfill: date range with rate-limit pacing (e.g., 0.5s between calls)
python src/ingestion/ingest_daily.py --start-date 2023-01-01 --end-date 2023-12-31 --request-delay 0.5

# 5. Dry run mode (validates API connectivity and parsing without writing to disk)
python src/ingestion/ingest_daily.py --date 2024-01-15 --dry-run

# 6. Build/extend the chaos copy of Bronze (default: 1% errors, seed 42)
python src/ingestion/chaos_injector.py
python src/ingestion/chaos_injector.py --rate 0.02 --seed 7
```

### Running on the VM Runner via GitHub Actions

The workflow `.github/workflows/pipeline.yml` runs automatically on the self-hosted runner. It has four jobs, each starting only after the previous one succeeds: `bronze` (ingestion), `bronze_chaos` (extends `/bronze_chaos/` with the newly ingested files), `silver` (rebuilds the Silver tables, see [Silver Layer](#silver-layer)) and `gold` (rebuilds the Gold tables, see [Gold Layer & Dashboard](#gold-layer--dashboard)). The last three are skipped on dry runs. Runs from `main` write to `prod/`, runs from `dev` to `dev/` (see [Environments](#environments)).

1. **Schedule**: Daily at `02:17 UTC` (cron `17 2 * * *`), automatically running a 3-day lookback catch-up. GitHub only runs scheduled workflows from the default branch, so the schedule is active once the workflow is on `main` (prod). GitHub can start scheduled runs late, especially at the start of an hour, so the cron avoids minute 0. At `0 2 * * *` the runs started 5.5–7 hours late.
2. **Quality Gate**: Runs the unit test suite without the Spark tests (`pytest -q -m "not spark"`) automatically prior to executing ingestion; CI runs the Spark tests.
3. **Concurrency & Safety**: Protected by a concurrency lock per branch (`data-pipeline-main`, `data-pipeline-dev`), so two runs never write the same environment at once and dev runs never delay prod. Inputs are checked with strict regexes to prevent shell injection.
4. **Manual Dispatch**: Go to **Actions** -> **Data Pipeline** -> **Run workflow**, choose the branch (`main` = prod, `dev` = dev):
   - Provide a `target_date` for a single day rerun.
   - Or provide `start_date` and `end_date` for bulk historical backfills.
   - Or adjust `lookback_days` (defaults to 3).
   - Optional `dry_run` checkbox.

Make sure `FROST_CLIENT_ID` is saved in your GitHub Repository under **Settings** > **Secrets and variables** > **Actions** > **New repository secret**.

---

## Historical Backfill

`src/ingestion/backfill_historical.py` loads multi-year history into the same `/bronze/` tree without interfering with the daily pipeline. It is a thin wrapper around the daily `run_ingestion()` (unchanged), so files, envelopes, and versioning are identical to daily ingestion.

* **History available**: Strømpris prices start on **2021-10-01**, but NO4 only on **2021-11-01**; Frost has data well before that. The project backfills from **2021-11-01**, the first day with prices for all five zones (`HISTORY_START` in `config/settings.py`).
* **One run per calendar month**: each month gets its own batch ID and run manifest, so a crash or timeout loses at most the current month's manifest.
* **Resumable**: months that already have a `SUCCESS` manifest are skipped. Rerun the same range after a failure; only unfinished months are requested again, and days already in Bronze are not rewritten (checksum `unchanged`).
* **Separate from daily runs**: end dates inside the daily 3-day lookback window are refused (latest allowed end date = today − 4 days, UTC), so backfill and daily runs never write the same files. The workflow uses its own concurrency lock per branch (`historical-backfill-main`, `historical-backfill-dev`), so a long backfill never delays the scheduled daily run.
* **Runtime**: about 9 s per day with the default 0.5 s request delay, i.e. ~1 h per year (~19k files, ~210 MB of Bronze for 2022 → today).

```bash
python src/ingestion/backfill_historical.py --start-date 2021-11-01 --end-date 2021-12-31
python src/ingestion/backfill_historical.py --start-date 2022-01-01 --end-date 2022-12-31 --request-delay 0.2
```

On the VM runner, use **Actions** -> **Historical Backfill** -> **Run workflow** (`.github/workflows/backfill.yml`, manual only, 6 h timeout) with a `start_date`, `end_date`, and optional `request_delay`. Running one calendar year per run is recommended. Run it from `main` to backfill prod; from `dev` it fills `dev/` (useful for a one-month test).

---

## Silver Layer

`src/silver/run_silver.py` rebuilds every Silver table from the whole input on each run (PySpark 3.5, DataFrame and SQL functions only, no Python UDFs). The design and the reasons behind each rule are in [docs/plans/silver_layer.md](docs/plans/silver_layer.md).

```bash
# Clean the chaos copy (default: SILVER_INPUT=bronze_chaos) into <data root>/<env>/silver/
python src/silver/run_silver.py

# Clean baseline: the untouched Bronze into a scratch folder, to compare with the chaos run
python src/silver/run_silver.py --input bronze --output-dir /tmp/silver_clean
```

| Table | One row per | Contents |
|---|---|---|
| `prices` | zone and UTC hour | Spot price excl. VAT in NOK and EUR, exchange rate, `is_repaired` |
| `weather_hourly` | station and UTC hour | 2 m temperature, 10 m wind, Frost quality codes, `*_is_imputed` |
| `zone_hourly` | zone and UTC hour | **The table Gold reads**: every hour, prices + zone weather + `hdh`, Norwegian date and hour, weekday/holiday, `has_price` / `has_weather` |
| `dim_date` | Norwegian date | Weekday, weekend, public holiday, `hours_in_day` (23/24/25) |
| `dim_station` | station | Zone, name and coordinates from `config/zones_and_stations.json` |
| `dq_issues` | problem found | The dead-letter queue: issue, action (`rejected` / `repaired` / `removed` / `excluded`), file, record, field, original record |
| `dq_metrics` | run and metric | Before/after scorecard and chaos score, appended on every run |
| `_runs/run__<id>.json` | run | Status, rows, checks, newest price and weather hour per zone |

* **Storage & time**: Parquet, partitioned by `year`/`month` of the Norwegian date, one file per month. `utc_hour` is UTC; readers must set `spark.sql.session.timeZone=UTC`.
* **Versions**: per zone/station and day, the newest readable, non-empty Bronze file is used; older versions are still checked, so their problems reach the issue log.
* **Cleaning**: price timestamps are parsed with their UTC offset (DST-safe) and a damaged NOK price is rebuilt as EUR × EXR; weather values that are null, impossible or flagged 5–7 by Frost are rejected, one series per element is used (2 m temperature, 10 m wind), and gaps of 1–2 hours are filled and flagged.
* **Checks before writing**: no duplicate or empty keys, whole hours, values within bounds, a complete zone × hour grid, non-empty `prices` and `weather_hourly`, and at most half of a source's records rejected. A failed hard check writes nothing and exits with code 1, so Gold keeps the previous Silver. Tables are written to `silver/_staging/` and swapped into place only after every write succeeded, so a crash while writing also leaves the previous Silver intact. Soft checks (incomplete days, weather completeness, rejected share) only warn.
* **Chaos score**: on the chaos copy, every error listed in `bronze_chaos/_manifests` is matched against `dq_issues`. The share found per error type and the issues not on the chaos list go to the run manifest, `dq_metrics` and the step summary.
* **Settings**: `SILVER_INPUT` (`bronze_chaos` or `bronze`), `SPARK_DRIVER_MEMORY` (default `2g`), `SPARK_SHUFFLE_PARTITIONS` (default 8).

---

## Gold Layer & Dashboard

`src/gold/run_gold.py` rebuilds every Gold table from the newest successful Silver run (PySpark, DataFrame functions only). The design is in [docs/plans/gold_layer.md](docs/plans/gold_layer.md).

```bash
python src/gold/run_gold.py                                                    # <data root>/<env>/silver -> gold
python src/gold/run_gold.py --silver-dir /tmp/silver_clean --output-dir /tmp/gold_clean
```

| Table | One row per | Contents |
|---|---|---|
| `zone_daily` | zone and Norwegian day | Average/min/max price, volatility, peak vs off-peak, negative hours, temperature, heating degree days, wind, hour counts, `is_complete` |
| `price_weather_sensitivity` | zone and month | Monthly averages, and the correlation and slope of the daily price on temperature and wind (complete days only) |
| `price_profile` | zone, year, season, day type, hour | Average price per Norwegian hour, for workdays vs weekends/holidays and winter vs summer |
| `household_bill_monthly` | zone, month and scheme (from 2024) | A modelled household's kWh and bill with no support, strømstønad and Norgespris, support received, `is_final` |
| `_status/latest.json` | newest successful run | When Gold was built, from which Silver run, rows, `data_through` (newest day complete in every zone), checks |

* **Checks before writing**: no duplicate or empty keys, no day longer than its 23/24/25 hours, a non-empty `zone_daily`, the same number of price hours as Silver, correlations in [−1, 1], and for the household bill: no month longer than its hours, each month's price hours equal to `zone_daily`, Norgespris bills equal to kWh × 0.40 plus VAT, and no month above the 5,000 kWh support cap. A failed check writes nothing (exit code 1), and the dashboard keeps showing the previous Gold. Every run also writes `_status/runs/run__<id>.json`.

### Dashboard

A Streamlit app with one page per use case. Use cases 2 (prices and weather) and 3 (monthly household bill) are built. The other use cases have placeholder pages generated from [dashboard/registry.py](dashboard/registry.py), and adding one is a page module plus a registry change (see [gold_layer.md](docs/plans/gold_layer.md), section 6). The sidebar shows when Gold was built, how far the data reaches, and a warning when the data is late (not complete through yesterday by 06:00) or the last Gold run failed.

**Locally, without the VM** (also on Windows; the sample tables are synthetic and marked as such on every page):

```bash
pip install -r requirements.txt -r requirements-dashboard.txt
python scripts/make_sample_gold.py           # synthetic Gold into ./data/dev/gold
streamlit run dashboard/app.py               # http://localhost:8501
```

**On the VM**, with the real Gold of an environment:

```bash
source ~/spark-env/bin/activate && pip install -r requirements-dashboard.txt
DAT535_ENV=prod DAT535_DATA_DIR=~/spark-project-data \
  streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true
```

It listens only on the VM itself. Open it from your laptop through an SSH tunnel, then browse to http://localhost:8501:

```bash
ssh -L 8501:localhost:8501 ubuntu@<vm floating ip>
```

To keep it running after you log out, use the systemd unit in [deploy/dat535-dashboard.service](deploy/dat535-dashboard.service). It runs the dashboard from its own clone, because the Actions runner replaces its work folder on every run. `DAT535_GOLD_DIR` points the dashboard at any other Gold folder.

---

## Lab Exercises (Labs 1, 2, 3)

The previous lab exercises and notebooks have been moved to the `labs/` directory to keep the root directory clean:

```bash
cd labs
python run_pipeline.py all
```

The lab GitHub Actions workflows are kept in `labs/workflows/` for reference only. GitHub runs workflows only from `.github/workflows/`, so they no longer trigger on push, PR, or schedule and don't occupy the self-hosted runner.
