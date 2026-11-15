# DAT535 Data Engineering: Regional Power Grid Economics and Microclimate Dynamics

An automated, end-to-end medallion pipeline (Bronze → Silver → Gold) built with **PySpark** and **Parquet**, run by **GitHub Actions** on a self-hosted VM runner and a 3-worker **Spark standalone cluster**. It ingests Norwegian day-ahead electricity spot prices for the bidding zones `NO1`–`NO5` and weather observations from MET Norway, cleans and combines them, and serves three consumers: an app/API, a dashboard and a monthly report.

```text
 hvakosterstrommen.no ─┐
  (prices, 5 zones)    │
                       ├─► Bronze ──► Bronze chaos copy ──► Silver ──► Gold ──► Streamlit dashboard
 MET Frost ────────────┘   raw JSON,   ~1% of records       clean,     tables      + HTTP API (FastAPI)
  (weather, 6 stations)    versioned,  damaged on purpose   hourly,    per
                           never       to test Silver       checked    consumer
                           overwritten
```

**Contents:** [Results](#results-at-a-glance) · [Repository](#repository-structure) · [Workflows](#workflows) · [Getting started](#getting-started) · [Environments and cluster](#environments-and-the-spark-cluster) · [Bronze](#bronze-layer) · [Silver](#silver-layer) · [Performance](#silver-performance) · [Gold, dashboard and API](#gold-layer-dashboard-and-api) · [Design decisions](#design-decisions) · [Labs and notebooks](#labs-and-notebooks)

---

## Results at a glance

From the scheduled prod run **Data Pipeline #50** (15 Nov 2026), which took 4 min 9 s end to end:

| Layer | Result |
|---|---|
| **Bronze** | Prices and weather from 2021-11-01, about 20,000 JSON files (~340 MB); every version kept, nothing overwritten |
| **Silver** | Rebuilt in 71 s on the cluster: 220,268 price hours, 263,631 station-hours, `zone_hourly` = 5 zones × 44,184 hours. 19 of 19 hard checks passed |
| **Cleaning evidence** | **3,941 of 3,942 injected chaos errors found (99.97%)**. The 3,067 issues not on the chaos list are real source problems: 2,967 values Frost itself flagged, and the 100 rows of four faulty API files for the 2024-10-27 DST day |
| **Gold** | Rebuilt in 39 s: 6 tables for 3 use cases, 25 of 25 hard checks passed; Gold counts the same 220,268 price hours as Silver |
| **Tests** | 158 test functions (incl. Spark tests), run by CI on every push to `dev` and every pull request |

---

## Repository structure

```text
.
├── .github/workflows/
│   ├── ci.yml                  # All tests incl. Spark, on GitHub-hosted runners (push to dev, PRs to dev/main)
│   ├── pipeline.yml            # Nightly: Bronze -> chaos copy -> Silver -> Gold on the VM runner
│   ├── day_ahead.yml           # Afternoon: tomorrow's prices -> chaos copy -> Silver -> Gold (use case 1)
│   ├── backfill.yml            # Manual historical Bronze backfill
│   └── perf.yml                # Manual Silver performance experiments, one batch at a time
├── config/
│   ├── settings.py             # dev/prod environment, data paths, Silver input, API settings
│   ├── price_rules.py          # VAT, strømstønad, Norgespris and the modelled household (Gold use cases 1 and 3)
│   └── zones_and_stations.json # Bidding zones NO1-NO5 -> MET Norway weather stations
├── src/
│   ├── ingestion/              # Bronze
│   │   ├── strompris_client.py     # Price API client (hvakosterstrommen.no)
│   │   ├── frost_client.py         # MET Frost API client (auth, hourly series only)
│   │   ├── bronze_writer.py        # Audit envelopes; versioned, checksum-deduplicated, atomic JSON writes
│   │   ├── ingest_daily.py         # Ingestion CLI: daily lookback, one date, a range, day-ahead
│   │   ├── backfill_historical.py  # Historical backfill: month by month, resumable
│   │   └── chaos_injector.py       # bronze_chaos/: a Bronze copy with ~1% injected errors and a ground-truth log
│   ├── silver/
│   │   ├── run_silver.py           # Silver CLI: rebuild, check, stage, swap, run manifest
│   │   ├── spark_session.py        # SparkSession factory (SPARK_MASTER, UTC, AQE, 8 shuffle partitions)
│   │   ├── bronze_reader.py        # Fixed schemas, truncated files, lineage, version choice
│   │   ├── prices.py               # -> prices (DST-safe UTC, repairs from EUR x EXR)
│   │   ├── weather.py              # -> weather_hourly (series choice, quality codes, gap filling)
│   │   ├── zone_hourly.py          # -> zone_hourly: every zone x hour, prices + zone weather + hdh (what Gold reads)
│   │   ├── reference.py            # -> dim_date (holidays, hours per day) and dim_station
│   │   └── quality.py              # Issue log (DLQ), checks, scorecard, chaos score, run manifest
│   ├── gold/
│   │   ├── run_gold.py             # Gold CLI: rebuild, check, stage, swap, status file
│   │   ├── cheapest_hours.py       # Use case 1: consumer_prices_hourly, cheapest_hours
│   │   ├── dashboard.py            # Use case 2: zone_daily, price_weather_sensitivity, price_profile
│   │   └── household_bill.py       # Use case 3: household_bill_monthly
│   └── utils/
│       ├── hashing.py              # Batch UUIDs, UTC timestamps, SHA-256 payload checksums
│       └── http.py                 # Shared requests session with retry/backoff and User-Agent
├── api/                        # Cheapest hours HTTP API for apps and chargers (FastAPI; guide: docs/api.md)
│   ├── main.py                 # Endpoints: cheapest hours, health, zones
│   └── gold.py                 # Reads Gold (cached) and builds every answer; the dashboard reuses it
├── dashboard/                  # Streamlit dashboard over Gold
│   ├── app.py                  # Entry point: menu from registry.py, sidebar data status
│   ├── registry.py             # The use cases: question, user, deadline, status, page module
│   ├── data.py, ui.py, charts.py   # Reading Gold with pandas, cached loading, Altair charts
│   └── views/                  # One page per use case, the API page, the overview, a placeholder
├── deploy/                     # systemd units that keep the dashboard and the API running on the VM
├── scripts/
│   ├── test_api_connections.py # Smoke test for API connectivity and credentials
│   ├── make_sample_gold.py     # Synthetic Gold, to try the dashboard without the VM
│   └── perf_silver.py          # Silver performance experiments (x1, x5, x10, tuning)
├── tests/                      # pytest: ingestion, chaos, backfill, day-ahead, settings, Silver, Gold, API, dashboard, perf
├── docs/
│   ├── api.md                  # The API guide (endpoints, status codes, examples)
│   ├── plans/                  # Design: silver_layer.md, gold_layer.md, use_cases.md
│   └── Data_Engineering_Project_Definition_Guide_v3.pdf   # The course guide
├── labs/                       # Archived course labs 1-3, incl. their two notebooks (not part of the pipeline)
├── dat535_data_engineering_project_definition.md          # Our original project definition (see Design decisions for what changed)
├── requirements.txt            # Pipeline dependencies (pyspark pinned to the VM's 3.5.0)
├── requirements-dashboard.txt  # Dashboard and API dependencies (Streamlit, pandas, pyarrow, FastAPI)
├── pyproject.toml              # pytest configuration (import path, the `spark` marker)
└── .env.example                # Template for local settings and the Frost credentials
```

---

## Workflows

| Workflow | Trigger | Runs on | What it does |
|---|---|---|---|
| **CI** ([ci.yml](.github/workflows/ci.yml)) | Push to `dev`, PRs to `dev`/`main` | GitHub-hosted (Python 3.11, Java 8 like the VM) | All tests, including the Spark tests |
| **Data Pipeline** ([pipeline.yml](.github/workflows/pipeline.yml)) | Nightly at 02:17 UTC on `main`; manual on `main` or `dev` | VM runner + Spark cluster | `bronze` → `bronze_chaos` → `silver` → `gold`, each job only after the previous one succeeded |
| **Day-ahead Prices** ([day_ahead.yml](.github/workflows/day_ahead.yml)) | Every 30 min 11:07–15:37 UTC on `main`; manual | VM runner + Spark cluster | Tomorrow's prices; rebuilds Silver and Gold only when new prices arrived |
| **Historical Backfill** ([backfill.yml](.github/workflows/backfill.yml)) | Manual | VM runner | Loads history into Bronze, month by month |
| **Silver Performance** ([perf.yml](.github/workflows/perf.yml)) | Manual, on `dev` | VM runner + Spark cluster | One batch of performance experiments on a frozen copy of the data |

- **Branch → environment:** runs from `main` write `prod/`, runs from `dev` write `dev/`. A *Guard branch* step refuses any other branch before it touches data.
- **Never two writers at once:** the pipeline and the day-ahead run share a concurrency lock per branch; the backfill has its own.
- **Where to see the results:** each run's summary page shows the Bronze parameters, Silver's rows, checks, issues and chaos score, and Gold's rows, checks and freshness. The dashboard's sidebar shows when Gold was built and how far the data reaches.

---

## Getting started

### 1. Credentials and settings

```bash
cp .env.example .env
```

Add your MET Norway Client ID ([free registration](https://frost.met.no/auth/requestCredentials.html)) to `.env`:

```env
FROST_CLIENT_ID=your_frost_client_id_here
DAT535_ENV=dev
DAT535_DATA_DIR=./data
```

`.env` is in `.gitignore` and never committed. Local runs then write to `./data/dev/` (see [Environments](#environments-and-the-spark-cluster)). On GitHub, `FROST_CLIENT_ID` is a repository secret (**Settings** → **Secrets and variables** → **Actions**).

### 2. Install and check

```bash
pip install -r requirements.txt                 # the pipeline
pip install -r requirements-dashboard.txt       # the dashboard and the API
python scripts/test_api_connections.py          # connectivity, credentials, response parsing
```

### 3. Run the tests

```bash
pytest -v                    # everything (the Spark tests need Java 8/11/17 and pyspark 3.5.0)
pytest -v -m "not spark"     # without the Spark tests
```

- **Spark tests** are marked `spark` and are skipped when `pyspark` is not installed. They start a local SparkSession and build small Bronze trees with the real `BronzeWriter`.
- **On Windows**, Spark can't write files without Hadoop's `winutils`, so run the Spark tests in CI, on the VM, in WSL, or in a Linux container (e.g. `python:3.11` with a Java 17 JRE and `pip install -r requirements.txt`).

### 4. Run the pipeline end to end on your machine

The same commands the workflows run. Silver and Gold need Java (and on Windows a Linux environment, as above).

```bash
python src/ingestion/ingest_daily.py --start-date 2024-01-01 --end-date 2024-01-31   # Bronze: one month
python src/ingestion/chaos_injector.py                                                # Bronze chaos copy
python src/silver/run_silver.py                                                       # Silver
python src/gold/run_gold.py                                                           # Gold
streamlit run dashboard/app.py                                                        # http://localhost:8501
uvicorn api.main:app --port 8000                                                      # http://localhost:8000/
```

Without Frost credentials, add `--allow-mock-weather` to the ingestion (the files are flagged `_is_synthetic`) and `--allow-synthetic` to Silver. To look at the dashboard only, `python scripts/make_sample_gold.py` writes synthetic Gold tables instead, marked as such on every page.

---

## Environments and the Spark cluster

Development and production are separated by `DAT535_ENV` (`dev` or `prod`, default `dev`). Each environment has its own data tree under the data root (`DAT535_DATA_DIR`), so a dev run can never overwrite production data:

```text
~/spark-project-data/            # data root on the VM (DAT535_DATA_DIR)
├── prod/   bronze/  bronze_chaos/  silver/  gold/
└── dev/    bronze/  ...
```

| Environment | Who runs it | Git branch | Data directory |
|---|---|---|---|
| `dev` | Local runs, manual VM runs | `dev` (and local feature branches) | `<data root>/dev/` |
| `prod` | Nightly and day-ahead schedules, manual runs | `main` only | `<data root>/prod/` |

- **Default is `dev`**: a script started without `DAT535_ENV` never touches prod. An unknown value fails at startup.
- **GitHub environments**: jobs run in the `prod` or `dev` GitHub environment; `prod` is restricted to `main` (**Settings** → **Environments** → `prod` → **Deployment branches**).
- **Dev → prod flow**: feature branch → PR to `dev` (CI) → **Run workflow** on `dev` on the VM (writes `dev/`) → PR `dev` → `main` → the next scheduled run uses it on `prod/`.

**Spark cluster.** Silver and Gold run on a Spark 3.5 standalone cluster: master `spark://spark:7077` and three workers (the `spark` VM, `spark-worker-1` and `spark-worker-2`, 4 cores and 6.8 GiB each).

- **Shared data:** the `spark` VM shares `/home/ubuntu/spark-project-data` over NFS, and the other workers mount it at the same path, so every executor reads and writes the same files. A cluster run must write under this folder, because `/tmp` is separate on each machine.
- **Settings:** the workflows set `SPARK_MASTER` and `PYSPARK_PYTHON` (the workers' system `python3` is a different version from the runner's). `SPARK_EXECUTOR_MEMORY` defaults to `3g`. Runs by hand and tests stay on one machine (`local[*]`) unless `SPARK_MASTER` is set.
- **If the cluster is down:** the job fails (or waits until the workflow's timeout), and nothing is written, so the previous Silver and Gold stay in place.

---

## Bronze layer

### Sources

| Source | Data | Access | Files in Bronze |
|---|---|---|---|
| **Strømpris API** (`hvakosterstrommen.no`) | Hourly day-ahead spot prices per zone `NO1`–`NO5`, in NOK and EUR per kWh excl. VAT; tomorrow's are published around 13:00 Norwegian time | Open, no key | One per zone and **Norwegian** day (23 or 25 hours on DST days) |
| **MET Norway Frost API** (`frost.met.no`) | Hourly `air_temperature` and `wind_speed` from 6 stations (NO2 has two) | Free `Client ID` | One per station and **UTC** day |

Frost requests use `timeresolutions=PT1H`; without it some stations also return 1- and 10-minute series (~2 MB a day instead of ~25 KB). Because prices and weather files cover different days, Silver joins them on the UTC hour, never file by file.

### Storage: immutable, versioned, never overwritten

- **Layout:** `bronze/electricity/zone={zone}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}__{ingestTs}_{batch}.json`, and the same under `bronze/weather/station={station_id}/…`.
- **Audit envelope:** every file holds the API's records unchanged in `payload`, plus `_metadata` with `_batch_id`, `_ingestion_timestamp_utc`, `_source_uri`, `_payload_checksum` (SHA-256 of the payload), `_record_count`, `_is_synthetic` and `_schema_version`.
- **Versioning:** files are never overwritten. On re-ingestion, the payload's checksum is compared with the newest version for that day: identical → nothing is written (`unchanged`); different → a new file (`new_version`, e.g. after MET's quality control). Every version name is unique, and a name collision gets a suffix instead of overwriting. Legacy `{YYYY}_{MM}_{DD}.json` files count as the first version.
- **Atomic writes:** each file is written to a hidden `.{name}.tmp` and then renamed, so a crash never leaves a partial file that Spark would read (Spark skips names starting with `.` or `_`).
- **Run manifests:** every non-dry run writes `bronze/_manifests/run__{batch_id}.json` with record counts, errors, and the written files with their status.

### Ingestion

The nightly pipeline re-checks the last 3 complete days (`--lookback-days 3`): a day missed by a failed run is created, a day MET corrected becomes a new version, and the rest is skipped by checksum. Before ingesting, the VM runs the tests without Spark (`pytest -q -m "not spark"`).

```bash
python src/ingestion/ingest_daily.py                          # yesterday (UTC)
python src/ingestion/ingest_daily.py --date 2024-01-15        # one date
python src/ingestion/ingest_daily.py --lookback-days 3        # re-check the last 3 days (the nightly run)
python src/ingestion/ingest_daily.py --start-date 2023-01-01 --end-date 2023-12-31 --request-delay 0.5
python src/ingestion/ingest_daily.py --date 2024-01-15 --dry-run   # fetch and parse without writing
python src/ingestion/ingest_daily.py --day-ahead              # tomorrow's prices (exit code 3 = not published yet)
```

Only complete past days are accepted (up to yesterday, UTC); the day-ahead mode is the one exception, and it fetches prices only. On the VM: **Actions** → **Data Pipeline** → **Run workflow**, on `main` (prod) or `dev`, with an optional `target_date`, a `start_date`/`end_date` range, `lookback_days` (default 3) or `dry_run`. Inputs are checked with strict regexes to prevent shell injection.

**Day-ahead run** ([day_ahead.yml](.github/workflows/day_ahead.yml), use case 1). Tomorrow's prices are published around 13:00 Norwegian time, and the cheapest-hours page promises them by 14:30. The schedule tries every 30 minutes from 11:07 to 15:37 UTC, off the hour. A try before publication ends after five API calls. When new price files arrive, or tomorrow is published but Gold doesn't have it yet, it runs `bronze_chaos` → `silver` → `gold` like the nightly run.

**Schedule: target and observed.** GitHub starts scheduled workflows late, and we can't control that. The nightly run is due at 02:17 UTC but started 6.5–7.5 hours late in November 2026 (around 10:00–11:00 Norwegian time). The day-ahead schedule has ten slots a day, but GitHub started one run a day, in the evening. The pipeline itself takes about 4 minutes. The dashboard says when its data is late instead of showing old numbers as current.

### Historical backfill

[backfill_historical.py](src/ingestion/backfill_historical.py) loads history into the same Bronze tree through the daily `run_ingestion()`, so files, envelopes and versioning are identical.

- **From 2021-11-01**, the first day with prices for all five zones (NO4 starts in November 2021; `HISTORY_START` in `config/settings.py`).
- **One run per calendar month**, each with its own batch ID and manifest. **Resumable:** months with a `SUCCESS` manifest are skipped.
- **Separate from the nightly run:** end dates inside the 3-day lookback window are refused (latest end date = today − 4 days), and it has its own concurrency lock.
- **Runtime:** about 9 s per day with the default 0.5 s request delay, about an hour per year.

```bash
python src/ingestion/backfill_historical.py --start-date 2021-11-01 --end-date 2021-12-31
python src/ingestion/backfill_historical.py --start-date 2022-01-01 --end-date 2022-12-31 --request-delay 0.2
```

On the VM: **Actions** → **Historical Backfill** → **Run workflow** (manual only, 6 h timeout), one calendar year per run.

### Bronze chaos copy

The APIs return very clean data, so [chaos_injector.py](src/ingestion/chaos_injector.py) mirrors Bronze into `bronze_chaos/` with about 1% of the records damaged, and **prod Silver cleans this copy** (`SILVER_INPUT=bronze_chaos`). Bronze itself is only read, and the injector refuses an output folder inside it.

- **Error types:** `duplicate` (record twice), `null_field` (price or observation value → `null`), `out_of_range` (999.0 NOK/kWh, 999.9 °C), `bad_timestamp` (`"not-a-timestamp"`), and `corrupt_file` (file truncated mid-JSON; 20% of the budget).
- **Ground truth:** every run writes `bronze_chaos/_manifests/run__{batch_id}.json` with each injected error (file, type, record index, field, original value). The data files carry no markers, so Silver must find the errors itself; its issue log is then scored against these manifests.
- **Reproducible and incremental:** each file's errors are seeded from `(seed, relative path)`, so daily runs and backfills damage a file identically. Files already in `bronze_chaos/` are skipped; delete the folder to rebuild with another `--rate`/`--seed`.

```bash
python src/ingestion/chaos_injector.py                     # 1% errors, seed 42
python src/ingestion/chaos_injector.py --rate 0.02 --seed 7
```

---

## Silver layer

[run_silver.py](src/silver/run_silver.py) rebuilds every Silver table from the whole input on each run, with PySpark DataFrame and SQL functions only (no Python UDFs). Silver has no business rules: VAT and support schemes belong to Gold. The design and the reasons behind each rule are in [docs/plans/silver_layer.md](docs/plans/silver_layer.md).

```bash
python src/silver/run_silver.py                                               # SILVER_INPUT (bronze_chaos) -> <data root>/<env>/silver/
python src/silver/run_silver.py --input bronze --output-dir /tmp/silver_clean # clean baseline, to compare with the chaos run
```

| Table | One row per | Contents |
|---|---|---|
| `prices` | zone and UTC hour | Spot price excl. VAT in NOK and EUR, exchange rate, `is_repaired`, lineage |
| `weather_hourly` | station and UTC hour | 2 m temperature, 10 m wind, Frost quality codes, `*_is_imputed`, lineage |
| **`zone_hourly`** | zone and UTC hour | **The table Gold reads**: every hour, price + zone weather + `hdh` (heating degree hours), Norwegian date and hour, weekday/holiday, `has_price` / `has_weather` |
| `dim_date` | Norwegian date | Weekday, weekend, public holiday, `hours_in_day` (23/24/25) |
| `dim_station` | station | Zone, name and coordinates from `config/zones_and_stations.json` |
| `dq_issues` | problem found | The dead-letter queue: issue, action (`rejected` / `repaired` / `removed` / `excluded`), file, record, field, original record |
| `dq_metrics` | run and metric | Before/after scorecard and chaos score, appended on every run |
| `_runs/run__<id>.json` | run | Status, rows, checks, newest price and weather hour per zone |

- **Storage and time:** Parquet; `prices`, `weather_hourly` and `zone_hourly` partitioned by `year`/`month` of the Norwegian date, one file per month. `utc_hour` is UTC; readers must set `spark.sql.session.timeZone=UTC`.
- **Reading:** fixed schemas, no type inference. A truncated file lands in `_corrupt_record` (`PERMISSIVE` mode) instead of failing the job. Per zone/station and day, the newest readable, non-empty version is used; older versions are still checked, so their problems reach the issue log.
- **Cleaning prices:** timestamps are parsed with their UTC offset (DST-safe); an unreadable start is rebuilt from `time_end`; a null or impossible NOK price is rebuilt exactly as EUR × EXR. A file with an hour of another day is rejected whole: the API's 2024-10-27 files for four zones have shifted labels.
- **Cleaning weather:** values that are null, impossible or flagged 5–7 by Frost ("very uncertain" or "erroneous") are rejected; one series per element is used (hourly, 2 m temperature, 10 m wind); gaps of 1–2 hours are filled with the last value and flagged.
- **`zone_hourly`:** a complete grid of every zone × hour, with prices and zone weather (the mean of its stations) left-joined onto it. An inner join would drop tomorrow's prices, which have no weather yet; the grid keeps them and makes every gap visible.
- **Checks before writing:** no duplicate or empty keys, whole hours, values within bounds, a complete zone × hour grid, non-empty `prices` and `weather_hourly`, and at most half of a source's records rejected. A failed hard check writes nothing and exits with code 1, so Gold keeps the previous Silver. Soft checks (incomplete days, weather completeness, rejected share) only warn.
- **Safe publishing:** after the checks pass, tables are written to `silver/_staging/` and swapped into place only when every write succeeded, so a crash while writing also leaves the previous Silver intact. The run manifest is written last.
- **Chaos score:** every error in `bronze_chaos/_manifests` is matched against `dq_issues` on file, record and field. The share found per error type and the issues not on the chaos list go to the run manifest, `dq_metrics` and the step summary.
- **Settings:** `SILVER_INPUT` (`bronze_chaos` or `bronze`), `SPARK_MASTER` (default `local[*]`), `SPARK_DRIVER_MEMORY` (default `2g`), `SPARK_EXECUTOR_MEMORY` (default `3g`), `SPARK_SHUFFLE_PARTITIONS` (default 8).

---

## Silver performance

[scripts/perf_silver.py](scripts/perf_silver.py) measures how Silver's run time grows with the data, on one VM and on the cluster, and tests the settings that would change with it. It runs on a frozen copy of prod's `bronze_chaos` and doesn't change Silver's code. The design is in [silver_layer.md](docs/plans/silver_layer.md), section 10.

**Running it:** **Actions** → **Silver Performance** → **Run workflow**, on `dev`, one batch at a time, starting with `setup`.

| Batch | What it does |
|---|---|
| `setup` | Copies `prod/bronze_chaos` to `perf/input/full` (×1), then builds ×5 and ×10 by copying every zone and station folder under new names (`zone=NO1_r1`, …) |
| `scale` | One VM (3 GB, the same as one cluster executor) against the cluster, at ×1, ×5 and ×10, with the project's own settings |
| `shuffle` | 8 (today), 24 or 200 shuffle partitions on the cluster, at the chosen `size` |
| `read_tasks` | How many small JSON files one read task gets: Spark's default `openCostInBytes` (4 MB) or 16 KB |

- **Method:** one warm-up run, then each variant 3 times in turn, each in a new process. A difference counts only if the ranges don't overlap, and a run counts only if its output matches the reference.
- **Results:** every run's **Artifacts** hold `summary.md`, `runs.csv`, the query plans and the per-stage figures. Everything stays under `<data root>/perf/`; only `setup` touches prod, and only reads it.

**Results (14 Nov 2026, 46 measured runs):**

| Input | Size | Cluster (3 machines) | One VM (3 GB) |
|---|---|---|---|
| ×1 | 338 MB | 69.8 s | 60.6 s (within the run-to-run variation) |
| ×5 | 1.7 GB | 119.2 s | 134.8 s |
| ×10 | 3.4 GB | 171.0 s | out of memory |

- **At today's size the cluster doesn't pay off yet**; it wins from about 0.75 GB (roughly twice today's data) and is the only setup that finishes at ×10. Prod runs on the cluster for that reason.
- **Reading and parsing the small Bronze JSON files dominates** (about half of a run at ×1, 72% at ×10); writing and the checks barely grow.
- **No setting changed:** 24 or 200 shuffle partitions made no measurable difference against 8, and packing more files into each read task didn't help. A full rebuild takes under 3 minutes even at ×10, so Silver needs no incremental mode.

---

## Gold layer, dashboard and API

[run_gold.py](src/gold/run_gold.py) rebuilds every Gold table from the newest successful Silver run. It reads only Silver's `zone_hourly` and `dim_date`; every table is shaped for one consumer, and the business rules live here ([config/price_rules.py](config/price_rules.py)). The design is in [docs/plans/gold_layer.md](docs/plans/gold_layer.md) and [docs/plans/use_cases.md](docs/plans/use_cases.md).

```bash
python src/gold/run_gold.py                                                    # <data root>/<env>/silver -> gold
python src/gold/run_gold.py --silver-dir /tmp/silver_clean --output-dir /tmp/gold_clean
```

| # | Consumer | Question | Ready by (Norwegian time) | Status |
|---|---|---|---|---|
| 1 | Application (API + page) | When should I charge the car or run the dishwasher tomorrow, and what will it cost? | 14:30 | Built |
| 2 | Dashboard | How do prices differ between zones, and how much do cold and wind move them? | 06:00 | Built |
| 3 | Report | What did a household pay last month, and was Norgespris cheaper than strømstønad? | 06:00 on the 1st | Built |
| 4 | Analytical model | Roughly what will tomorrow's prices be, before they are published? | – | Not built (placeholder page) |

| Table | Use case | One row per | Contents |
|---|---|---|---|
| `consumer_prices_hourly` | 1 | zone and hour (from 2024) | Spot price, VAT, strømstønad and the price after it, the Norgespris price |
| `cheapest_hours` | 1 | zone, Norwegian day and block length (1, 3, 6 h) | The cheapest block's start and end, its average household price, the block starting at 17:00, the saving per kWh |
| `zone_daily` | 2 | zone and Norwegian day | Average/min/max price, volatility, peak vs off-peak, negative hours, temperature, heating degree days, wind, hour counts, `is_complete` |
| `price_weather_sensitivity` | 2 | zone and month | Monthly averages, and the correlation and slope of the daily price on temperature and wind (complete days only) |
| `price_profile` | 2 | zone, year, season, day type, hour | Average price per Norwegian hour: workdays vs weekends/holidays, winter vs summer |
| `household_bill_monthly` | 3 | zone, month and scheme (from 2024) | A modelled household's kWh (`0.6 + 0.1 × hdh` per hour) and bill with no support, strømstønad and Norgespris; support received; `is_final` |
| `_status/latest.json` | all | newest successful run | When Gold was built, from which Silver run, rows, `data_through` (newest day complete in every zone), the zones that have tomorrow, checks |

- **Checks before writing** (25, all hard): no duplicate or empty keys; no day longer than its 23/24/25 hours; a non-empty `zone_daily`; the same number of price hours as Silver; correlations in [−1, 1]; for use case 1, as many priced hours as `zone_daily`, prices that follow the VAT and strømstønad rules, three blocks per complete day, none dearer than the 17:00 block; for use case 3, no month longer than its hours, each month's price hours equal to `zone_daily`, Norgespris bills equal to kWh × 0.40 plus VAT, no month above the 5,000 kWh support cap. A failed check writes nothing (exit code 1), and the dashboard keeps the previous Gold.
- **Safe publishing:** the same staging and swap as Silver; `_status/latest.json` is written last, and every run also writes `_status/runs/run__<id>.json`.

### Dashboard

A Streamlit app with one page per use case (1, 2 and 3), an **API for apps** page, an overview and a placeholder for use case 4, all generated from [dashboard/registry.py](dashboard/registry.py). The sidebar shows when Gold was built, how far the data reaches, and a warning when the data is late (not complete through yesterday after 06:00) or the last Gold run failed.

```bash
python scripts/make_sample_gold.py      # locally, without the VM: synthetic Gold into ./data/dev/gold
streamlit run dashboard/app.py          # http://localhost:8501
```

By hand on the VM, e.g. to look at dev's Gold (the prod service already uses port 8501):

```bash
source ~/dashboard-env/bin/activate
DAT535_ENV=dev DAT535_DATA_DIR=~/spark-project-data \
  streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8503 --server.headless true
```

### Cheapest hours API

Apps and smart chargers get use case 1 as JSON from a small, open (no key, read-only) FastAPI app, [api/main.py](api/main.py). For example, `GET /api/v1/cheapest-hours/NO1/block?hours=6&day=tomorrow` says when the cheapest 6 hours of tomorrow start and end. Before tomorrow's prices are published it answers `503` with a `Retry-After` header. The full guide, with examples for a charger and Home Assistant, is **[docs/api.md](docs/api.md)**.

```bash
uvicorn api.main:app --port 8000          # http://localhost:8000/ lists the endpoints
```

### Deployment on the VM

Both run as systemd services on the VM, serving prod Gold: [deploy/dat535-dashboard.service](deploy/dat535-dashboard.service) and [deploy/dat535-api.service](deploy/dat535-api.service), whose headers have the install steps.

- **Their own clone and environment:** both run from the clone `~/dat535-group6` (the Actions runner replaces its work folder on every run) with the Python environment `~/dashboard-env` (`requirements-dashboard.txt`), kept apart from the runner's `~/spark-env`.
- **Updating:** run the pipeline on `main` first, so prod Gold already has any new tables, then `git -C ~/dat535-group6 pull && sudo systemctl restart dat535-dashboard dat535-api`. New Gold data needs no restart: both read the tables again after a new Gold run.
- **Access:** they listen only on `127.0.0.1`, so nothing is public. Open them from your laptop through an SSH tunnel:

```bash
ssh -L 8501:localhost:8501 -L 8000:localhost:8000 ubuntu@<vm floating ip>
```

---

## Design decisions

The main decisions, and where we changed the original [project definition](dat535_data_engineering_project_definition.md). The reasons are in [silver_layer.md](docs/plans/silver_layer.md) (D1–D10) and [gold_layer.md](docs/plans/gold_layer.md) (G1–G13).

| Decision | Why |
|---|---|
| **Bronze is immutable and versioned** (unique names, checksum dedup, atomic writes) | Reruns are safe and free; corrected source data is kept as history, not overwritten |
| **Prod Silver cleans the chaos copy** (D1) | Cleaning is exercised and scored on every run. The cost: truncated files can't be repaired |
| **Full rebuild on every run** (D3) | Simple, and a rerun always gives the same result; measured at 71 s today and under 3 minutes at ×10 |
| **Parquet with a staging swap, instead of Delta Lake** (D2) | Checks before writing plus the swap give an all-or-nothing publish without another dependency |
| **UTC keys, Norwegian time as extra columns** (D5) | Daylight saving is handled once; on the autumn day 02:00 happens twice |
| **Every problem logged with its action** (D7) | `dq_issues` is the dead-letter queue, and it can be scored against the chaos list |
| **A zone × hour grid with left joins, instead of an inner join** | Keeps tomorrow's prices (no weather yet) and makes gaps visible as `has_price` / `has_weather` |
| **`PERMISSIVE` mode instead of `badRecordsPath`** | `badRecordsPath` exists only on Databricks |
| **Own checks in `quality.py`, instead of Great Expectations** | Avoids a dependency; the checks are plain Spark |
| **A station lookup from config, instead of zone polygons** (D9) | We picked the stations for each zone by hand |
| **No business rules in Silver; Gold shaped per consumer type** (D8) | One clean table serves an app, a dashboard and a report; price rules change in one place |
| **A separate afternoon workflow and an HTTP API for tomorrow's prices** (G10, G13) | The nightly run only collects finished days; the API can answer for today and tomorrow and tells a device when to retry |
| **The cluster for prod; settings kept by measurement** | Equal to one VM today, faster from ~2× the data, the only setup that works at ×10 |
| **Two sources only: prices, and Frost temperature and wind** | Open-Meteo and precipitation from the project definition were dropped, and a trial with Elhub consumption data was archived (branch `feat/elhub-bronze`), to keep the scope manageable |

---

## Labs and notebooks

The pipeline is Python modules and command-line scripts that the workflows run; it has no notebooks of its own. The course labs (1–3) are archived in [labs/](labs/), including the notebooks `lab2_spark_fundamentals.ipynb` and `lab3_advanced_spark.ipynb`:

```bash
cd labs
python run_pipeline.py all
```

The lab workflows are kept in `labs/workflows/` for reference only. GitHub runs workflows only from `.github/workflows/`, so they don't trigger and don't occupy the self-hosted runner.
