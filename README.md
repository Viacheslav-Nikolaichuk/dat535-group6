# DAT535 Data Engineering: Regional Power Grid Economics and Microclimate Dynamics

An automated, end-to-end Data Lakehouse architecture using **PySpark**, **Delta Lake/Parquet**, and **GitHub Actions** running on a self-hosted VM runner to ingest, clean, harmonize, and model Norwegian regional electricity spot prices alongside localized meteorological observations across bidding zones `NO1`–`NO5`.

---

## Repository Structure

```text
.
├── .github/
│   └── workflows/
│       ├── ci.yml                         # Unit tests on GitHub-hosted runners (push to dev, PRs to dev/main)
│       ├── pipeline.yml                   # Medallion pipeline on the VM runner (cron 02:00 UTC on main = prod; manual on dev = dev)
│       └── backfill.yml                   # Manual historical Bronze backfill on the VM runner (main = prod, dev = dev)
├── config/
│   ├── settings.py                        # dev/prod environment, data paths, env loader, and API base settings
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
│   └── utils/
│       ├── hashing.py                     # Batch UUID4, ISO-8601 UTC, and SHA-256 payload checksums
│       └── http.py                        # Shared requests.Session with retry/backoff & User-Agent
├── scripts/
│   └── test_api_connections.py            # Diagnostic smoke-test for API connectivity & credentials
├── tests/
│   ├── test_ingestion.py                  # PyTest suite for hashing, envelopes, versioning, CLI dates, and clients
│   └── test_backfill.py                   # PyTest suite for the historical backfill (monthly runs, resume, date guard)
│   └── test_chaos.py                      # PyTest suite for chaos injection (rates, determinism, Bronze untouched)
│   └── test_settings.py                   # PyTest suite for dev/prod environment and data path resolution
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
│       ├── silver_layer.md                # Proposed Silver layer (tables Gold reads, cleaning rules, quality evidence)
│       └── use_cases.md                   # Proposed Gold use cases (dashboard, application, report, ML model)
├── dat535_data_engineering_project_definition.md # Official project specification & architecture
├── pyproject.toml                         # Pytest configuration (import path, test discovery)
├── requirements.txt                       # Project Python dependencies
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
* **Silver Layer** *(Upcoming)*: Strongly typed PySpark schema enforcement, UTC timezone harmonization, deduplication window functions, forward-fill gap imputation, Heating Degree Hours (HDH) calculation, and DLQ quarantine. Proposed design: [docs/plans/silver_layer.md](docs/plans/silver_layer.md).
* **Gold Layer** *(Upcoming)*: Serving assets for regional grid economics and price sensitivity features. Proposed use cases: [docs/plans/use_cases.md](docs/plans/use_cases.md).

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
pytest -v
```

The same suite runs automatically in `.github/workflows/ci.yml` on every push to `dev` and every pull request to `dev`/`main`. It uses GitHub-hosted runners, so the self-hosted VM runner is used only for jobs that touch real data.

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

The workflow `.github/workflows/pipeline.yml` runs automatically on the self-hosted runner. It currently has two jobs: `bronze` (ingestion) and `bronze_chaos` (extends `/bronze_chaos/` with the newly ingested files; runs only after `bronze` succeeds and is skipped on dry runs). Silver and Gold will be added as dependent jobs in the same workflow. Runs from `main` write to `prod/`, runs from `dev` to `dev/` (see [Environments](#environments)).

1. **Schedule**: Daily at `02:00 UTC` (cron `0 2 * * *`), automatically running a 3-day lookback catch-up. GitHub only runs scheduled workflows from the default branch, so the schedule is active once the workflow is on `main` (prod).
2. **Quality Gate**: Runs the unit test suite (`pytest -q`) automatically prior to executing ingestion.
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

* **History available**: Strømpris prices start on **2021-10-01**; Frost has data well before that. The project backfills from **2022-01-01** (full calendar years).
* **One run per calendar month**: each month gets its own batch ID and run manifest, so a crash or timeout loses at most the current month's manifest.
* **Resumable**: months that already have a `SUCCESS` manifest are skipped. Rerun the same range after a failure; only unfinished months are requested again, and days already in Bronze are not rewritten (checksum `unchanged`).
* **Separate from daily runs**: end dates inside the daily 3-day lookback window are refused (latest allowed end date = today − 4 days, UTC), so backfill and daily runs never write the same files. The workflow uses its own concurrency lock per branch (`historical-backfill-main`, `historical-backfill-dev`), so a long backfill never delays the scheduled daily run.
* **Runtime**: about 9 s per day with the default 0.5 s request delay, i.e. ~1 h per year (~19k files, ~210 MB of Bronze for 2022 → today).

```bash
python src/ingestion/backfill_historical.py --start-date 2022-01-01 --end-date 2022-12-31
python src/ingestion/backfill_historical.py --start-date 2023-01-01 --end-date 2023-12-31 --request-delay 0.2
```

On the VM runner, use **Actions** -> **Historical Backfill** -> **Run workflow** (`.github/workflows/backfill.yml`, manual only, 6 h timeout) with a `start_date`, `end_date`, and optional `request_delay`. Running one calendar year per run is recommended. Run it from `main` to backfill prod; from `dev` it fills `dev/` (useful for a one-month test).

---

## Lab Exercises (Labs 1, 2, 3)

The previous lab exercises and notebooks have been moved to the `labs/` directory to keep the root directory clean:

```bash
cd labs
python run_pipeline.py all
```

The lab GitHub Actions workflows are kept in `labs/workflows/` for reference only. GitHub runs workflows only from `.github/workflows/`, so they no longer trigger on push, PR, or schedule and don't occupy the self-hosted runner.
