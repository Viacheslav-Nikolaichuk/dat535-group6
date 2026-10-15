# DAT535 Data Engineering: Regional Power Grid Economics and Microclimate Dynamics

An automated, end-to-end Data Lakehouse architecture using **PySpark**, **Delta Lake/Parquet**, and **GitHub Actions** running on a self-hosted VM runner to ingest, clean, harmonize, and model Norwegian regional electricity spot prices alongside localized meteorological observations across bidding zones `NO1`–`NO5`.

---

## Repository Structure

```text
.
├── .github/
│   └── workflows/
│       ├── daily_bronze_ingestion.yml     # Automated daily batch ingestion on runner (cron 02:00 UTC)
│       ├── dat535-dev.yml                 # Dev branch CI/CD pipeline for lab exercises
│       ├── dat535-prod.yml                # Main branch production deployment for lab exercises
│       └── pipeline.yml                   # Lab scheduled & manual dispatch workflow
├── config/
│   ├── settings.py                        # Path resolutions, env loader, and API base settings
│   └── zones_and_stations.json            # Mapping of bidding zones NO1-NO5 to MET Norway weather stations
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── strompris_client.py            # Hva Koster Strømmen API extractor
│   │   ├── frost_client.py                # MET Norway Frost API extractor (auth & error handling)
│   │   ├── bronze_writer.py               # Metadata audit envelopes & versioned, checksum-deduplicated JSON storage
│   │   └── ingest_daily.py                # Ingestion entrypoint CLI (daily, lookback catch-up, backfill)
│   └── utils/
│       ├── hashing.py                     # Batch UUID4, ISO-8601 UTC, and SHA-256 payload checksums
│       └── http.py                        # Shared requests.Session with retry/backoff & User-Agent
├── scripts/
│   └── test_api_connections.py            # Diagnostic smoke-test for API connectivity & credentials
├── tests/
│   └── test_ingestion.py                  # PyTest suite for hashing, envelopes, versioning, CLI dates, and clients
├── labs/                                  # Archived course laboratory materials (Lab 1, 2, 3)
│   ├── lab1_1_setup_guide.md
│   ├── lab1_install-spark.sh
│   ├── lab2_pipeline.py
│   ├── lab2_spark_fundamentals.ipynb
│   ├── lab3_advanced_spark.ipynb
│   ├── lab3_pipeline.py
│   ├── run_pipeline.py
│   └── project_cicd_guide.md
├── docs/                                  # Project guides & documentation
│   └── Data_Engineering_Project_Definition_Guide_v3.pdf
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
* **Silver Layer** *(Upcoming)*: Strongly typed PySpark schema enforcement, UTC timezone harmonization, deduplication window functions, forward-fill gap imputation, Heating Degree Hours (HDH) calculation, and DLQ quarantine.
* **Gold Layer** *(Upcoming)*: Serving assets for regional grid economics and price sensitivity features.

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
DAT535_DATA_DIR=./data
```

*(Note: `.env` is listed in `.gitignore` and is never committed to git).*

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
pytest tests/test_ingestion.py -v
```

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
```

### Running on the VM Runner via GitHub Actions

The workflow `.github/workflows/daily_bronze_ingestion.yml` runs automatically on the self-hosted runner:
1. **Schedule**: Daily at `02:00 UTC` (cron `0 2 * * *`), automatically running a 3-day lookback catch-up.
2. **Quality Gate**: Runs the unit test suite (`pytest -q`) automatically prior to executing ingestion.
3. **Concurrency & Safety**: Protected by GitHub Actions concurrency locks (`bronze-ingestion`) and strict regex parameter sanitization to prevent race conditions or shell injection.
4. **Manual Dispatch**: Go to **Actions** -> **Daily Bronze Ingestion Pipeline** -> **Run workflow**:
   - Provide a `target_date` for a single day rerun.
   - Or provide `start_date` and `end_date` for bulk historical backfills.
   - Or adjust `lookback_days` (defaults to 3).
   - Optional `dry_run` checkbox.

Make sure `FROST_CLIENT_ID` is saved in your GitHub Repository under **Settings** > **Secrets and variables** > **Actions** > **New repository secret**.

---

## Lab Exercises (Labs 1, 2, 3)

The previous lab exercises and notebooks have been moved to the `labs/` directory to keep the root directory clean:

```bash
cd labs
python run_pipeline.py all
```
