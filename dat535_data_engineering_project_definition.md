# DAT535 Data Engineering Project Definition
## Regional Power Grid Economics and Microclimate Dynamics

---

## 1. Project Statement

* **What will the project build?**
* **What problem or analytical need will it address?**
* **What is the intended final outcome?**
* **Who or what will use the result?**
* **What is intentionally outside the project scope?**

**Overview:**
This project builds an automated, end-to-end Data Lakehouse architecture using PySpark, Delta Lake, and GitHub Actions to ingest, clean, harmonize, and model regional electricity spot prices alongside localized meteorological observations across Norwegian price zones (`NO1`–`NO5`).

The platform addresses the analytical challenge of non-linear correlation between localized heating/cooling degree days (HDD/CDD) and regional electricity price spikes caused by climate variations and localized grid congestion.

The final outcome is a set of Gold-layer analytics assets, including aggregated heating degree day metrics, spot price volatility indexes, and price sensitivity features designed for downstream machine learning and cost forecasting dashboards.

**Intentionally out of scope:** Real-time streaming ingestion (batch ingestion is sufficient).

---

## 2. Dataset Source and Suitability

* **What is the source, format, volume, and refresh pattern?**
* **Why is the dataset suitable for distributed processing?**
* **Which fields and reference datasets are relevant?**
* **What limitations, quality risks, or schema changes are expected?**
* **How will access, licensing, and reproducibility be handled?**

### Strømpris API (Hva Koster Strømmen)
Provides hourly electricity spot prices per pricing zone in JSON format. Updated daily for current and next-day prices.

### Frost API (MET Norway) & Open-Meteo Historical Archive
Provides hourly surface temperature, wind speed, and precipitation observations across weather stations in JSON/JSON-LD formats.

### Suitability for Distributed Processing
The combined historical time-series dataset spans several years across multiple regions and measurement stations, yielding millions of fine-grained hourly records ideal for distributed PySpark transformations, window functions, and partitioning.

### Licensing & Reproducibility
All sources use free, open APIs with NLOD/CC-BY licensing. Ingestion modules use deterministic request parameterization to ensure full historical reproducibility.

---

## 3. Bronze Layer: Ingest and Preserve

* **Which source data must be preserved unchanged?**
* **Which ingestion method and schedule fit the source?**
* **How will files, partitions, and folders be organized?**
* **Which metadata will support traceability and reruns?**
* **How will duplicate ingestion, late data, and schema changes be handled?**

### Raw Ingestion & Immutability
Raw payloads are written as immutable Delta tables / Parquet files without structural alteration.

### Partitioning & Organization
Partitioned by target domain, regional zone, year, and month (`/bronze/electricity/zone=NO1/year=2026/month=10/`).

### Metadata Wrapper Audit Fields
* `_ingestion_timestamp_utc`: Execution system timestamp.
* `_batch_id`: Unique UUID generated per run.
* `_source_uri`: Exact API endpoint requested.
* `_payload_checksum`: SHA-256 payload hash to guarantee tamper evidence and idempotency.

---

## 4. Silver Layer: Primary Focus

* **Data understanding:** What does one record represent? Which fields are essential, optional, derived, or unreliable?
* **Data model:** What is the grain? What keys and relationships are needed? How will history and reference data be represented?
* **Cleaning:** How will missing, duplicate, invalid, inconsistent, or out-of-range values be treated?
* **Standardization:** How will names, types, timestamps, units, and schemas be made consistent?
* **Enrichment:** Which joins, reference data, or derived attributes are needed?
* **Quality evidence:** Which rules and metrics will prove improvement before and after processing?
* **Implementation:** Which transformations should use basic Spark transformations and actions, and where are higher-level APIs appropriate?
* **Performance:** Where are scans, joins, shuffles, skew, or partitioning likely to matter? Which tuning is justified by evidence?
* **Lifecycle:** How will Silver logic be tested, reviewed, versioned, deployed, rerun, and monitored?
* **Scalability:** How would the design respond to larger volumes, late data, or schema evolution?

### Data Understanding
A price record represents the market clearing electricity price for one megawatt/kilowatt-hour in a specific bidding zone during an hourly window. Essential: `time_start`, `NOK_per_kWh`, `bidding_zone`. A weather record represents an empirical observation at an official meteorological station. Essential: `referenceTime`, `elementId`, `value`. Unreliable: unvalidated sensor readings lacking quality verification codes.

### Data Model
The core fact grain is strictly one record per bidding zone per UTC hour (`bidding_zone`, `utc_hour`). Natural composite primary key: `[bidding_zone, utc_hour]`. Multiple weather stations within a zone are aggregated into a single regional observation via a reference station mapping table. History is modeled as immutable, partition-overwritten hourly time-series.

### Cleaning & DLQ Triage
* **Deduplication:** Window function `row_number().over(Window.partitionBy("bidding_zone", "utc_hour").orderBy(desc("_ingestion_timestamp_utc"))) == 1` removes duplicate timestamps.
* **Outlier Boundaries:** Filters impossible domain outliers (temperatures outside $-50.0^\circ\text{C}$ to $+45.0^\circ\text{C}$; power prices outside $-15.00\text{ NOK}$ to $+20.00\text{ NOK}$ per $\text{kWh}$).
* **Dead-Letter Queue (DLQ):** Records with corrupt JSON structures, unparseable timestamps, or fatal schema errors are routed to `silver/_rejected_records/` with explicit `rejection_reason` tags rather than silently dropped.

### Standardization
Standardize divergent timestamp representations (Norwegian local time offsets `+01:00`/`+02:00` vs. UTC) to unified `TimestampType` in UTC using `to_utc_timestamp()` and `date_trunc('hour')`. Enforce consistent `snake_case` column names with explicit measurement units (`price_nok_kwh`, `temp_celsius`, `wind_speed_mps`).

### Enrichment
* **Spatial Aggregation:** Group station-level observations by `[bidding_zone, utc_hour]` and compute the zone-wide arithmetic mean: `avg(temp_celsius)`, `avg(wind_speed_mps)`.
* **Feature Engineering:** Compute Heating Degree Hours (HDH) based on the Norwegian residential baseline of $17.0^\circ\text{C}$: `when(17.0 - avg_temp > 0, 17.0 - avg_temp).otherwise(0.0)`.
* **Relational Join:** Inner equi-join between conformed hourly prices and zonal weather on composite key `[bidding_zone, utc_hour]`.

### Quality Evidence
Provide a quantitative before-and-after scorecard measuring: total Bronze rows ingested, exact count and percentage of duplicate rows eliminated, count of chaos-injected records isolated into DLQ, null percentage drop across critical columns, and post-cleansing uniqueness validation ($0$ duplicate primary keys).

### Spark Implementation
Implemented in PySpark using high-level DataFrame and Spark SQL APIs. Ingestion parsing utilizes explicit `StructType` schemas with `badRecordsPath` configuration. Custom Python User-Defined Functions (UDFs) are avoided to preserve Catalyst query optimization.

### Performance & Tuning
Broadcast Hash Join (`broadcast()`) applied to the dimension lookup table (`station_zone_mapping`), eliminating unnecessary shuffle stages across worker nodes. Silver Parquet tables are partitioned by `year` and `bidding_zone`, enabling efficient partition pruning during analytical range queries. Physical execution plans (`df.explain()`) demonstrate partition skipping and shuffle-free joins.

### Lifecycle & CI/CD
Tested using `pytest-spark` with parameterized synthetic test fixtures. Automated CI checks in GitHub Actions validate unit tests, schema contracts, and code formatting on pull requests. Runs are monitored via structured execution logs.

### Scalability
The pipeline uses dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode = "dynamic"`), guaranteeing idempotent batch execution whether backfilling four years of historical archives or processing a single daily increment.

---

### Silver Layer Implementation Plan (Summary)

| Category | Raw Issue / Requirement | Silver Transformation Logic | Verification Metric |
| :--- | :--- | :--- | :--- |
| **Data Model & Grain** | Mismatch between weather station coordinates and electricity price regions (`NO1`–`NO5`). | Map stations to price zones via geospatial point-in-polygon lookup. | $100\%$ of Silver records assigned valid `price_zone_id`. |
| **Standardization** | Inconsistent timezones (CET/CEST daylight saving vs UTC weather logs). | Convert all timestamps to ISO-8601 UTC format using `to_utc_timestamp()`. | Zero non-UTC or unparseable timestamps remaining. |
| **Cleaning** | Duplicate price entries from retried API queries; sporadic missing temperatures. | Deduplicate on `(timestamp_utc, zone_id)`; apply forward-fill for gaps $< 3\text{h}$. | Duplicate rate $= 0$; null temperature rate $< 0.1\%$. |
| **Enrichment** | Raw temperature lacks direct correlation with heating demand. | Compute hourly Heating Degree Hours (HDH) base $17^\circ\text{C}$: `max(0, 17 - temp)`. | HDH column present and non-negative across all rows. |

---

## 5. Gold Layer: Serve the Outcome

* **Analytical Focus & Consumer Needs**
* **Dimensional Model & Aggregations**
* **Usefulness & Downstream Serving**

### Analytical Outcome
Supports energy consumption forecasting, regional grid congestion analysis, and consumer pricing analytics.

### Gold Assets
1. `gold_daily_grid_economics`: Aggregated daily metrics per price zone including average spot price, price volatility, total Heating Degree Days (HDD), and peak/off-peak price ratios.
2. `gold_price_sensitivity_features`: Rolling 7-day and 30-day cross-correlation features between temperature drops and spot price spikes for ML model feature stores.

---

## 6. End-to-End Delivery: Operate the Lifecycle

* **Project Structure & Reproducibility**
* **CI/CD & Automation**
* **Testing & Monitoring**

### CI/CD & Workflows
GitHub Actions automates PyTest execution on pull requests, testing API retries, schema validation, and Spark transformations against mock fixtures. Merged code deploys scheduled pipelines executing Bronze to Gold refreshes.

### Environment Separation
Strict separation between `dev` (local/mocked data runs) and `prod` environment configs managed via environment variables.

### Observability
Pipeline logs track batch completion status, rejected records count, and data quality check results.

---

## 7. Dedicated Silver Layer Implementation Plan

| Pipeline Phase | Technical Objective | PySpark Implementation Pattern | Concrete Example / Data State |
| :--- | :--- | :--- | :--- |
| **01. Bronze Schema Enforcement** | Parse raw JSON payloads from Bronze into strongly typed PySpark DataFrames with explicit schemas. | `from_json(col("raw_payload"), schema)` with predefined `StructType` definitions. | Converts raw JSON string into structured columns: `NOK_per_kWh`, `time_start`, `station_id`. |
| **02. Timezone Standardization** | Harmonize heterogeneous time zone offsets (CET/CEST for prices vs. UTC for weather) into standard ISO-8601 UTC timestamps. | `to_utc_timestamp(col("time_start"), col("source_tz"))` or explicit timestamp casting. | `"2026-10-09T10:00:00+02:00"` converted to `"2026-10-09T08:00:00Z"` (`timestamp_utc`). |
| **03. Data Deduplication** | Eliminate duplicate records caused by overlapping API batch extraction or retry mechanisms. | `windowSpec = Window.partitionBy("timestamp_utc", "region_id").orderBy(col("_ingestion_timestamp").desc())` followed by `.filter(col("row_num") == 1)`. | Retains only the latest ingested record for duplicate `(timestamp_utc, NO1)` combinations. |
| **04. Geospatial & Regional Alignment** | Map weather station coordinates (lat/lon) to corresponding Norwegian electricity price zones (`NO1`–`NO5`). | Broadcast join with static price zone polygon reference dataset using geospatial point-in-polygon mapping. | Station `SN18700` (Blindern, lat $59.94$, lon $10.72$) mapped to price zone `NO1`. |
| **05. Missing Value Imputation** | Handle missing weather observations due to sensor downtime without dropping historical price records. | Forward-fill window function: `last("air_temperature", ignorenulls=True).over(window_3h)` for gaps $< 3\text{ hours}$. | A 1-hour gap in temperature data is imputed using the prior hour's observed value. |
| **06. Domain Feature Derivation** | Compute domain-specific metrics connecting weather dynamics to energy economics. | `when(col("air_temperature") < 17, 17 - col("air_temperature")).otherwise(0)` for Heating Degree Hours (HDH). | An observed temperature of $5^\circ\text{C}$ yields an `hdh` value of `12.0` degree-hours. |
| **07. Outlier Detection & Filtering** | Detect and flag extreme sensor errors or malformed API responses. | `withColumn("is_valid_temp", col("air_temperature").between(-50, 45))` and routing corrupted rows to quarantine. | An erroneous reading of $999.9^\circ\text{C}$ is routed to the Silver quarantine table. |
| **08. Table Joining & Harmonization** | Join spot price time series with aggregated zonal microclimate metrics on unified grain. | Inner join: `prices_df.join(zonal_weather_df, ["timestamp_utc", "price_zone_id"], "inner")`. | Single row containing `timestamp_utc`, `price_zone_id`, `spot_price_nok`, `avg_zonal_temp`, `hdh`. |
| **09. Data Quality Verification** | Execute automated contract validation checks prior to writing to the Silver layer. | Great Expectations / PySpark assertion suite: verify null thresholds, non-negative prices, and key uniqueness. | Pipeline halts or flags warning if `null_count(timestamp_utc) > 0` or duplicate rate $> 0\%$. |
| **10. Silver Layer Persistence** | Persist cleaned and harmonized data into optimized Delta Lake format with partition pruning support. | `df.write.format("delta").mode("append").partitionBy("price_zone_id", "year", "month").save("/silver/grid_economics/")`. | Clean Delta table saved under `/silver/grid_economics/price_zone_id=NO1/year=2026/month=10/`. |