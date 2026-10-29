"""
Reads Bronze (or its chaos copy) into Spark for the Silver layer.

Every Bronze file becomes one row with its envelope, lineage and a status:
  * Explicit schemas: Spark never guesses types. A new API field is ignored; a removed field
    turns into nulls that the Silver checks catch.
  * Truncated or otherwise unreadable files land in `_corrupt_record` (PERMISSIVE mode) instead
    of failing the job. (`badRecordsPath` from the project definition exists only on Databricks.)
  * Versions (docs/plans/silver_layer.md, D6): per zone or station and day, the newest readable,
    non-empty file is `used`. Older readable versions are `superseded`: their records are still
    checked, so their problems reach the issue log (D7), but they never reach the Silver tables.

File statuses: used, superseded, corrupt, empty, synthetic (mock data, left out unless allowed).
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src.silver.quality import ACTION_EXCLUDED, ACTION_REJECTED, RAW_VALUE_MAX_CHARS

DOMAIN_PRICES = "electricity"
DOMAIN_WEATHER = "weather"

# Bronze folder key per domain (bronze/electricity/zone=NO1/..., bronze/weather/station=SN18700/...)
_ENTITY_KEY = {DOMAIN_PRICES: "zone", DOMAIN_WEATHER: "station"}

STATUS_USED = "used"
STATUS_SUPERSEDED = "superseded"
STATUS_CORRUPT = "corrupt"
STATUS_EMPTY = "empty"
STATUS_SYNTHETIC = "synthetic"

_METADATA = StructType([
    StructField("_batch_id", StringType()),
    StructField("_ingestion_timestamp_utc", StringType()),
    StructField("_source_uri", StringType()),
    StructField("_payload_checksum", StringType()),
    StructField("_domain", StringType()),
    StructField("_partition_keys", MapType(StringType(), StringType())),
    StructField("_record_count", LongType()),
    StructField("_is_synthetic", BooleanType()),  # missing in schema v1.0 files -> null -> False
    StructField("_schema_version", StringType()),
])

PRICE_RECORD = StructType([
    StructField("NOK_per_kWh", DoubleType()),
    StructField("EUR_per_kWh", DoubleType()),
    StructField("EXR", DoubleType()),
    StructField("time_start", StringType()),
    StructField("time_end", StringType()),
])

OBSERVATION = StructType([
    StructField("elementId", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("level", StructType([
        StructField("levelType", StringType()),
        StructField("unit", StringType()),
        StructField("value", DoubleType()),
    ])),
    StructField("timeOffset", StringType()),
    StructField("timeResolution", StringType()),
    StructField("timeSeriesId", LongType()),
    StructField("performanceCategory", StringType()),
    StructField("exposureCategory", StringType()),
    StructField("qualityCode", LongType()),
])

WEATHER_RECORD = StructType([
    StructField("sourceId", StringType()),
    StructField("referenceTime", StringType()),
    StructField("observations", ArrayType(OBSERVATION)),
])


def envelope_schema(record: StructType) -> StructType:
    """Bronze envelope: audit metadata, the raw payload, and a column for unreadable files."""
    return StructType([
        StructField("_metadata", _METADATA),
        StructField("payload", ArrayType(record)),
        StructField("_corrupt_record", StringType()),
    ])


ENVELOPE_SCHEMAS = {DOMAIN_PRICES: envelope_schema(PRICE_RECORD), DOMAIN_WEATHER: envelope_schema(WEATHER_RECORD)}


def _read_envelopes(spark: SparkSession, domain_dir: Path, schema: StructType) -> DataFrame:
    """Raw envelopes plus the file path; an empty frame if the domain has no files yet."""
    if next(domain_dir.rglob("*.json"), None) is None:
        return spark.createDataFrame([], schema).withColumn("_input_file", F.lit(None).cast("string"))
    return (
        spark.read.schema(schema)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(domain_dir))
        # Spark's own `_metadata` file column is hidden by our envelope field of the same name.
        .withColumn("_input_file", F.expr("url_decode(input_file_name())"))
    )


def read_bronze_files(
    spark: SparkSession,
    input_dir: Path,
    domain: str,
    allow_synthetic: bool = False,
) -> DataFrame:
    """
    One row per Bronze file of `domain` under `input_dir`, cached.

    Columns: entity (zone or station), file_date, source_file (relative to `input_dir`, the same
    form the chaos manifests use), _batch_id, _ingested_at_utc, payload, corrupt_text, status,
    is_used.
    """
    key = _ENTITY_KEY[domain]
    raw = _read_envelopes(spark, Path(input_dir) / domain, ENVELOPE_SCHEMAS[domain])

    source_file = F.regexp_extract("_input_file", rf"({domain}/[^/]+/year=\d{{4}}/month=\d{{2}}/[^/]+\.json)$", 1)
    files = raw.select(
        source_file.alias("source_file"),
        F.col("_metadata._batch_id").alias("_batch_id"),
        F.try_to_timestamp(F.col("_metadata._ingestion_timestamp_utc"), F.lit("yyyy-MM-dd'T'HH:mm:ssX"))
        .alias("_ingested_at_utc"),
        "payload",
        F.substring("_corrupt_record", 1, RAW_VALUE_MAX_CHARS).alias("corrupt_text"),
        (F.col("_corrupt_record").isNotNull() | F.col("_metadata").isNull() | F.col("payload").isNull())
        .alias("is_corrupt"),
        F.coalesce(F.col("_metadata._is_synthetic"), F.lit(False)).alias("is_synthetic"),
    ).select(
        "*",
        F.regexp_extract("source_file", rf"^{domain}/{key}=([^/]+)/", 1).alias("entity"),
        # The day comes from the file name, so it is known even for truncated files.
        F.try_to_timestamp(
            F.regexp_extract("source_file", r"/(\d{4}_\d{2}_\d{2})(?:__[^/]*)?\.json$", 1), F.lit("yyyy_MM_dd")
        ).cast("date").alias("file_date"),
    )

    excluded_synthetic = F.col("is_synthetic") & F.lit(not allow_synthetic)
    is_empty = ~F.col("is_corrupt") & (F.size("payload") == 0)
    readable = ~F.col("is_corrupt") & ~excluded_synthetic & ~is_empty
    newest_first = Window.partitionBy("entity", "file_date").orderBy(
        readable.desc(), F.col("_ingested_at_utc").desc_nulls_last(), F.col("source_file").desc()
    )
    is_used = readable & (F.row_number().over(newest_first) == 1)

    status = (
        F.when(F.col("is_corrupt"), STATUS_CORRUPT)
        .when(excluded_synthetic, STATUS_SYNTHETIC)
        .when(is_empty, STATUS_EMPTY)
        .when(is_used, STATUS_USED)
        .otherwise(STATUS_SUPERSEDED)
    )
    return (
        files.withColumn("status", status)
        .withColumn("is_used", F.col("status") == STATUS_USED)
        .drop("is_corrupt", "is_synthetic")
        # Cached: Spark refuses queries that need only `_corrupt_record` from the raw files, and
        # every later step (issues, records, metrics) reads these rows again.
        .cache()
    )


def file_issues(files: DataFrame, source: str) -> DataFrame:
    """Issue-log rows for files that were not used as a whole: corrupt, empty or synthetic."""
    status = F.col("status")
    return files.filter(status.isin(STATUS_CORRUPT, STATUS_EMPTY, STATUS_SYNTHETIC)).select(
        F.lit(source).alias("source"),
        F.when(status == STATUS_CORRUPT, "corrupt_file")
        .when(status == STATUS_EMPTY, "empty_file")
        .otherwise("synthetic_file")
        .alias("issue"),
        F.when(status == STATUS_CORRUPT, ACTION_REJECTED).otherwise(ACTION_EXCLUDED).alias("action"),
        "source_file",
        F.lit(None).cast("int").alias("record_index"),
        F.lit(None).cast("string").alias("field"),
        F.col("corrupt_text").alias("raw_value"),
    )


def explode_records(files: DataFrame) -> DataFrame:
    """One row per record of every checked file (used and superseded versions), with its position."""
    return files.filter(F.col("status").isin(STATUS_USED, STATUS_SUPERSEDED)).select(
        "entity",
        "file_date",
        "source_file",
        "_batch_id",
        "_ingested_at_utc",
        "is_used",
        F.posexplode("payload").alias("record_index", "rec"),
    )
