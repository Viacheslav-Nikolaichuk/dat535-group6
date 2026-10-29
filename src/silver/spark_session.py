"""
SparkSession factory shared by the Silver (and later Gold) jobs.

One place for the settings every job relies on:
  * local mode (`local[*]` on the VM); driver memory from SPARK_DRIVER_MEMORY;
  * all timestamps in UTC (`spark.sql.session.timeZone`). Norwegian local time is derived
    explicitly where needed, so daylight saving is handled in one place;
  * dynamic partition overwrite, so rewriting some months never deletes the others;
  * adaptive query execution and few shuffle partitions: the data is small (~0.5M hourly rows),
    and Spark's default of 200 partitions would mostly schedule empty tasks.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession

DEFAULT_SHUFFLE_PARTITIONS = 8


def get_spark(
    app_name: str = "dat535-silver",
    master: Optional[str] = None,
    shuffle_partitions: Optional[int] = None,
    ui_enabled: bool = True,
) -> SparkSession:
    """Create (or reuse) the project's SparkSession."""
    partitions = shuffle_partitions or int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", DEFAULT_SHUFFLE_PARTITIONS))
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master or os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.ui.enabled", str(ui_enabled).lower())
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
