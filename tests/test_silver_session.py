"""
Unit tests for the shared SparkSession settings (src/silver/spark_session.py).
"""

import pytest

pytestmark = pytest.mark.spark


def test_session_runs_in_utc_with_dynamic_partition_overwrite(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"


def test_spark_version_matches_the_vm(spark):
    # requirements.txt pins pyspark to the VM's Spark; an accidental upgrade fails here first.
    assert spark.version == "3.5.0"
