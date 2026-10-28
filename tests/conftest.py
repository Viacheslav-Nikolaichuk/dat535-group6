"""
Shared pytest fixtures.

Spark tests are marked `spark` (registered in pyproject.toml). They need Java and pyspark and are
skipped when pyspark is missing; the VM runs `pytest -m "not spark"` before ingesting.
"""

import os
import time

import pytest


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole test run: 2 cores, 2 shuffle partitions, no UI, UTC."""
    pytest.importorskip("pyspark")
    from src.silver.spark_session import get_spark

    # PySpark converts collected timestamps with the Python process's time zone; pin it to UTC so
    # tests see the same instants Spark computes.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    session = get_spark(app_name="dat535-tests", master="local[2]", shuffle_partitions=2, ui_enabled=False)
    yield session
    session.stop()
