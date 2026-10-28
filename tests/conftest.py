"""
Shared pytest fixtures.

Spark tests are marked `spark` (registered in pyproject.toml). They need Java and pyspark and are
skipped when pyspark is missing; the VM runs `pytest -m "not spark"` before ingesting.
"""

from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import time
from typing import Callable, Dict, List, Optional
from unittest import mock

from dateutil import tz
import pytest

from src.ingestion.bronze_writer import BronzeWriter

OSLO = tz.gettz("Europe/Oslo")
FIXED_INGESTED_AT = "2026-01-01T00:00:00Z"


# ---------------------------------------------------------------------- #
# Bronze test data, shaped like the real APIs
# ---------------------------------------------------------------------- #
def price_records(day: str, exr: float = 11.2765) -> List[Dict]:
    """Hourly prices for one Norwegian day (23, 24 or 25 rows), like the Strømpris API."""
    d, nd = date.fromisoformat(day), date.fromisoformat(day) + timedelta(days=1)
    t = datetime(d.year, d.month, d.day, tzinfo=OSLO).astimezone(timezone.utc)
    end = datetime(nd.year, nd.month, nd.day, tzinfo=OSLO).astimezone(timezone.utc)
    records = []
    while t < end:
        eur = round(0.05 + 0.001 * len(records), 5)
        records.append({
            "NOK_per_kWh": round(eur * exr, 5),
            "EUR_per_kWh": eur,
            "EXR": exr,
            "time_start": t.astimezone(OSLO).isoformat(),
            "time_end": (t + timedelta(hours=1)).astimezone(OSLO).isoformat(),
        })
        t += timedelta(hours=1)
    return records


def observation(element: str, value, level: float, resolution: str = "PT1H", offset: str = "PT0H",
                quality: int = 0, series: int = 0) -> Dict:
    """One Frost observation with the fields the real API returns."""
    return {
        "elementId": element,
        "value": value,
        "unit": "degC" if element == "air_temperature" else "m/s",
        "level": {"levelType": "height_above_ground", "unit": "m", "value": level},
        "timeOffset": offset,
        "timeResolution": resolution,
        "timeSeriesId": series,
        "performanceCategory": "C",
        "exposureCategory": "1",
        "qualityCode": quality,
    }


def default_temp(h: int) -> float:
    return round(5.0 + 0.5 * h, 1)


def default_wind(h: int) -> float:
    return round(3.0 + 0.1 * h, 1)


def weather_records(
    day: str,
    station: str = "SN18700",
    hours=range(24),
    temp: Callable[[int], float] = default_temp,
    wind: Callable[[int], float] = default_wind,
) -> List[Dict]:
    """Hourly Frost records for one UTC day, like Blindern: temperature at 2 m (the series Silver
    uses) and at 10 m (ignored, quality code 2), and wind at 10 m."""
    return [
        {
            "sourceId": f"{station}:0",
            "referenceTime": f"{day}T{h:02d}:00:00.000Z",
            "observations": [
                observation("air_temperature", temp(h), 2),
                observation("air_temperature", round(temp(h) - 0.1, 1), 10, quality=2),
                observation("wind_speed", wind(h), 10),
            ],
        }
        for h in hours
    ]


class BronzeFactory:
    """Writes Bronze files with the real BronzeWriter under a fixed ingestion time, so file names
    (and the per-file chaos seeds derived from them) are the same on every run."""

    def __init__(self, root: Path):
        self.root = root
        self.writer = BronzeWriter(bronze_dir=root)

    def prices(self, zone: str, day: str, records: Optional[List[Dict]] = None,
               ingested_at: str = FIXED_INGESTED_AT) -> Path:
        records = price_records(day) if records is None else records
        with mock.patch("src.ingestion.bronze_writer.get_utc_now_iso", return_value=ingested_at):
            return self.writer.write_electricity_prices(zone, day, records, "test://prices", "batch").path

    def weather(self, station: str, day: str, records: Optional[List[Dict]] = None,
                ingested_at: str = FIXED_INGESTED_AT, is_synthetic: bool = False) -> Path:
        records = weather_records(day, station) if records is None else records
        with mock.patch("src.ingestion.bronze_writer.get_utc_now_iso", return_value=ingested_at):
            return self.writer.write_weather_observations(
                station, day, records, "test://weather", "batch", is_synthetic=is_synthetic
            ).path


@pytest.fixture
def bronze(tmp_path: Path) -> BronzeFactory:
    return BronzeFactory(tmp_path / "bronze")


# ---------------------------------------------------------------------- #
# Spark
# ---------------------------------------------------------------------- #
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
