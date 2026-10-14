"""
Unit tests for Bronze Layer Ingestion, Metadata Envelopes, and API Clients.
"""

import json
from pathlib import Path
import pytest

from config.settings import PRICE_ZONES
from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.frost_client import FrostClient
from src.ingestion.ingest_daily import load_stations_config
from src.ingestion.strompris_client import StromprisClient
from src.utils.hashing import (
    compute_payload_checksum,
    generate_batch_id,
    get_utc_now_iso,
    parse_date_components,
)


def test_batch_id_and_hashing():
    """Verify UUID generation and SHA-256 payload checksum calculation."""
    b1 = generate_batch_id()
    b2 = generate_batch_id()
    assert b1 != b2
    assert len(b1) == 36

    sample_payload = [{"price": 1.25, "hour": 0}, {"price": 1.30, "hour": 1}]
    hash1 = compute_payload_checksum(sample_payload)
    hash2 = compute_payload_checksum(sample_payload)
    assert hash1 == hash2
    assert len(hash1) == 64  # SHA-256 hex digest length


def test_date_components_parsing():
    """Verify valid and invalid date parsing."""
    year, month, day = parse_date_components("2024-01-15")
    assert (year, month, day) == ("2024", "01", "15")

    with pytest.raises(ValueError):
        parse_date_components("invalid-date-format")


def test_strompris_client_url_construction():
    """Verify StromprisClient endpoint format and zone validation."""
    client = StromprisClient()

    for zone in PRICE_ZONES:
        url = client.build_url("2024-01-15", zone)
        assert f"2024/01-15_{zone}.json" in url

    with pytest.raises(ValueError):
        client.fetch_prices("2024-01-15", "NO99")


def test_frost_client_mock_generation():
    """Verify mock observation generation for offline testing."""
    client = FrostClient(client_id="")
    mock_res = client.fetch_observations(
        date_str="2024-01-15",
        station_id="SN18700",
        elements=["air_temperature", "wind_speed"],
        allow_mock_fallback=True,
    )

    assert mock_res["station_id"] == "SN18700"
    assert mock_res["record_count"] == 24
    assert len(mock_res["records"]) == 24
    first_obs = mock_res["records"][0]["observations"]
    element_names = [o["elementId"] for o in first_obs]
    assert "air_temperature" in element_names
    assert "wind_speed" in element_names


def test_bronze_writer_envelope_and_persistence(tmp_path: Path):
    """Verify writing and reading Bronze envelope with partition hierarchy."""
    writer = BronzeWriter(bronze_dir=tmp_path)
    sample_payload = [
        {"NOK_per_kWh": 0.85, "time_start": "2024-01-15T00:00:00+01:00"},
        {"NOK_per_kWh": 0.90, "time_start": "2024-01-15T01:00:00+01:00"},
    ]

    # Write electricity prices
    written_path = writer.write_electricity_prices(
        zone="NO1",
        date_str="2024-01-15",
        payload=sample_payload,
        source_uri="https://mock.api/prices",
        batch_id="test-batch-001",
    )

    assert written_path.exists()
    assert "electricity" in str(written_path)
    assert "zone=NO1" in str(written_path)
    assert "year=2024" in str(written_path)
    assert "month=01" in str(written_path)

    # Read back and inspect envelope
    envelope = writer.read_bronze_file(written_path)
    meta = envelope["_metadata"]
    assert meta["_batch_id"] == "test-batch-001"
    assert meta["_domain"] == "electricity"
    assert meta["_partition_keys"]["zone"] == "NO1"
    assert meta["_record_count"] == 2
    assert meta["_payload_checksum"] == compute_payload_checksum(sample_payload)
    assert envelope["payload"] == sample_payload


def test_load_stations_config():
    """Verify that all bidding zones NO1-NO5 have mapped weather stations."""
    mapping = load_stations_config()
    for zone in PRICE_ZONES:
        assert zone in mapping
        assert len(mapping[zone]) > 0
