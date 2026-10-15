"""
Unit tests for Bronze Layer Ingestion, Metadata Envelopes, and API Clients.
"""

from datetime import date
import json
from pathlib import Path
import pytest

from config.settings import PRICE_ZONES
from src.ingestion import ingest_daily
from src.ingestion.bronze_writer import (
    STATUS_CREATED,
    STATUS_NEW_VERSION,
    STATUS_UNCHANGED,
    BronzeWriter,
)
from src.ingestion.frost_client import FrostClient
from src.ingestion.ingest_daily import load_stations_config, resolve_target_dates, run_ingestion
from src.ingestion.strompris_client import StromprisClient
from src.utils.hashing import (
    compute_payload_checksum,
    generate_batch_id,
    get_utc_now_iso,
    parse_date_components,
)
from src.utils.http import RETRY_STATUS_CODES, build_session

SAMPLE_PRICES = [
    {"NOK_per_kWh": 0.85, "time_start": "2024-01-15T00:00:00+01:00"},
    {"NOK_per_kWh": 0.90, "time_start": "2024-01-15T01:00:00+01:00"},
]


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
    assert mock_res["is_synthetic"] is True
    first_obs = mock_res["records"][0]["observations"]
    element_names = [o["elementId"] for o in first_obs]
    assert "air_temperature" in element_names
    assert "wind_speed" in element_names


def test_frost_client_without_credentials_refuses_by_default():
    """Without a client ID and without explicit opt-in, Frost must raise rather than mock."""
    client = FrostClient(client_id="")
    with pytest.raises(ValueError):
        client.fetch_observations(date_str="2024-01-15", station_id="SN18700")


class _FakeResponse:
    status_code = 200
    url = "https://frost.met.no/observations/v0.jsonld?fake"

    def raise_for_status(self):
        pass

    def json(self):
        return {"data": [{"sourceId": "SN18700:0", "observations": []}]}


class _CapturingSession:
    def __init__(self):
        self.params = None

    def get(self, url, params=None, **kwargs):
        self.params = params
        return _FakeResponse()


def test_frost_client_requests_hourly_series_only_by_default():
    """Without a resolution filter Frost also returns 1-min/10-min series (~100x larger payloads)."""
    session = _CapturingSession()
    client = FrostClient(client_id="abc", session=session)
    res = client.fetch_observations(date_str="2024-01-15", station_id="SN18700")

    assert session.params["timeresolutions"] == "PT1H"
    assert session.params["referencetime"] == "2024-01-15/2024-01-16"
    assert res["record_count"] == 1


def test_bronze_writer_envelope_and_persistence(tmp_path: Path):
    """Verify writing and reading Bronze envelope with partition hierarchy."""
    writer = BronzeWriter(bronze_dir=tmp_path)

    result = writer.write_electricity_prices(
        zone="NO1",
        date_str="2024-01-15",
        payload=SAMPLE_PRICES,
        source_uri="https://mock.api/prices",
        batch_id="test-batch-001",
    )
    written_path = result.path

    assert result.status == STATUS_CREATED
    assert written_path.exists()
    assert "electricity" in str(written_path)
    assert "zone=NO1" in str(written_path)
    assert "year=2024" in str(written_path)
    assert "month=01" in str(written_path)
    assert written_path.name.startswith("2024_01_15__")

    # Read back and inspect envelope
    envelope = writer.read_bronze_file(written_path)
    meta = envelope["_metadata"]
    assert meta["_batch_id"] == "test-batch-001"
    assert meta["_domain"] == "electricity"
    assert meta["_partition_keys"]["zone"] == "NO1"
    assert meta["_record_count"] == 2
    assert meta["_payload_checksum"] == compute_payload_checksum(SAMPLE_PRICES)
    assert meta["_is_synthetic"] is False
    assert envelope["payload"] == SAMPLE_PRICES


def test_bronze_writer_is_idempotent_for_identical_payload(tmp_path: Path):
    """Re-ingesting an identical payload must not create a second file."""
    writer = BronzeWriter(bronze_dir=tmp_path)
    kwargs = dict(zone="NO1", date_str="2024-01-15", payload=SAMPLE_PRICES, source_uri="u")

    first = writer.write_electricity_prices(batch_id="batch-a", **kwargs)
    second = writer.write_electricity_prices(batch_id="batch-b", **kwargs)

    assert first.status == STATUS_CREATED
    assert second.status == STATUS_UNCHANGED
    assert second.path == first.path
    assert not second.written
    assert len(writer.list_versions("electricity", "NO1", "2024-01-15")) == 1


def test_bronze_writer_keeps_history_for_changed_payload(tmp_path: Path):
    """A revised payload must be written as a new version, never overwriting the old one."""
    writer = BronzeWriter(bronze_dir=tmp_path)
    revised = [dict(SAMPLE_PRICES[0], NOK_per_kWh=0.99), SAMPLE_PRICES[1]]

    first = writer.write_weather_observations("SN18700", "2024-01-15", SAMPLE_PRICES, "u", "batch-a")
    second = writer.write_weather_observations("SN18700", "2024-01-15", revised, "u", "batch-a")

    assert first.status == STATUS_CREATED
    assert second.status == STATUS_NEW_VERSION
    assert first.path != second.path
    assert first.path.exists() and second.path.exists()

    versions = writer.list_versions("weather", "SN18700", "2024-01-15")
    assert versions == [first.path, second.path]
    assert writer.read_bronze_file(versions[-1])["payload"] == revised


def test_bronze_writer_recognises_legacy_files(tmp_path: Path):
    """Pre-versioning files (YYYY_MM_DD.json) count as an existing version."""
    legacy_dir = tmp_path / "electricity" / "zone=NO1" / "year=2024" / "month=01"
    legacy_dir.mkdir(parents=True)
    legacy = {
        "_metadata": {
            "_ingestion_timestamp_utc": "2026-10-14T17:12:53Z",
            "_payload_checksum": compute_payload_checksum(SAMPLE_PRICES),
            "_schema_version": "v1.0",
        },
        "payload": SAMPLE_PRICES,
    }
    (legacy_dir / "2024_01_15.json").write_text(json.dumps(legacy), encoding="utf-8")

    writer = BronzeWriter(bronze_dir=tmp_path)
    result = writer.write_electricity_prices("NO1", "2024-01-15", SAMPLE_PRICES, "u", "batch-x")

    assert result.status == STATUS_UNCHANGED
    assert result.path.name == "2024_01_15.json"


def test_bronze_writer_flags_synthetic_payloads(tmp_path: Path):
    writer = BronzeWriter(bronze_dir=tmp_path)
    result = writer.write_weather_observations(
        "SN18700", "2024-01-15", SAMPLE_PRICES, "mock://", "b", is_synthetic=True
    )
    assert writer.read_bronze_file(result.path)["_metadata"]["_is_synthetic"] is True


def test_load_stations_config():
    """Verify that all bidding zones NO1-NO5 have mapped weather stations."""
    mapping = load_stations_config()
    for zone in PRICE_ZONES:
        assert zone in mapping
        assert len(mapping[zone]) > 0


# ---------------------------------------------------------------------- #
# Date resolution
# ---------------------------------------------------------------------- #
TODAY = date(2026, 10, 15)


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({}, ["2026-10-14"]),
        ({"target_date": "2024-01-15"}, ["2024-01-15"]),
        ({"lookback_days": 3}, ["2026-10-12", "2026-10-13", "2026-10-14"]),
        (
            {"start_date": "2024-02-27", "end_date": "2024-03-01"},
            ["2024-02-27", "2024-02-28", "2024-02-29", "2024-03-01"],
        ),
        ({"start_date": "2026-10-13"}, ["2026-10-13", "2026-10-14"]),
    ],
)
def test_resolve_target_dates(kwargs, expected):
    assert resolve_target_dates(today=TODAY, **kwargs) == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {"target_date": "2026-10-15"},  # today is not a complete day
        {"start_date": "2024-01-10", "end_date": "2024-01-01"},  # reversed range
        {"end_date": "2024-01-01"},  # end without start
        {"target_date": "2024-01-01", "lookback_days": 2},  # conflicting modes
        {"lookback_days": 0},
    ],
)
def test_resolve_target_dates_rejects_invalid(kwargs):
    with pytest.raises(ValueError):
        resolve_target_dates(today=TODAY, **kwargs)


# ---------------------------------------------------------------------- #
# Pipeline orchestration (no network)
# ---------------------------------------------------------------------- #
class _FakeStrom:
    def __init__(self):
        self.calls = 0

    def fetch_prices(self, date_str, zone):
        self.calls += 1
        return {"records": SAMPLE_PRICES, "source_uri": f"fake://{zone}/{date_str}"}


class _NoCredsFrost(FrostClient):
    def __init__(self):
        super().__init__(client_id="")


def test_run_ingestion_refuses_without_frost_credentials(tmp_path: Path):
    """Missing credentials must abort before any API call or write (no silent mock data)."""
    strom = _FakeStrom()
    rc = run_ingestion(
        dates=["2024-01-15"], output_dir=tmp_path, strom_client=strom, frost_client=_NoCredsFrost()
    )
    assert rc == ingest_daily.EXIT_CONFIG_ERROR
    assert strom.calls == 0
    assert not any(tmp_path.rglob("*.json"))


def test_run_ingestion_mock_opt_in_writes_flagged_files_and_is_rerunnable(tmp_path: Path):
    kwargs = dict(
        dates=["2024-01-15", "2024-01-16"],
        zones=["NO1"],
        output_dir=tmp_path,
        allow_mock_weather=True,
        strom_client=_FakeStrom(),
        frost_client=_NoCredsFrost(),
    )
    assert run_ingestion(**kwargs) == ingest_daily.EXIT_OK

    weather_files = sorted((tmp_path / "weather").rglob("*.json"))
    assert len(weather_files) == 2  # NO1 -> SN18700, two days
    assert all(BronzeWriter.read_bronze_file(f)["_metadata"]["_is_synthetic"] for f in weather_files)

    # Second run over the same dates writes nothing new to domain partitions (idempotent).
    data_files_before = len(list((tmp_path / "weather").rglob("*.json"))) + len(list((tmp_path / "electricity").rglob("*.json")))
    assert run_ingestion(**kwargs) == ingest_daily.EXIT_OK
    data_files_after = len(list((tmp_path / "weather").rglob("*.json"))) + len(list((tmp_path / "electricity").rglob("*.json")))
    assert data_files_after == data_files_before
    # But a second run manifest is recorded in _manifests
    assert len(list((tmp_path / "_manifests").glob("run__*.json"))) == 2


# ---------------------------------------------------------------------- #
# HTTP session
# ---------------------------------------------------------------------- #
def test_build_session_has_retry_and_user_agent():
    session = build_session(total_retries=3)
    retry = session.get_adapter("https://frost.met.no").max_retries
    assert retry.total == 3
    assert set(RETRY_STATUS_CODES) <= set(retry.status_forcelist)
    assert isinstance(StromprisClient().session.get_adapter("https://x").max_retries.total, int)


# ---------------------------------------------------------------------- #
# Manifest persistence
# ---------------------------------------------------------------------- #
def test_bronze_writer_and_ingestion_manifest(tmp_path: Path):
    writer = BronzeWriter(bronze_dir=tmp_path)
    manifest = {
        "batch_id": "batch-manifest-1234",
        "date_from": "2024-01-15",
        "date_to": "2024-01-15",
        "electricity_records": 24,
        "weather_records": 144,
    }
    mpath = writer.write_manifest(manifest)
    assert mpath.exists()
    assert "_manifests" in str(mpath)
    assert "batch-manifest-1234" in mpath.name

    with open(mpath, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["electricity_records"] == 24

    # Test run_ingestion persists manifest
    kwargs = dict(
        dates=["2024-01-15"],
        zones=["NO1"],
        output_dir=tmp_path,
        allow_mock_weather=True,
        strom_client=_FakeStrom(),
        frost_client=_NoCredsFrost(),
    )
    assert run_ingestion(**kwargs) == ingest_daily.EXIT_OK
    manifests = list((tmp_path / "_manifests").glob("run__*.json"))
    assert len(manifests) >= 1

