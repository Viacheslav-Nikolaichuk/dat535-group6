"""
Unit tests for chaos injection into a copy of Bronze data.
"""

import copy
from datetime import date, timedelta
import json
from pathlib import Path
import random

import pytest

from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.chaos_injector import (
    ERROR_BAD_TIMESTAMP,
    ERROR_CORRUPT_FILE,
    ERROR_DUPLICATE,
    ERROR_NULL_FIELD,
    ERROR_OUT_OF_RANGE,
    RECORD_ERROR_TYPES,
    inject_bronze_tree,
    inject_record_errors,
    run_chaos_injection,
)
from src.ingestion.frost_client import FrostClient
from src.utils.hashing import compute_payload_checksum


def _prices(n):
    return [{"NOK_per_kWh": 0.5 + i / 1000, "time_start": f"2024-01-15T{i % 24:02d}:00:00+01:00"} for i in range(n)]


def _weather(n_days):
    client = FrostClient(client_id="")
    records = []
    for d in range(n_days):
        res = client.fetch_observations(f"2024-01-{d % 28 + 1:02d}", "SN18700", allow_mock_fallback=True)
        records += res["records"]
    return records


@pytest.mark.parametrize("domain, payload", [("electricity", _prices(10_000)), ("weather", _weather(400))])
def test_inject_record_errors_hits_about_one_percent(domain, payload):
    out, events = inject_record_errors(payload, domain, random.Random(42), rate=0.01)
    assert 0.007 <= len(events) / len(payload) <= 0.013
    n_dupes = sum(e["type"] == ERROR_DUPLICATE for e in events)
    assert len(out) == len(payload) + n_dupes
    assert {e["type"] for e in events} == set(RECORD_ERROR_TYPES)


def test_inject_record_errors_is_deterministic_and_does_not_mutate_input():
    payload = _prices(2_000)
    original = copy.deepcopy(payload)
    a = inject_record_errors(payload, "electricity", random.Random(7))
    b = inject_record_errors(payload, "electricity", random.Random(7))
    assert a == b
    assert payload == original


def test_inject_record_errors_events_describe_the_corruption():
    out, events = inject_record_errors(_prices(5_000), "electricity", random.Random(1), rate=0.05)
    for e in events:
        rec = out[e["index"]]
        if e["type"] == ERROR_DUPLICATE:
            assert rec == out[e["index"] - 1]
        elif e["type"] == ERROR_NULL_FIELD:
            assert rec["NOK_per_kWh"] is None and e["original"] is not None
        elif e["type"] == ERROR_OUT_OF_RANGE:
            assert rec["NOK_per_kWh"] == 999.0
        elif e["type"] == ERROR_BAD_TIMESTAMP:
            assert rec["time_start"] == "not-a-timestamp"


def test_inject_record_errors_weather_targets_observation_values():
    out, events = inject_record_errors(_weather(200), "weather", random.Random(3), rate=0.05)
    value_events = [e for e in events if e["type"] in (ERROR_NULL_FIELD, ERROR_OUT_OF_RANGE)]
    assert value_events
    for e in value_events:
        i = int(e["field"].split("[")[1].split("]")[0])
        value = out[e["index"]]["observations"][i]["value"]
        assert value is None if e["type"] == ERROR_NULL_FIELD else value == 999.9


def test_inject_record_errors_rate_zero_returns_identical_copy():
    payload = _prices(100)
    out, events = inject_record_errors(payload, "electricity", random.Random(0), rate=0.0)
    assert out == payload and events == []


# ---------------------------------------------------------------------- #
# Bronze tree mirroring
# ---------------------------------------------------------------------- #
def _build_bronze(bronze_dir: Path, n_days: int) -> None:
    """Write n_days of NO1 prices and SN18700 mock weather with the real BronzeWriter."""
    writer = BronzeWriter(bronze_dir=bronze_dir)
    frost = FrostClient(client_id="")
    for i in range(n_days):
        d = (date(2024, 1, 1) + timedelta(days=i)).isoformat()
        writer.write_electricity_prices("NO1", d, _prices(24), "fake://", "b")
        weather = frost.fetch_observations(d, "SN18700", allow_mock_fallback=True)["records"]
        writer.write_weather_observations("SN18700", d, weather, "mock://", "b", is_synthetic=True)
    writer.write_manifest({"batch_id": "b"})


def _snapshot(root: Path):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_inject_bronze_tree_never_modifies_original_bronze(tmp_path: Path):
    bronze, chaos = tmp_path / "bronze", tmp_path / "bronze_chaos"
    _build_bronze(bronze, 20)
    before = _snapshot(bronze)
    inject_bronze_tree(bronze, chaos, rate=0.5)
    assert _snapshot(bronze) == before


def test_inject_bronze_tree_mirrors_data_files_only(tmp_path: Path):
    bronze, chaos = tmp_path / "bronze", tmp_path / "bronze_chaos"
    _build_bronze(bronze, 5)
    inject_bronze_tree(bronze, chaos)

    expected = {k for k in _snapshot(bronze) if not k.startswith("_manifests/")}
    assert set(_snapshot(chaos)) == expected
    assert len(expected) == 10


def test_inject_bronze_tree_corrupts_files_and_keeps_metadata(tmp_path: Path):
    bronze, chaos = tmp_path / "bronze", tmp_path / "bronze_chaos"
    _build_bronze(bronze, 20)
    events = inject_bronze_tree(bronze, chaos, rate=1.0)["events"]  # 20% of files corrupt in expectation

    corrupt = {e["file"] for e in events if e["type"] == ERROR_CORRUPT_FILE}
    assert corrupt
    for rel in _snapshot(chaos):
        text = (chaos / rel).read_text(encoding="utf-8")
        if rel in corrupt:
            with pytest.raises(json.JSONDecodeError):
                json.loads(text)
            continue
        envelope = json.loads(text)
        original = json.loads((bronze / rel).read_text(encoding="utf-8"))
        assert envelope["_metadata"] == original["_metadata"]
        changed = envelope["payload"] != original["payload"]
        assert changed == (envelope["_metadata"]["_payload_checksum"] != compute_payload_checksum(envelope["payload"]))
        assert changed == any(e["file"] == rel for e in events)


def test_inject_bronze_tree_total_damage_is_about_rate(tmp_path: Path):
    """Record errors + records lost in corrupt files together stay ~1% of all records."""
    bronze, chaos = tmp_path / "bronze", tmp_path / "bronze_chaos"
    _build_bronze(bronze, 730)  # two years of history
    events = inject_bronze_tree(bronze, chaos, rate=0.01)["events"]

    total = 730 * 24 * 2
    damaged = sum(e.get("records", 1) for e in events)
    assert 0.006 <= damaged / total <= 0.014
    assert {e["type"] for e in events} == set(RECORD_ERROR_TYPES) | {ERROR_CORRUPT_FILE}


def test_inject_bronze_tree_is_deterministic_per_file(tmp_path: Path):
    """A file's corruption must not depend on which other files exist (daily vs backfill)."""
    full, partial = tmp_path / "full", tmp_path / "partial"
    _build_bronze(full, 30)
    keep = sorted(p for p in full.rglob("*.json") if "_manifests" not in p.parts)[:4]
    for p in keep:
        dst = partial / p.relative_to(full)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(p.read_bytes())

    inject_bronze_tree(full, tmp_path / "chaos_full", rate=0.5, seed=1)
    inject_bronze_tree(partial, tmp_path / "chaos_partial", rate=0.5, seed=1)
    for p in keep:
        rel = p.relative_to(full)
        assert (tmp_path / "chaos_full" / rel).read_bytes() == (tmp_path / "chaos_partial" / rel).read_bytes()


# ---------------------------------------------------------------------- #
# Ground-truth manifest & reruns
# ---------------------------------------------------------------------- #
def test_run_chaos_injection_writes_ground_truth_manifest(tmp_path: Path):
    bronze, chaos = tmp_path / "bronze", tmp_path / "bronze_chaos"
    _build_bronze(bronze, 20)
    mpath = run_chaos_injection(bronze, chaos, rate=0.3, seed=5)

    assert mpath.parent == chaos / "_manifests" and mpath.name.startswith("run__")
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    assert manifest["rate"] == 0.3 and manifest["seed"] == 5
    assert manifest["files_processed"] == 40 and manifest["files_skipped"] == 0
    assert sum(manifest["counts"].values()) == len(manifest["events"]) > 0
    for e in manifest["events"]:
        assert (chaos / e["file"]).is_file()


def test_run_chaos_injection_rerun_only_processes_new_bronze_files(tmp_path: Path):
    bronze, chaos = tmp_path / "bronze", tmp_path / "bronze_chaos"
    _build_bronze(bronze, 10)
    run_chaos_injection(bronze, chaos, rate=0.5)
    data_before = {k: v for k, v in _snapshot(chaos).items() if not k.startswith("_manifests/")}

    # Rerun with nothing new: no data file touched, an empty manifest recorded.
    second = json.loads(run_chaos_injection(bronze, chaos, rate=0.5).read_text(encoding="utf-8"))
    assert second["files_processed"] == 0 and second["files_skipped"] == 20 and second["events"] == []
    assert {k: v for k, v in _snapshot(chaos).items() if not k.startswith("_manifests/")} == data_before

    # A newly ingested day is picked up on the next run.
    BronzeWriter(bronze_dir=bronze).write_electricity_prices("NO1", "2024-02-01", _prices(24), "fake://", "b")
    third = json.loads(run_chaos_injection(bronze, chaos, rate=0.5).read_text(encoding="utf-8"))
    assert third["files_processed"] == 1 and third["files_skipped"] == 20
    assert len(list((chaos / "_manifests").glob("run__*.json"))) == 3


def test_run_chaos_injection_refuses_output_inside_bronze(tmp_path: Path):
    bronze = tmp_path / "bronze"
    _build_bronze(bronze, 1)
    before = _snapshot(bronze)
    for bad in (bronze, bronze / "chaos"):
        with pytest.raises(ValueError):
            run_chaos_injection(bronze, bad)
    assert _snapshot(bronze) == before
