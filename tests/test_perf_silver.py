"""
Tests for the Silver performance experiments (scripts/perf_silver.py): what the numbers are read from,
how results are judged, and that switching a setting leaves Silver's output the same.
"""

import json
from pathlib import Path

import pytest

from conftest import weather_records
from config.settings import PRICE_ZONES
from scripts import perf_silver as perf
from src.ingestion.chaos_injector import run_chaos_injection

STATIONS = ["SN18700", "SN39040", "SN44560", "SN50540", "SN68860", "SN90450"]


def _task_end(stage: int, run_ms: int, host: str = "worker-1", read: int = 0, remote: int = 0, spilled: int = 0,
              failed: bool = False) -> dict:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage,
        "Task Info": {"Host": host, "Failed": failed},
        "Task Metrics": {
            "Executor Run Time": run_ms,
            "Executor CPU Time": run_ms * 500_000,  # half of the run time, in nanoseconds
            "JVM GC Time": 10,
            "Disk Bytes Spilled": spilled,
            "Input Metrics": {"Bytes Read": 2_000_000},
            "Shuffle Read Metrics": {"Remote Bytes Read": remote, "Local Bytes Read": read},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 1_000_000},
        },
    }


def _stage_done(stage: int, seconds: float) -> dict:
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {"Stage ID": stage, "Stage Name": f"stage {stage}", "Submission Time": 1000,
                       "Completion Time": 1000 + int(seconds * 1000)},
    }


def test_event_log_gives_totals_and_the_worst_skew(tmp_path):
    events = [
        {"Event": "SparkListenerJobStart"},
        {"Event": "SparkListenerTaskStart"},  # kinds it doesn't need are ignored
        _task_end(1, 1000, host="worker-1", read=1_000_000, remote=3_000_000),
        _task_end(1, 1000, host="worker-2", spilled=5_000_000),
        _task_end(1, 8000, host="worker-2"),  # one task eight times as slow as the median
        _task_end(2, 100),
        _task_end(2, 200),  # a stage this quick can't count as skewed, whatever the ratio
        {"Event": "SparkListenerTaskEnd", "Stage ID": 2, "Task Info": {"Host": "worker-3", "Failed": True}},
        _stage_done(1, 9.0),
        _stage_done(2, 0.5),
    ]
    log = tmp_path / "app-1"
    log.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")

    parsed = perf.parse_event_log(log)
    totals = parsed["totals"]

    assert totals["jobs"] == 1 and totals["stages"] == 2
    assert totals["tasks"] == 6 and totals["failed_tasks"] == 1
    assert totals["hosts"] == 2  # a task that died before reporting metrics doesn't count
    assert totals["task_s"] == pytest.approx(10.3)
    assert totals["cpu_s"] == pytest.approx(5.15)
    assert totals["shuffle_read_mb"] == 4.0 and totals["shuffle_remote_mb"] == 3.0
    assert totals["shuffle_write_mb"] == 5.0 and totals["spill_disk_mb"] == 5.0
    assert totals["slowest_task_s"] == 8.0 and totals["worst_skew_ratio"] == 8.0
    assert parsed["stages"][0] == {
        "stage": 1, "name": "stage 1", "seconds": 9.0, "tasks": 3, "slowest_task_s": 8.0, "median_task_s": 1.0,
        "input_mb": 6.0, "shuffle_read_mb": 4.0, "shuffle_write_mb": 3.0,
    }


def _node(name: str, *children, text: str = "") -> dict:
    return {"nodeName": name, "simpleString": text or name, "children": list(children)}


def _write_plan(path: str, *children) -> dict:
    return _node("Execute InsertIntoHadoopFsRelationCommand", *children,
                 text=f"Execute InsertIntoHadoopFsRelationCommand file:{path}, false, Parquet")


def test_the_plan_is_the_final_one_of_the_query_writing_zone_hourly(tmp_path):
    prefix = "org.apache.spark.sql.execution.ui."
    planned = _write_plan("/x/_staging/1/zone_hourly", _node("SortMergeJoin"))  # the write at the root
    final = _node(
        "AdaptiveSparkPlan",
        _write_plan("/x/_staging/1/zone_hourly",
                    _node("BroadcastHashJoin", _node("BroadcastExchange"), _node("Exchange")),
                    _node("SortMergeJoin(skew=true)"), _node("CartesianProduct")),
        text="AdaptiveSparkPlan isFinalPlan=true",  # the write below the root, as adaptive execution has it
    )
    events = [
        {"Event": prefix + "SparkListenerSQLExecutionStart", "sparkPlanInfo": planned},
        {"Event": prefix + "SparkListenerSQLAdaptiveExecutionUpdate", "sparkPlanInfo": final},
        # Other queries don't count: writing another table, and only reading zone_hourly.
        {"Event": prefix + "SparkListenerSQLExecutionStart",
         "sparkPlanInfo": _write_plan("/x/_staging/1/zone_hourly_copy", _node("SortMergeJoin"))},
        {"Event": prefix + "SparkListenerSQLExecutionStart",
         "sparkPlanInfo": _node("Scan parquet", text="Scan parquet file:/x/silver/zone_hourly")},
    ]
    log = tmp_path / "app-1"
    log.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")

    parsed = perf.parse_event_log(log)

    assert parsed["plan_counts"] == {"broadcast_hash_joins": 1, "sort_merge_joins": 1, "other_joins": 1, "exchanges": 1}
    assert parsed["plan"].startswith("AdaptiveSparkPlan isFinalPlan=true\n  Execute InsertIntoHadoopFsRelationCommand")
    assert "\n    BroadcastHashJoin\n      BroadcastExchange\n" in parsed["plan"]


def test_a_log_without_that_query_has_no_plan(tmp_path):
    log = tmp_path / "app-1"
    log.write_text(json.dumps({"Event": "SparkListenerJobStart"}), encoding="utf-8")

    parsed = perf.parse_event_log(log)

    assert parsed["plan"] == "" and set(parsed["plan_counts"].values()) == {0}


def test_schedule_warms_up_once_then_takes_turns():
    assert list(perf.schedule(["A", "B"], 2)) == [("A", 0), ("A", 1), ("B", 1), ("A", 2), ("B", 2)]


@pytest.mark.parametrize("values, verdict", [
    ([7, 8, 9], "faster"),
    ([13, 14, 15], "slower"),
    ([9, 11, 13], "no measurable difference"),  # overlapping ranges, though the medians differ
    ([5], "too few runs"),  # one run has no range
    ([], "not comparable"),
])
def test_a_difference_counts_only_when_the_ranges_dont_overlap(values, verdict):
    assert perf.compare([10, 11, 12], values) == verdict


def test_every_batch_starts_with_the_settings_the_workflows_use():
    for batch, variants in perf.FULL_BATCHES.items():
        reference = variants[0]
        assert (reference.master, reference.input, reference.shuffle_partitions) == (perf.CLUSTER, "full", 8), batch
        assert reference.adaptive and reference.cache == "on" and reference.broadcast == "hint", batch
        assert len({v.name for v in variants}) == len(variants), batch


def _write_source(root: Path) -> None:
    for relative in [
        "electricity/zone=NO1/year=2024/month=12/2024_12_31.json",
        "electricity/zone=NO1/year=2025/month=01/2025_01_01.json",
        "weather/station=SN18700/year=2025/month=06/2025_06_01.json",
        "_manifests/run__1.json",
        "electricity/zone=NO1/year=2025/month=01/.2025_01_02.json.tmp",
    ]:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")


def _files(root: Path):
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


def test_setup_freezes_the_input_once_and_links_the_subsets(tmp_path):
    source, paths = tmp_path / "source", perf.Paths(tmp_path / "perf")
    _write_source(source)

    sizes = perf.freeze_input(source, paths)

    assert sizes["full"]["files"] == 3 and sizes["1y"]["files"] == 2 and sizes["1m"]["files"] == 1
    assert _files(paths.input / "1m") == [
        "_manifests/run__1.json", "electricity/zone=NO1/year=2025/month=01/2025_01_01.json",
    ]
    assert ".2025_01_02.json.tmp" not in " ".join(_files(paths.input / "full"))  # a writer's temp file

    # A second setup keeps the frozen copy: new source files must not change later measurements.
    (source / "electricity/zone=NO1/year=2025/month=01/2025_01_03.json").write_text("{}", encoding="utf-8")
    assert perf.freeze_input(source, paths)["full"]["files"] == 3

    # Setup never changes the source.
    assert "_manifests/run__1.json" in _files(source)


def test_setup_refuses_a_folder_that_is_not_a_chaos_copy(tmp_path):
    with pytest.raises(FileNotFoundError):
        perf.freeze_input(tmp_path / "missing", perf.Paths(tmp_path / "perf"))


def _run_row(variant, repeat, seconds, batch="cache", status="SUCCESS", fingerprint="same", warmup=False, **extra):
    return {"session": "s1", "batch": batch, "variant": variant, "repeat": repeat, "is_warmup": str(warmup),
            "status": status, "elapsed_s": seconds, "fingerprint": fingerprint, "input": "full", **extra}


def test_summary_judges_each_variant_against_the_reference():
    rows = [
        _run_row("all_on", 0, 999.0, warmup=True),  # the warm-up must not count
        _run_row("all_on", 1, 10.0),
        _run_row("all_on", 2, 11.0),
        _run_row("bronze_only", 1, 20.0),
        _run_row("bronze_only", 2, 21.0, fingerprint="different"),
        _run_row("all_off", 1, 1200.0, status="TIMEOUT", error="over 20 min"),
    ]

    lines = perf.build_summary(rows, []).splitlines()

    reference = next(line for line in lines if line.startswith("| all_on | 2 "))
    assert "10.5" in reference and "10.0–11.0" in reference and "reference" in reference
    slower = next(line for line in lines if line.startswith("| bronze_only | 2 "))
    assert "+95%" in slower and "slower" in slower and "**NO**" in slower  # a different output is flagged
    assert any(line.startswith("| all_off | 0 ") and "TIMEOUT: over 20 min" in line for line in lines)


def test_summary_uses_the_newest_session_of_a_batch():
    old = _run_row("all_on", 1, 500.0)
    new = {**_run_row("all_on", 1, 10.0), "session": "s2"}

    summary = perf.build_summary([old, new], [])

    assert "| all_on | 1 | 10.0 " in summary and "500" not in summary


def test_summary_of_the_read_tests():
    explicit = {"session": "s1", "test": "schema", "variant": "explicit", "status": "SUCCESS", "seconds": "2.0",
                "inference_s": "0", "files_read": "10", "tasks_per_query": "4", "input_mb_per_query": "1"}
    inferred = {"session": "s1", "test": "schema", "variant": "inferred", "status": "SUCCESS", "seconds": "6.0",
                "inference_s": "3", "files_read": "10", "tasks_per_query": "8", "input_mb_per_query": "2"}

    summary = perf.build_summary([], [explicit, explicit, inferred, inferred])

    assert "| schema | explicit | 2.0 " in summary
    assert "| schema | inferred | 6.0 | 6.0–6.0 | +200% | slower | 10 | 8 | 2 | 3 |" in summary


# ---------------------------------------------------------------------- #
# Real runs, each in its own process
# ---------------------------------------------------------------------- #
@pytest.mark.spark
def test_switching_settings_changes_the_plan_but_not_the_output(bronze, tmp_path):
    pytest.importorskip("pyspark")
    for day in ["2024-01-15", "2024-01-16"]:
        for zone in PRICE_ZONES:
            bronze.prices(zone, day)
        for station in STATIONS:
            bronze.weather(station, day, weather_records(day, station))
    paths = perf.Paths(tmp_path / "perf")
    run_chaos_injection(bronze.root, paths.input / "full", rate=0.05, seed=7)

    def measure(variant):
        return perf.measure_full_run(paths, "test", variant, 1, "s1", "local[2]", 300)

    usual = measure(perf.Variant("usual", master="local[2]", shuffle_partitions=2))
    switched = measure(perf.Variant("switched", master="local[2]", shuffle_partitions=2, adaptive=False,
                                    cache="bronze_only", broadcast="off"))

    for row in (usual, switched):
        assert row["status"] == "SUCCESS", row.get("error")
        assert row["elapsed_s"] > 0 and row["jobs"] > 0 and row["tasks"] > 0 and row["hosts"] >= 1
        assert row["chaos_recall"] == 1.0
        assert not (paths.out / f"test__{row['variant']}__1").exists()  # the output is removed
        assert not list(paths.eventlogs.glob("local-*"))  # the event log too, unless asked to keep it
        assert (paths.results / "plans" / f"test__{row['variant']}.txt").read_text(encoding="utf-8")
        assert (paths.results / "stages" / f"test__{row['variant']}.csv").exists()

    assert usual["broadcast_hash_joins"] > 0
    assert switched["broadcast_hash_joins"] == 0 and switched["sort_merge_joins"] > usual["sort_merge_joins"]
    assert usual["zone_hourly_s"] > 0 and switched["zone_hourly_s"] == 0  # timed apart only with the cache on
    assert usual["fingerprint"] and usual["fingerprint"] == switched["fingerprint"]
