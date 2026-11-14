"""
Tests for the Silver performance experiments (scripts/perf_silver.py): the bigger inputs, what the numbers
are read from, how results are judged, and that a bigger input gives Silver N times the rows.
"""

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from conftest import weather_records
from config.settings import PRICE_ZONES
from scripts import perf_silver as perf
from src.ingestion.chaos_injector import run_chaos_injection

STATIONS = ["SN18700", "SN39040", "SN44560", "SN50540", "SN68860", "SN90450"]


# ---------------------------------------------------------------------- #
# Reading Spark's event log
# ---------------------------------------------------------------------- #
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


# ---------------------------------------------------------------------- #
# Batches, schedule and verdicts
# ---------------------------------------------------------------------- #
def test_the_scale_batch_compares_one_vm_with_the_cluster_on_each_input():
    variants = perf.batch_variants("scale")

    assert [(v.name, v.input) for v in variants] == [
        ("cluster_x1", "x1"), ("one_vm_x1", "x1"),
        ("cluster_x5", "x5"), ("one_vm_x5", "x5"),
        ("cluster_x10", "x10"), ("one_vm_x10", "x10"),
    ]
    cluster, one_vm = variants[0], variants[1]
    assert cluster.master == perf.CLUSTER and cluster.driver_memory is None  # the project's own settings
    assert one_vm.master == "local[*]" and one_vm.driver_memory == perf.SINGLE_VM_DRIVER_MEMORY


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


# ---------------------------------------------------------------------- #
# Setup: the frozen input and the bigger ones
# ---------------------------------------------------------------------- #
ORIGINAL = "electricity/zone=NO1/year=2025/month=01/2025_01_01.json"
COPY_3 = "electricity/zone=NO1_r3/year=2025/month=01/2025_01_01.json"


def _write_source(root: Path) -> None:
    files = {
        ORIGINAL: '{"payload": [1]}',
        "electricity/zone=NO1/year=2025/month=01/2025_01_02.json": "{}",
        "weather/station=SN18700/year=2025/month=01/2025_01_01.json": "{}",
        "_manifests/run__1.json": json.dumps({"events": [{"file": ORIGINAL, "type": "duplicate", "index": 3}]}),
        "electricity/zone=NO1/year=2025/month=01/.2025_01_03.json.tmp": "{}",  # a writer's temp file
    }
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def test_setup_freezes_the_input_and_builds_the_bigger_ones(tmp_path):
    source, paths = tmp_path / "source", perf.Paths(tmp_path / "perf")
    _write_source(source)

    sizes = perf.freeze_input(source, paths)

    full, x5, x10 = paths.input_dir("x1"), paths.input_dir("x5"), paths.input_dir("x10")
    assert {name: s["files"] for name, s in sizes.items()} == {"x1": 3, "x5": 15, "x10": 30}
    assert sorted(p.name for p in (x10 / "electricity").iterdir()) == \
        ["zone=NO1"] + [f"zone=NO1_r{k}" for k in range(1, 10)]
    assert sorted(p.name for p in (x5 / "weather").iterdir()) == \
        ["station=SN18700"] + [f"station=SN18700_r{k}" for k in range(1, 5)]
    assert perf.replica_suffixes(x5) == ["", "_r1", "_r2", "_r3", "_r4"] and perf.replica_suffixes(full) == [""]

    assert os.path.samefile(full / ORIGINAL, x10 / ORIGINAL)  # the originals are links: no extra disk
    assert not os.path.samefile(full / ORIGINAL, x10 / COPY_3)  # the copies in x10 are real copies
    assert (x10 / COPY_3).read_text(encoding="utf-8") == '{"payload": [1]}'  # with unchanged content
    assert os.path.samefile(x10 / COPY_3, x5 / COPY_3)  # x5 links to x10's copies
    assert not list(full.rglob("*.tmp"))

    # The chaos score stays meaningful: each copy has the same injected errors, under its own folders.
    manifest = json.loads((x10 / "_manifests" / "run__replica_r3.json").read_text(encoding="utf-8"))
    assert manifest["events"] == [{"file": COPY_3, "type": "duplicate", "index": 3}]

    # A second setup changes nothing, so every batch measures the same data.
    (source / "electricity/zone=NO1/year=2025/month=01/2025_01_03.json").write_text("{}", encoding="utf-8")
    assert perf.freeze_input(source, paths)["x10"]["files"] == 30
    assert json.loads((paths.results / "input.json").read_text(encoding="utf-8"))["inputs"]["x5"]["files"] == 15
    assert ORIGINAL in {p.relative_to(source).as_posix() for p in source.rglob("*.json")}  # the source is untouched


def test_setup_refuses_when_the_copies_would_not_fit(tmp_path, monkeypatch):
    source, paths = tmp_path / "source", perf.Paths(tmp_path / "perf")
    _write_source(source)
    monkeypatch.setattr(perf.shutil, "disk_usage", lambda _: SimpleNamespace(free=1024))

    with pytest.raises(OSError, match="Not enough disk space"):
        perf.freeze_input(source, paths)

    assert paths.input_dir("x1").is_dir() and not paths.input_dir("x10").exists()
    assert not list(paths.input.glob("*.partial"))


def test_setup_refuses_a_folder_that_is_not_a_chaos_copy(tmp_path):
    with pytest.raises(FileNotFoundError):
        perf.freeze_input(tmp_path / "missing", perf.Paths(tmp_path / "perf"))


def test_results_of_an_older_script_are_kept_beside(tmp_path):
    path = tmp_path / "runs.csv"
    path.write_text("session,batch,adaptive\ns0,cores,True\n", encoding="utf-8")

    perf._append_rows(path, ["session", "batch"], [{"session": "s1", "batch": "scale"}])
    perf._append_rows(path, ["session", "batch"], [{"session": "s2", "batch": "scale"}])

    assert (tmp_path / "runs.earlier-1.csv").read_text(encoding="utf-8").startswith("session,batch,adaptive")
    assert path.read_text(encoding="utf-8").splitlines() == ["session,batch", "s1,scale", "s2,scale"]
    assert not (tmp_path / "runs.earlier-2.csv").exists()


# ---------------------------------------------------------------------- #
# The summary
# ---------------------------------------------------------------------- #
def _row(variant, input_, repeat, seconds, rows=(10, 20, 10), status="SUCCESS", warmup=False, session="s1",
         batch="scale", **extra):
    prices, weather, zone = rows
    return {"session": session, "batch": batch, "variant": variant, "input": input_, "repeat": str(repeat),
            "is_warmup": str(warmup), "status": status, "elapsed_s": str(seconds),
            "fingerprint": f"prices={prices}|weather_hourly={weather}|zone_hourly={zone}|issues=1|price_sum=1.00",
            **extra}


def _line(summary: str, start: str) -> str:
    return next(line for line in summary.splitlines() if line.startswith(start))


def test_the_scale_summary_compares_on_the_same_input():
    rows = [
        _row("cluster_x1", "x1", 0, 999, warmup=True),  # the warm-up must not count
        _row("cluster_x1", "x1", 1, 60), _row("cluster_x1", "x1", 2, 62),
        _row("one_vm_x1", "x1", 1, 50), _row("one_vm_x1", "x1", 2, 52),
        _row("cluster_x5", "x5", 1, 200, rows=(50, 100, 50)), _row("cluster_x5", "x5", 2, 210, rows=(50, 100, 50)),
        _row("one_vm_x5", "x5", 1, 300, rows=(50, 100, 50)), _row("one_vm_x5", "x5", 2, 320, rows=(50, 100, 51)),
        _row("cluster_x10", "x10", 1, 400, rows=(100, 200, 99)), _row("cluster_x10", "x10", 2, 410, rows=(100, 200, 99)),
        _row("one_vm_x10", "x10", 1, 1800, status="TIMEOUT", error="over 30 min"),
    ]

    summary = perf.build_summary(rows, {"x1": {"mb": 337.6}, "x5": {"mb": 1688.0}})

    reference = _line(summary, "| cluster_x1 |")
    assert "| 61.0 |" in reference and "reference" in reference
    one_vm = _line(summary, "| one_vm_x1 |")
    assert "-16%" in one_vm and "faster" in one_vm and "| yes |" in one_vm
    # x5 is compared with the cluster at x5, not at x1; its reference has five times the rows of x1.
    assert "reference" in _line(summary, "| cluster_x5 |") and "rows ×5" in _line(summary, "| cluster_x5 |")
    assert "+51%" in _line(summary, "| one_vm_x5 |") and "slower" in _line(summary, "| one_vm_x5 |")
    assert "**NO**" in _line(summary, "| one_vm_x5 |")  # one run's output differs from the reference's
    assert "**NO**" in _line(summary, "| cluster_x10 |")  # not ten times the rows of x1
    assert "TIMEOUT: over 30 min" in _line(summary, "| one_vm_x10 |")

    assert "| x1 | 337.6 | 61.0 | 51.0 | -16% |" in summary
    assert "| x5 | 1688.0 | 205.0 | 310.0 | +51% |" in summary
    assert "| x10 | – | 405.0 | – | – |" in summary


def test_the_summary_uses_the_newest_session_and_skips_old_batches():
    rows = [
        _row("cluster_x1", "x1", 1, 500.0),
        _row("cluster_x1", "x1", 1, 10.0, session="s2"),
        _row("cluster", "x1", 1, 42.0, batch="cores"),  # a batch this version no longer has
    ]

    summary = perf.build_summary(rows)

    assert "| cluster_x1 | x1 | 1 | 10.0 " in summary and "500" not in summary
    assert "cores" not in summary


# ---------------------------------------------------------------------- #
# Real runs, each in its own process
# ---------------------------------------------------------------------- #
@pytest.mark.spark
def test_a_bigger_input_gives_n_times_the_rows(bronze, tmp_path, monkeypatch):
    pytest.importorskip("pyspark")
    for day in ["2024-01-15", "2024-01-16"]:
        for zone in PRICE_ZONES:
            bronze.prices(zone, day)
        for station in STATIONS:
            bronze.weather(station, day, weather_records(day, station))
    paths = perf.Paths(tmp_path / "perf")
    run_chaos_injection(bronze.root, paths.input_dir("x1"), rate=0.05, seed=7)
    monkeypatch.setitem(perf.INPUT_DIRS, "x2", "x2")
    perf.build_scaled_input(paths.input_dir("x1"), paths.input_dir("x2"), copies=1)

    def measure(variant):
        return perf.measure_full_run(paths, "test", variant, 1, "s1", "local[2]", 300)

    x1 = measure(perf.Variant("x1", master="local[2]"))
    x2 = measure(perf.Variant("x2", master="local[2]", input="x2", driver_memory="1g"))

    for row in (x1, x2):
        assert row["status"] == "SUCCESS", row.get("error")
        assert row["elapsed_s"] > 0 and row["jobs"] > 0 and row["tasks"] > 0 and row["hosts"] >= 1
        assert row["zone_hourly_s"] > 0
        assert row["chaos_recall"] == 1.0  # the copies' errors are found too
        assert not (paths.out / f"test__{row['variant']}__1").exists()  # the output is removed
        assert not list(paths.eventlogs.glob("local-*"))  # the event log too, unless asked to keep it
        assert (paths.results / "plans" / f"test__{row['variant']}.txt").read_text(encoding="utf-8")
        assert (paths.results / "stages" / f"test__{row['variant']}.csv").exists()

    assert x1["broadcast_hash_joins"] > 0
    assert x2["driver_memory"] == "1g"
    base, double = perf._row_counts(x1["fingerprint"]), perf._row_counts(x2["fingerprint"])
    assert base["zone_hourly"] == 5 * 48
    assert double == {table: 2 * n for table, n in base.items()}
