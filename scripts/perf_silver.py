"""
Silver performance experiments (docs/plans/silver_layer.md, section 10).

How does Silver's run time grow with the data, on one VM and on the cluster? The script measures this on
a frozen copy of the data, without changing Silver's code. Settings are switched from outside, with
environment variables and Spark settings. For the bigger inputs, the measuring process also tells Silver
about the extra zones and stations.

Batches:
    setup       freeze the input: copy prod/bronze_chaos once (x1), then build x5 and x10 from it
    scale       one VM against the cluster, at x1, x5 and x10 input, with the project's own settings
    shuffle     8, 24 or 200 shuffle partitions on the cluster, at one input size (--size)
    read_tasks  how many small JSON files one read task gets (openCostInBytes), at one input size

The tuning batches test settings that grow with the data. A value that beats today's setting at x1 and
x10 becomes the project's default; rerunning scale then gives the numbers before and after.

The bigger inputs hold every zone and station folder of x1, plus 4 or 9 copies of each under new names
(zone=NO1_r1, station=SN18700_r1, ...). Silver takes the zone or station from the folder name, so the
files stay unchanged and every cleaning rule still applies. This adds zones and stations, not years.

How a batch runs: one warm-up run that is thrown away, then the variants in turn (A, B, C, A, B, C), so
a noisy period doesn't hit only one of them. Every run is a new process with its own SparkSession. Each
variant is compared with the batch's first variant on the same input. A variant that fails or passes the
time limit isn't repeated.

Everything stays under <data root>/perf/:
    input/     the frozen input: full (x1), x5 and x10
    out/       scratch output, deleted after each run
    eventlogs/ one Spark event log per batch, for the History Server
    results/   runs.csv, summary.md, input.json, plans/, stages/, logs/

Usage:
    python scripts/perf_silver.py setup
    python scripts/perf_silver.py scale --repeats 3
    python scripts/perf_silver.py shuffle --size x10 --repeats 2
    python scripts/perf_silver.py summary
"""

import argparse
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATA_ROOT  # noqa: E402

CLUSTER = "cluster"  # replaced by --cluster when a run starts
DEFAULT_CLUSTER_URL = "spark://spark:7077"
SINGLE_VM_DRIVER_MEMORY = "4g"  # what the spark VM can spare; cluster runs keep the project's settings
RUN_TIME_LIMIT_S = 30 * 60
DEFAULT_REPEATS = 3

# Input name -> its folder under input/, and how many times it holds every zone and station.
INPUT_DIRS = {"x1": "full", "x5": "x5", "x10": "x10"}
SCALES = {"x1": 1, "x5": 5, "x10": 10}
REPLICA = "_r"  # zone=NO1_r1 is the first copy of NO1
DOMAINS = ("electricity", "weather")  # Bronze's domain folders (src/silver/bronze_reader.py)
DISK_MARGIN_BYTES = 2 * 1024 ** 3  # left free for Spark's shuffle files and Silver's output
ROW_TABLES = ("prices", "weather_hourly", "zone_hourly")


@dataclass(frozen=True)
class Variant:
    """One setup of a full Silver run. A field left as None keeps the project's setting."""

    name: str
    master: str = CLUSTER
    input: str = "x1"
    driver_memory: Optional[str] = None
    shuffle_partitions: Optional[int] = None  # the project's default is DEFAULT_SHUFFLE_PARTITIONS (spark_session.py)
    open_cost_bytes: Optional[int] = None  # spark.sql.files.openCostInBytes; Spark's default is 4 MB


BATCH_NAMES = ("scale", "shuffle", "read_tasks")


def batch_variants(batch: str, size: str = "x1") -> List[Variant]:
    """A batch's variants. On each input, the first one is the reference: the project's setting today."""
    if batch == "scale":
        return [
            variant
            for scale in SCALES
            for variant in (
                Variant(f"cluster_{scale}", input=scale),
                Variant(f"one_vm_{scale}", master="local[*]", input=scale, driver_memory=SINGLE_VM_DRIVER_MEMORY),
            )
        ]
    if batch == "shuffle":
        # 8 is today's setting, 24 is twice the cluster's 12 cores, and 200 is Spark's default.
        return [Variant(f"partitions_{n}", input=size, shuffle_partitions=n) for n in (8, 24, 200)]
    if batch == "read_tasks":
        # Spark counts every file as at least openCostInBytes when it packs files into read tasks. With
        # 4 MB a task gets about 32 Bronze files of ~17 KB. With 16 KB, about one file's size, Spark packs
        # them into a few large tasks, about one per core.
        return [
            Variant("open_cost_4mb", input=size, open_cost_bytes=4 * 1024 ** 2),
            Variant("open_cost_16kb", input=size, open_cost_bytes=16 * 1024),
        ]
    raise KeyError(batch)


SQL_PLAN_EVENTS = (
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",  # adaptive execution's changes
)

PHASES = ["read_clean", "zone_hourly", "checks", "chaos_score", "scorecard", "write", "other"]
RUN_COLUMNS = (
    ["session", "batch", "variant", "input", "repeat", "is_warmup", "status", "elapsed_s"]
    + [f"{p}_s" for p in PHASES]
    + ["jobs", "stages", "tasks", "failed_tasks", "hosts", "task_s", "cpu_s", "gc_s", "input_mb", "shuffle_read_mb",
       "shuffle_remote_mb", "shuffle_write_mb", "spill_disk_mb", "slowest_task_s", "worst_skew_ratio"]
    + ["broadcast_hash_joins", "sort_merge_joins", "other_joins", "exchanges", "chaos_recall", "fingerprint"]
    + ["master", "driver_memory", "shuffle_partitions", "open_cost_bytes", "load_before", "load_after", "git_commit",
       "started_at_utc", "error"]
)


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def input(self) -> Path:
        return self.root / "input"

    def input_dir(self, name: str) -> Path:
        return self.input / INPUT_DIRS[name]

    @property
    def out(self) -> Path:
        return self.root / "out"

    @property
    def eventlogs(self) -> Path:
        return self.root / "eventlogs"

    @property
    def results(self) -> Path:
        return self.root / "results"


# ---------------------------------------------------------------------- #
# Freezing the input, and the bigger inputs
# ---------------------------------------------------------------------- #
def freeze_input(source: Path, paths: Paths) -> Dict[str, Dict[str, float]]:
    """Copy `source` once into input/full (x1), then build x10 and x5 from it.

    An existing input is never replaced, so every batch measures the same data. The biggest input gets
    real copies, so they take disk and memory as growing data would. The others link to its files.
    """
    full = paths.input_dir("x1")
    paths.input.mkdir(parents=True, exist_ok=True)
    if not full.exists():
        if not (source / "_manifests").is_dir():
            raise FileNotFoundError(f"{source} is not a chaos copy: it has no _manifests folder")
        partial = full.with_name(full.name + ".partial")
        shutil.rmtree(partial, ignore_errors=True)
        # Hidden names are the writers' temp files.
        shutil.copytree(source, partial, ignore=shutil.ignore_patterns(".*"))
        os.replace(partial, full)

    largest = max(SCALES, key=SCALES.get)
    for name in sorted(SCALES, key=SCALES.get, reverse=True):
        copies, target = SCALES[name] - 1, paths.input_dir(name)
        if copies == 0 or target.exists():
            continue
        if name == largest:
            _check_disk_space(full, copies, paths.input)
            build_scaled_input(full, target, copies)
        else:
            build_scaled_input(full, target, copies, link_copies_from=paths.input_dir(largest))

    sizes = {name: _input_size(paths.input_dir(name)) for name in SCALES if paths.input_dir(name).exists()}
    paths.results.mkdir(parents=True, exist_ok=True)
    (paths.results / "input.json").write_text(json.dumps({"source": str(source), "inputs": sizes}, indent=2), "utf-8")
    return sizes


def build_scaled_input(full: Path, target: Path, copies: int, link_copies_from: Optional[Path] = None) -> None:
    """`target`: every file of `full` (hard links) plus `copies` copies of each zone and station folder.

    The copies are real copies, or hard links to the same copies in `link_copies_from`. One extra chaos
    manifest per copy lists the same injected errors under the copy's folders, so the chaos score still
    means something.
    """
    partial = target.with_name(target.name + ".partial")
    shutil.rmtree(partial, ignore_errors=True)
    shutil.copytree(full, partial, copy_function=os.link)
    for domain in DOMAINS:
        if not (full / domain).is_dir():
            continue
        for entity in sorted(p for p in (full / domain).iterdir() if p.is_dir()):
            for k in range(1, copies + 1):
                name = f"{entity.name}{REPLICA}{k}"
                if link_copies_from is None:
                    shutil.copytree(entity, partial / domain / name)
                else:
                    shutil.copytree(link_copies_from / domain / name, partial / domain / name, copy_function=os.link)
    events = _chaos_events(full)
    for k in range(1, copies + 1):
        manifest = {"replica": k, "events": [{**e, "file": replica_path(e["file"], k)} for e in events]}
        (partial / "_manifests" / f"run__replica{REPLICA}{k}.json").write_text(json.dumps(manifest), "utf-8")
    os.replace(partial, target)


def replica_path(relative: str, k: int) -> str:
    """electricity/zone=NO1/year=... becomes electricity/zone=NO1_r2/year=... for copy 2."""
    domain, entity, rest = relative.split("/", 2)
    return f"{domain}/{entity}{REPLICA}{k}/{rest}"


def replica_suffixes(input_dir: Path) -> List[str]:
    """'' for the original zones and stations, then '_r1', '_r2', ... for the copies in a bigger input."""
    copies = {int(p.name.rsplit(REPLICA, 1)[1]) for p in (input_dir / "electricity").glob(f"zone=*{REPLICA}*")}
    return [""] + [f"{REPLICA}{k}" for k in sorted(copies)]


def _chaos_events(folder: Path) -> List[Dict]:
    """Every error the chaos injector recorded for `folder` (as src/silver/quality.load_chaos_events reads them)."""
    events: List[Dict] = []
    for path in sorted((folder / "_manifests").glob("run__*.json")):
        events.extend(json.loads(path.read_text(encoding="utf-8")).get("events", []))
    return events


def _data_files(folder: Path) -> List[Path]:
    return [p for p in folder.rglob("*.json") if "_manifests" not in p.relative_to(folder).parts]


def _input_size(folder: Path) -> Dict[str, float]:
    files = _data_files(folder)
    return {"files": len(files), "mb": round(sum(p.stat().st_size for p in files) / 1e6, 1)}


def _check_disk_space(full: Path, copies: int, where: Path) -> None:
    """Refuse to start copying when the copies wouldn't fit, with room to spare for the runs."""
    needed = copies * sum(p.stat().st_size for p in _data_files(full)) + DISK_MARGIN_BYTES
    free = shutil.disk_usage(where).free
    if free < needed:
        raise OSError(f"Not enough disk space for the bigger inputs: {free / 1e9:.1f} GB free, "
                      f"{needed / 1e9:.1f} GB needed")


# ---------------------------------------------------------------------- #
# Reading the results of a run
# ---------------------------------------------------------------------- #
def parse_event_log(path: Path) -> Dict:
    """Totals, per-stage figures and the final plan from one Spark event log (one JSON event per line)."""
    jobs = 0
    zone_hourly_plan: Optional[Dict] = None
    stage_info: Dict[int, Dict] = {}
    stage_tasks: Dict[int, List[float]] = {}
    stage_totals: Dict[int, Dict[str, float]] = {}
    hosts = set()
    totals = dict.fromkeys(
        ["tasks", "failed_tasks", "task_s", "cpu_s", "gc_s", "input_mb", "shuffle_read_mb", "shuffle_remote_mb",
         "shuffle_write_mb", "spill_disk_mb"], 0.0)

    with open(path, encoding="utf-8") as lines:
        for line in lines:
            event = json.loads(line)
            kind = event["Event"]
            if kind == "SparkListenerJobStart":
                jobs += 1
            elif kind in SQL_PLAN_EVENTS:
                # The query writing zone_hourly reaches every earlier step, through the cached tables.
                # Its last event holds the plan as adaptive execution left it.
                if _writes_zone_hourly(event["sparkPlanInfo"]):
                    zone_hourly_plan = event["sparkPlanInfo"]
            elif kind == "SparkListenerStageCompleted":
                info = event["Stage Info"]
                seconds = (info.get("Completion Time", 0) - info.get("Submission Time", 0)) / 1000
                stage_info[info["Stage ID"]] = {"name": info["Stage Name"], "seconds": seconds}
            elif kind == "SparkListenerTaskEnd":
                task, metrics = event["Task Info"], event.get("Task Metrics")
                totals["tasks"] += 1
                totals["failed_tasks"] += 1 if task.get("Failed") else 0
                if not metrics:
                    continue
                hosts.add(task["Host"])
                run_s = metrics["Executor Run Time"] / 1000
                shuffle_in, shuffle_out = metrics["Shuffle Read Metrics"], metrics["Shuffle Write Metrics"]
                remote = shuffle_in["Remote Bytes Read"] / 1e6
                read = remote + shuffle_in["Local Bytes Read"] / 1e6
                written = shuffle_out["Shuffle Bytes Written"] / 1e6
                input_mb = metrics["Input Metrics"]["Bytes Read"] / 1e6
                totals["task_s"] += run_s
                totals["cpu_s"] += metrics["Executor CPU Time"] / 1e9
                totals["gc_s"] += metrics["JVM GC Time"] / 1000
                totals["input_mb"] += input_mb
                totals["shuffle_read_mb"] += read
                totals["shuffle_remote_mb"] += remote
                totals["shuffle_write_mb"] += written
                totals["spill_disk_mb"] += metrics["Disk Bytes Spilled"] / 1e6
                stage = event["Stage ID"]
                stage_tasks.setdefault(stage, []).append(run_s)
                sums = stage_totals.setdefault(stage, {"input_mb": 0.0, "shuffle_read_mb": 0.0, "shuffle_write_mb": 0.0})
                sums["input_mb"] += input_mb
                sums["shuffle_read_mb"] += read
                sums["shuffle_write_mb"] += written

    stages = []
    for stage, times in sorted(stage_tasks.items()):
        info = stage_info.get(stage, {"name": "", "seconds": 0.0})
        median = statistics.median(times)
        stages.append({
            "stage": stage,
            "name": info["name"],
            "seconds": round(info["seconds"], 2),
            "tasks": len(times),
            "slowest_task_s": round(max(times), 2),
            "median_task_s": round(median, 2),
            **{k: round(v, 2) for k, v in stage_totals[stage].items()},
        })
    # Skew only means something where a slow task costs real time: at least 1 s, and more than one task.
    ratios = [s["slowest_task_s"] / s["median_task_s"] for s in stages
              if s["tasks"] > 1 and s["slowest_task_s"] >= 1 and s["median_task_s"] > 0]
    result = {k: round(v, 2) for k, v in totals.items()}
    result.update(
        jobs=jobs,
        stages=len(stage_info),
        hosts=len(hosts),
        tasks=int(totals["tasks"]),
        failed_tasks=int(totals["failed_tasks"]),
        slowest_task_s=max((s["slowest_task_s"] for s in stages), default=0.0),
        worst_skew_ratio=round(max(ratios), 2) if ratios else 1.0,
    )
    return {
        "totals": result,
        "stages": stages,
        "plan_counts": count_plan_operators(zone_hourly_plan),
        "plan": "\n".join(plan_lines(zone_hourly_plan)) + "\n" if zone_hourly_plan else "",
    }


def _writes_zone_hourly(node: Dict) -> bool:
    """With adaptive execution the write sits below the plan's root, without it at the root."""
    if "InsertIntoHadoopFsRelationCommand" in node["nodeName"]:
        return re.search(r"/zone_hourly\b", node["simpleString"]) is not None
    return any(_writes_zone_hourly(child) for child in node.get("children", []))


def plan_lines(node: Dict, depth: int = 0) -> Iterator[str]:
    """A plan from the event log as an indented tree."""
    yield "  " * depth + node["simpleString"][:300]
    for child in node.get("children", []):
        yield from plan_lines(child, depth + 1)


def count_plan_operators(plan: Optional[Dict]) -> Dict[str, int]:
    """Join types and shuffles in a plan from the event log."""
    counts = dict.fromkeys(["broadcast_hash_joins", "sort_merge_joins", "other_joins", "exchanges"], 0)

    def visit(node: Dict) -> None:
        name = node["nodeName"]
        if name.startswith("BroadcastHashJoin"):
            counts["broadcast_hash_joins"] += 1
        elif name.startswith("SortMergeJoin"):
            counts["sort_merge_joins"] += 1
        elif name.startswith(("BroadcastNestedLoopJoin", "CartesianProduct", "ShuffledHashJoin")):
            counts["other_joins"] += 1
        elif name == "Exchange":
            counts["exchanges"] += 1
        for child in node.get("children", []):
            visit(child)

    if plan:
        visit(plan)
    return counts


def schedule(variants: Sequence, repeats: int) -> Iterator[Tuple[object, int]]:
    """One warm-up of the first variant (round 0), then every variant in turn, `repeats` times."""
    yield variants[0], 0
    for round_number in range(1, repeats + 1):
        for variant in variants:
            yield variant, round_number


def compare(reference: Sequence[float], values: Sequence[float]) -> str:
    """A difference counts only if the two ranges don't overlap. One run has no range to compare."""
    if not reference or not values:
        return "not comparable"
    if len(reference) < 2 or len(values) < 2:
        return "too few runs"
    if max(values) < min(reference):
        return "faster"
    if min(values) > max(reference):
        return "slower"
    return "no measurable difference"


# ---------------------------------------------------------------------- #
# Inside the measuring process (one SparkSession per process)
# ---------------------------------------------------------------------- #
def tell_silver_about_copies(silver, input_dir: Path) -> None:
    """A bigger input has more zones and stations than config/zones_and_stations.json lists."""
    suffixes = replica_suffixes(input_dir)
    if len(suffixes) == 1:
        return
    from src.silver.reference import DIM_STATION_SCHEMA, station_rows

    zones = list(silver.PRICE_ZONES)
    silver.PRICE_ZONES = [zone + s for s in suffixes for zone in zones]
    rows = [(station + s, name, zone + s, *rest) for s in suffixes for station, name, zone, *rest in station_rows()]
    silver.build_dim_station = lambda spark: spark.createDataFrame(rows, DIM_STATION_SCHEMA)


def _full_run_child(spec: Dict) -> int:
    """One full Silver run, with timers around its phases. Writes spec['result_file']."""
    from pyspark.sql import functions as F

    from src.silver import run_silver as silver
    from src.silver.spark_session import get_spark

    variant = Variant(**spec["variant"])
    input_dir, output_dir = Path(spec["input_dir"]), Path(spec["output_dir"])
    tell_silver_about_copies(silver, input_dir)
    spark = get_spark(app_name=f"perf-{spec['batch']}-{variant.name}")
    if variant.open_cost_bytes is not None:
        spark.conf.set("spark.sql.files.openCostInBytes", str(variant.open_cost_bytes))

    phases = dict.fromkeys(PHASES, 0.0)
    started = time.perf_counter()

    def timed(phase: str, function: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            begin = time.perf_counter()
            try:
                return function(*args, **kwargs)
            finally:
                phases[phase] += time.perf_counter() - begin
        return wrapper

    real_checks = silver.run_checks

    def checks(*args, **kwargs):
        # Everything up to here is reading and cleaning: the first actions materialize those caches.
        phases["read_clean"] = time.perf_counter() - started
        # zone_hourly is lazy and cached, so it would otherwise count as the first check's time. This
        # count does the work that check would do, and the check then reuses it.
        begin = time.perf_counter()
        args[2].count()
        phases["zone_hourly"] = time.perf_counter() - begin
        return timed("checks", real_checks)(*args, **kwargs)

    silver.run_checks = checks
    silver.chaos_score = timed("chaos_score", silver.chaos_score)
    silver.scorecard = timed("scorecard", silver.scorecard)
    silver.write_table = timed("write", silver.write_table)
    silver.publish = timed("write", silver.publish)

    exit_code = silver.run_silver(input_dir, output_dir, "bronze_chaos", spark=spark)
    elapsed = time.perf_counter() - started
    phases["other"] = max(0.0, elapsed - sum(phases.values()))

    manifest = json.loads(max((output_dir / "_runs").glob("run__*.json")).read_text(encoding="utf-8"))
    fingerprint = ""
    if exit_code == silver.EXIT_OK:
        # The timer has stopped: this check isn't part of the measurement.
        totals = spark.read.parquet(str(output_dir / "zone_hourly")).agg(
            F.sum("price_nok_kwh").alias("price"), F.sum("temp_celsius").alias("temp")
        ).first()
        fingerprint = "|".join([
            *(f"{t}={manifest['rows'][t]}" for t in ROW_TABLES),
            f"issues={sum(i['count'] for i in manifest['issues'])}",
            f"price_sum={totals.price:.2f}",
            f"temp_sum={totals.temp:.2f}",
        ])

    Path(spec["result_file"]).write_text(json.dumps({
        "status": manifest["status"],
        "elapsed_s": round(elapsed, 2),
        "phases": {p: round(s, 2) for p, s in phases.items()},
        "chaos_recall": (manifest.get("chaos") or {}).get("recall_all"),
        "fingerprint": fingerprint,
        "app_id": spark.sparkContext.applicationId,
    }), "utf-8")
    spark.stop()
    return exit_code


# ---------------------------------------------------------------------- #
# The measuring loop (the parent process)
# ---------------------------------------------------------------------- #
def _git_commit() -> str:
    commit = os.environ.get("GITHUB_SHA")
    if commit:
        return commit[:12]
    try:
        return subprocess.run(["git", "rev-parse", "--short=12", "HEAD"], cwd=PROJECT_ROOT, capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def _child_env(master: str, variant: Variant, paths: Paths) -> Dict[str, str]:
    env = dict(os.environ)
    env.pop("GITHUB_STEP_SUMMARY", None)  # Silver would add its own summary for every run
    env.update({
        "DAT535_ENV": "dev",  # the runs pass their own folders; prod is never the default
        "SPARK_MASTER": master,
        "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON", sys.executable),
        "PYSPARK_SUBMIT_ARGS": (
            f"--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://{paths.eventlogs} pyspark-shell"
        ),
    })
    if variant.driver_memory:
        env["SPARK_DRIVER_MEMORY"] = variant.driver_memory
    if variant.shuffle_partitions:
        env["SPARK_SHUFFLE_PARTITIONS"] = str(variant.shuffle_partitions)
    return env


def _run_child(spec: Dict, env: Dict[str, str], time_limit: float, work: Path) -> Optional[int]:
    """Start this script again for one measurement. Returns its exit code, None past the time limit."""
    spec_file, log_file = work / "spec.json", work / "child.log"
    spec_file.write_text(json.dumps(spec), "utf-8")
    with open(log_file, "w", encoding="utf-8") as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "_child", str(spec_file)], env=env,
                                   cwd=PROJECT_ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return process.wait(timeout=time_limit)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)  # also the Spark JVM it started
            process.wait()
            return None


def _last_error(log_file: Path) -> str:
    """The most telling line of a failed run's log."""
    if not log_file.exists():
        return ""
    lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    wanted = [line for line in lines if re.search(r"Exception|Error|CHECK FAILED", line)]
    return (wanted[-1] if wanted else (lines[-1] if lines else "")).strip()[:300]


def _append_rows(path: Path, columns: List[str], rows: List[Dict]) -> None:
    if path.exists():
        with open(path, newline="", encoding="utf-8") as file:
            header = next(csv.reader(file), [])
        if header != columns:
            # Results of an earlier version of this script: kept beside, under their own columns.
            number = 1
            while path.with_name(f"{path.stem}.earlier-{number}{path.suffix}").exists():
                number += 1
            path.rename(path.with_name(f"{path.stem}.earlier-{number}{path.suffix}"))
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


def _read_rows(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def _keep_failure_log(paths: Paths, work: Path, name: str) -> None:
    log_file = work / "child.log"
    if log_file.exists():
        (paths.results / "logs").mkdir(parents=True, exist_ok=True)
        shutil.copy(log_file, paths.results / "logs" / f"{name}.log")


def measure_full_run(paths: Paths, batch: str, variant: Variant, repeat: int, session: str, cluster: str,
                     time_limit: float, keep_event_log: bool = False) -> Dict:
    """One full Silver run in its own process: returns its row for runs.csv, and removes its output."""
    master = cluster if variant.master == CLUSTER else variant.master
    work = paths.out / f"{batch}__{variant.name}__{repeat}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    paths.eventlogs.mkdir(parents=True, exist_ok=True)
    (paths.results / "plans").mkdir(parents=True, exist_ok=True)
    (paths.results / "stages").mkdir(parents=True, exist_ok=True)

    row: Dict = {
        **{k: v for k, v in asdict(variant).items() if k != "name"},
        "master": master, "session": session, "batch": batch, "variant": variant.name, "repeat": repeat,
        "is_warmup": repeat == 0, "git_commit": _git_commit(),
        "started_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "load_before": round(os.getloadavg()[0], 2),
    }
    spec = {
        "kind": "full", "batch": batch, "variant": asdict(variant), "input_dir": str(paths.input_dir(variant.input)),
        "output_dir": str(work / "silver"), "result_file": str(work / "result.json"),
    }
    code = _run_child(spec, _child_env(master, variant, paths), time_limit, work)
    row["load_after"] = round(os.getloadavg()[0], 2)

    if code is None:
        row.update(status="TIMEOUT", elapsed_s=round(time_limit, 2), error=f"over {time_limit / 60:g} min")
    elif code != 0 or not (work / "result.json").exists():
        row.update(status="FAILED", error=_last_error(work / "child.log"))
        _keep_failure_log(paths, work, f"{batch}__{variant.name}__{repeat}")
    else:
        result = json.loads((work / "result.json").read_text(encoding="utf-8"))
        row.update(status=result["status"], elapsed_s=result["elapsed_s"], chaos_recall=result["chaos_recall"],
                   fingerprint=result["fingerprint"], **{f"{p}_s": s for p, s in result["phases"].items()})
        log_path = paths.eventlogs / result["app_id"]
        parsed = parse_event_log(log_path)
        row.update(parsed["totals"])
        row.update(parsed["plan_counts"])
        if repeat <= 1:
            stem = f"{batch}__{variant.name}"
            (paths.results / "plans" / f"{stem}.txt").write_text(parsed["plan"], "utf-8")
            _write_stages(paths.results / "stages" / f"{stem}.csv", parsed["stages"])
        if not keep_event_log:
            log_path.unlink(missing_ok=True)
    shutil.rmtree(work, ignore_errors=True)
    return row


def _write_stages(path: Path, stages: List[Dict]) -> None:
    columns = ["stage", "name", "seconds", "tasks", "slowest_task_s", "median_task_s", "input_mb", "shuffle_read_mb",
               "shuffle_write_mb"]
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(sorted(stages, key=lambda s: -s["seconds"]))


def run_full_batch(paths: Paths, batch: str, variants: List[Variant], repeats: int, cluster: str,
                   time_limit: float) -> bool:
    session = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stopped = set()
    for variant, repeat in schedule(variants, repeats):
        if variant.name in stopped:
            continue
        label = "warm-up" if repeat == 0 else f"round {repeat}/{repeats}"
        row = measure_full_run(paths, batch, variant, repeat, session, cluster, time_limit,
                               keep_event_log=repeat == 1 and variant == variants[0])
        print(f"[{batch}] {variant.name} {label}: {row['status']} {row.get('elapsed_s', '-')} s "
              f"{row.get('error', '')}", flush=True)
        _append_rows(paths.results / "runs.csv", RUN_COLUMNS, [row])
        if row["status"] != "SUCCESS":
            if variant == variants[0]:
                print(f"[{batch}] the reference failed, so the batch stops here.", flush=True)
                return False
            stopped.add(variant.name)
    return True


# ---------------------------------------------------------------------- #
# The summary
# ---------------------------------------------------------------------- #
def _floats(rows: List[Dict], column: str) -> List[float]:
    return [float(r[column]) for r in rows if r.get(column) not in (None, "")]


def _median(rows: List[Dict], column: str) -> Optional[float]:
    values = _floats(rows, column)
    return statistics.median(values) if values else None


def _median_text(rows: List[Dict], column: str) -> str:
    value = _median(rows, column)
    return "–" if value is None else f"{value:g}"


def _time_cells(values: List[float], reference: List[float]) -> List[str]:
    """Median, range, and the change against the reference's median."""
    if not values:
        return ["–", "–", "–"]
    median = statistics.median(values)
    base = statistics.median(reference) if reference else None
    change = f"{100 * (median - base) / base:+.0f}%" if base else "–"
    return [f"{median:.1f}", f"{min(values):.1f}–{max(values):.1f}", change]


def _row_counts(fingerprint: str) -> Dict[str, int]:
    parts = dict(p.split("=", 1) for p in (fingerprint or "").split("|") if "=" in p)
    return {table: int(parts[table]) for table in ROW_TABLES if table in parts}


def _group(row: Dict) -> Tuple[str, str]:
    """A batch's results: scale covers every input, the other batches one input each."""
    return (row["batch"], "all" if row["batch"] == "scale" else row["input"])


def _latest(rows: List[Dict]) -> Dict[Tuple[str, str], List[Dict]]:
    """Rows per batch, from its newest session only (a rerun replaces the earlier one)."""
    grouped: Dict[Tuple[str, str], List[Dict]] = {}
    for row in rows:
        grouped.setdefault(_group(row), []).append(row)
    return {key: [r for r in group if r["session"] == max(x["session"] for x in group)]
            for key, group in grouped.items()}


def _same_output(variant: Variant, good: List[Dict], references: Dict[str, Variant],
                 good_by_name: Dict[str, List[Dict]]) -> str:
    """The reference's fingerprint on the same input; for a bigger input's reference, N times the rows at x1."""
    reference = references[variant.input]
    if variant != reference:
        expected = {r["fingerprint"] for r in good_by_name.get(reference.name, [])}
        if not expected:
            return "–"
        return "yes" if {r["fingerprint"] for r in good} <= expected else "**NO**"
    base = references.get("x1")
    if variant.input == "x1" or base is None or not good_by_name.get(base.name):
        return "–"
    factor = SCALES[variant.input]
    base_rows = _row_counts(good_by_name[base.name][0]["fingerprint"])
    ok = bool(base_rows) and all(
        _row_counts(r["fingerprint"]) == {t: factor * n for t, n in base_rows.items()} for r in good
    )
    return f"rows ×{factor}" if ok else "**NO**"


def build_summary(run_rows: List[Dict], input_sizes: Optional[Dict[str, Dict]] = None) -> str:
    lines = [
        "# Silver performance results",
        "",
        "Median and range of the measured runs. Each variant is compared with the batch's first variant on the "
        "same input; a difference counts only if the two ranges don't overlap. A run counts only if it ends with "
        "SUCCESS.",
    ]
    groups = _latest(run_rows)
    known = [key for key in groups if key[0] in BATCH_NAMES]
    order = sorted(known, key=lambda k: (BATCH_NAMES.index(k[0]), list(INPUT_DIRS).index(k[1]) if k[1] in INPUT_DIRS else -1))
    for batch, size in order:
        variants = batch_variants(batch) if size == "all" else batch_variants(batch, size)
        rows = [r for r in groups[(batch, size)] if r["is_warmup"] != "True"]
        good_by_name = {v.name: [r for r in rows if r["variant"] == v.name and r["status"] == "SUCCESS"]
                        for v in variants}
        references: Dict[str, Variant] = {}
        for variant in variants:
            references.setdefault(variant.input, variant)

        title = batch if size == "all" else f"{batch} at {size}"
        lines += ["", f"## {title}", "",
                  "| Variant | Input | Runs | Median (s) | Range (s) | Change | Verdict | Same output | Hosts | Tasks | "
                  "Shuffled (MB) | Spilled (MB) | GC (s) | Broadcast joins | Sort-merge joins | Other joins | Shuffles |",
                  "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for variant in variants:
            good = good_by_name[variant.name]
            if not good:
                tried = [r for r in rows if r["variant"] == variant.name]
                problem = tried[0] if tried else {"status": "not run", "error": ""}
                detail = f"{problem['status']}: {problem.get('error') or ''}".strip().rstrip(":").replace("|", "/")
                lines.append(f"| {variant.name} | {variant.input} | 0 | – | – | – | {detail} |" + " – |" * 10)
                continue
            reference = references[variant.input]
            reference_times = _floats(good_by_name[reference.name], "elapsed_s")
            times = _floats(good, "elapsed_s")
            verdict = "reference" if variant == reference else compare(reference_times, times)
            cells = [variant.name, variant.input, str(len(good)), *_time_cells(times, reference_times), verdict,
                     _same_output(variant, good, references, good_by_name)]
            cells += [_median_text(good, c) for c in ("hosts", "tasks", "shuffle_read_mb", "spill_disk_mb", "gc_s",
                                                      "broadcast_hash_joins", "sort_merge_joins", "other_joins",
                                                      "exchanges")]
            lines.append("| " + " | ".join(cells) + " |")

        if batch == "scale":
            lines += ["", "Time by input size (median seconds):", "",
                      "| Input | Data (MB) | Cluster | One VM | One VM against the cluster |", "|---|---|---|---|---|"]
            for size_name in SCALES:
                cluster = _median(good_by_name.get(f"cluster_{size_name}", []), "elapsed_s")
                one_vm = _median(good_by_name.get(f"one_vm_{size_name}", []), "elapsed_s")
                mb = (input_sizes or {}).get(size_name, {}).get("mb", "–")
                versus = f"{100 * (one_vm - cluster) / cluster:+.0f}%" if cluster and one_vm else "–"
                lines.append(f"| {size_name} | {mb} | {'–' if cluster is None else f'{cluster:.1f}'} | "
                             f"{'–' if one_vm is None else f'{one_vm:.1f}'} | {versus} |")

        lines += ["", "Where the time goes (median seconds):", "",
                  "| Variant | " + " | ".join(p.replace("_", " ") for p in PHASES) + " |",
                  "|---|" + "---|" * len(PHASES)]
        for variant in variants:
            good = good_by_name[variant.name]
            if good:
                lines.append(f"| {variant.name} | " + " | ".join(_median_text(good, f"{p}_s") for p in PHASES) + " |")
    return "\n".join(lines) + "\n"


def write_summary(paths: Paths) -> str:
    paths.results.mkdir(parents=True, exist_ok=True)
    sizes_file = paths.results / "input.json"
    sizes = json.loads(sizes_file.read_text(encoding="utf-8")).get("inputs", {}) if sizes_file.exists() else {}
    text = build_summary(_read_rows(paths.results / "runs.csv"), sizes)
    (paths.results / "summary.md").write_text(text, "utf-8")
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as file:
            file.write(text)
    return text


# ---------------------------------------------------------------------- #
# Command line
# ---------------------------------------------------------------------- #
def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "_child":
        import logging

        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        spec = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
        return _full_run_child(spec)

    parser = argparse.ArgumentParser(description="DAT535 Silver performance experiments",
                                     formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("batch", choices=["setup", *BATCH_NAMES, "summary"])
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS, help="measured runs per variant")
    parser.add_argument("--size", choices=list(INPUT_DIRS), default="x1",
                        help="input size for shuffle and read_tasks (scale runs every size)")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER_URL, help="Spark master of the cluster variants")
    parser.add_argument("--time-limit", type=float, default=RUN_TIME_LIMIT_S, help="seconds per run")
    parser.add_argument("--perf-dir", type=Path, default=DATA_ROOT / "perf", help="the experiments' folder")
    parser.add_argument("--source", type=Path, default=DATA_ROOT / "prod" / "bronze_chaos",
                        help="what `setup` freezes; it is only read")
    args = parser.parse_args()
    paths = Paths(args.perf_dir)

    if args.batch == "setup":
        print(json.dumps(freeze_input(args.source, paths), indent=2))
        return 0
    if args.batch == "summary":
        print(write_summary(paths))
        return 0

    variants = batch_variants(args.batch, args.size)
    missing = sorted({v.input for v in variants if not paths.input_dir(v.input).is_dir()})
    if missing:
        print(f"No frozen input for {', '.join(missing)} in {paths.input}: run the setup batch first.", file=sys.stderr)
        return 2
    paths.results.mkdir(parents=True, exist_ok=True)
    finished = run_full_batch(paths, args.batch, variants, args.repeats, args.cluster, args.time_limit)
    print(write_summary(paths))
    return 0 if finished else 1


if __name__ == "__main__":
    sys.exit(main())
