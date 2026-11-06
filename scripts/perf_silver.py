"""
Silver performance experiments (docs/plans/silver_layer.md, section 10).

Measures the performance decisions in the Silver code on a frozen copy of the data, without changing
that code. Settings are switched from outside: environment variables and Spark settings, plus two
small replacements made in the measuring process only (`DataFrame.cache` and `functions.broadcast`).

Batches:
    setup      freeze the input: copy prod/bronze_chaos once (run this first)
    cores      the cluster against one VM with all cores, 2 cores and 1 core
    cache      all caches on, only the Bronze file cache on, all off
    shuffle    8 or 200 shuffle partitions, each with adaptive execution on and off
    broadcast  hints on, hints off with Spark choosing, broadcast off
    size       5 years, 1 year and 1 month of input
    read       read-only tests: partitioning, schema, small files

How a batch runs: one warm-up run that is thrown away, then the variants in turn (A, B, C, A, B, C), so
a noisy period doesn't hit only one of them. Every run is a new process with its own SparkSession. The
first variant of a batch is its reference. A variant that fails or passes the time limit isn't repeated.

Everything stays under <data root>/perf/:
    input/     the frozen input (full, 1y, 1m)
    out/       scratch output, deleted after each run
    eventlogs/ one Spark event log per batch, for the History Server
    results/   runs.csv, read_runs.csv, summary.md, plans/, stages/, logs/

Usage:
    python scripts/perf_silver.py setup
    python scripts/perf_silver.py cache --repeats 3
    python scripts/perf_silver.py summary
"""

import argparse
import contextlib
import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import io
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
EXECUTOR_MEMORY = "3g"
RUN_TIME_LIMIT_S = 20 * 60
DEFAULT_REPEATS = 3

# Input subsets, as (year, month) folders. 2025 is the newest complete year when this was written.
SUBSETS = {"1y": ("2025", None), "1m": ("2025", "01")}
READ_MONTH = (2025, 1)  # the month the partitioning test asks for


@dataclass(frozen=True)
class Variant:
    """One setup of a full Silver run. The defaults are the settings the workflows use."""

    name: str
    master: str = CLUSTER
    input: str = "full"
    shuffle_partitions: int = 8
    adaptive: bool = True
    cache: str = "on"  # on | bronze_only | off
    broadcast: str = "hint"  # hint | auto | off


FULL_BATCHES: Dict[str, List[Variant]] = {
    "cores": [
        Variant("cluster"),
        Variant("one_vm_all_cores", master="local[*]"),
        Variant("one_vm_2_cores", master="local[2]"),
        Variant("one_vm_1_core", master="local[1]"),
    ],
    "cache": [
        Variant("all_on"),
        Variant("bronze_only", cache="bronze_only"),
        Variant("all_off", cache="off"),
    ],
    "shuffle": [
        Variant("p8_adaptive"),
        Variant("p200_adaptive", shuffle_partitions=200),
        Variant("p8_no_adaptive", adaptive=False),
        Variant("p200_no_adaptive", shuffle_partitions=200, adaptive=False),
    ],
    "broadcast": [
        Variant("hints"),
        Variant("spark_chooses", broadcast="auto"),
        Variant("off", broadcast="off"),
    ],
    "size": [
        Variant("5_years"),
        Variant("1_year", input="1y"),
        Variant("1_month", input="1m"),
    ],
}

# The read batch: test -> variants (the first is the reference), and queries per process.
READ_TESTS: Dict[str, List[str]] = {
    "partition_one_month": ["partitioned", "unpartitioned"],
    "partition_whole_table": ["partitioned", "unpartitioned"],
    "schema": ["explicit", "inferred"],
    "small_files": ["default_tasks", "fewer_tasks", "compacted_parquet"],
}
QUERIES_PER_PROCESS = {"partition_one_month": 5, "partition_whole_table": 5, "schema": 1, "small_files": 1}
LOW_OPEN_COST_BYTES = 16 * 1024  # about the size of one Bronze file; Spark's default is 4 MB

SQL_PLAN_EVENTS = (
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",  # adaptive execution's changes
)

PHASES = ["read_clean", "zone_hourly", "checks", "chaos_score", "scorecard", "write", "other"]
RUN_COLUMNS = (
    ["session", "batch", "variant", "repeat", "is_warmup", "status", "elapsed_s"]
    + [f"{p}_s" for p in PHASES]
    + ["jobs", "stages", "tasks", "failed_tasks", "hosts", "task_s", "cpu_s", "gc_s", "input_mb", "shuffle_read_mb",
       "shuffle_remote_mb", "shuffle_write_mb", "spill_disk_mb", "slowest_task_s", "worst_skew_ratio"]
    + ["broadcast_hash_joins", "sort_merge_joins", "other_joins", "exchanges", "chaos_recall", "fingerprint"]
    + ["master", "shuffle_partitions", "adaptive", "cache", "broadcast", "input", "load_before", "load_after",
       "git_commit", "started_at_utc", "error"]
)
READ_COLUMNS = ["session", "test", "variant", "process", "query", "status", "seconds", "inference_s", "files_read",
                "tasks_per_query", "input_mb_per_query", "git_commit", "error"]


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def input(self) -> Path:
        return self.root / "input"

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
# Freezing the input
# ---------------------------------------------------------------------- #
def freeze_input(source: Path, paths: Paths) -> Dict[str, Dict[str, float]]:
    """Copy `source` once into input/full and link the subsets. An existing copy is never replaced."""
    full = paths.input / "full"
    paths.input.mkdir(parents=True, exist_ok=True)
    if not full.exists():
        if not (source / "_manifests").is_dir():
            raise FileNotFoundError(f"{source} is not a chaos copy: it has no _manifests folder")
        partial = paths.input / "full.partial"
        shutil.rmtree(partial, ignore_errors=True)
        # Hidden names are the writers' temp files.
        shutil.copytree(source, partial, ignore=shutil.ignore_patterns(".*"))
        os.replace(partial, full)

    for name, (year, month) in SUBSETS.items():
        target = paths.input / name
        if not target.exists():
            partial = paths.input / f"{name}.partial"
            shutil.rmtree(partial, ignore_errors=True)
            # Hard links: a subset takes no extra disk space.
            shutil.copytree(full, partial, ignore=_subset_filter(year, month), copy_function=os.link)
            os.replace(partial, target)

    sizes = {}
    for name in ["full", *SUBSETS]:
        files = [p for p in (paths.input / name).rglob("*.json") if "_manifests" not in p.parts]
        sizes[name] = {"files": len(files), "mb": round(sum(p.stat().st_size for p in files) / 1e6, 1)}
    paths.results.mkdir(parents=True, exist_ok=True)
    (paths.results / "input.json").write_text(json.dumps({"source": str(source), "inputs": sizes}, indent=2), "utf-8")
    return sizes


def _subset_filter(year: str, month: Optional[str]) -> Callable[[str, List[str]], List[str]]:
    def ignore(_folder: str, names: List[str]) -> List[str]:
        return [
            n for n in names
            if (n.startswith("year=") and n != f"year={year}")
            or (month is not None and n.startswith("month=") and n != f"month={month}")
        ]
    return ignore


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
def _apply_cache_mode(mode: str) -> None:
    """Make DataFrame.cache do nothing, optionally except in the Bronze reader."""
    if mode == "on":
        return
    from pyspark.sql import DataFrame

    real_cache = DataFrame.cache

    def cache(self):
        if mode == "bronze_only" and sys._getframe(1).f_code.co_name == "read_bronze_files":
            return real_cache(self)
        return self

    DataFrame.cache = cache


def _apply_broadcast_mode(mode: str) -> None:
    """Make functions.broadcast do nothing, so Spark chooses the join itself."""
    if mode == "hint":
        return
    from pyspark.sql import functions

    functions.broadcast = lambda df: df


def _plan_text(df) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()


def _session(app_name: str, adaptive: bool = True, no_auto_broadcast: bool = False):
    from src.silver.spark_session import get_spark

    spark = get_spark(app_name=app_name)
    spark.conf.set("spark.sql.adaptive.enabled", str(adaptive).lower())
    if no_auto_broadcast:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    return spark


def _full_run_child(spec: Dict) -> int:
    """One full Silver run, with timers around its phases. Writes spec['result_file']."""
    from pyspark.sql import functions as F

    from src.silver import run_silver as silver

    variant = Variant(**spec["variant"])
    _apply_cache_mode(variant.cache)
    _apply_broadcast_mode(variant.broadcast)
    spark = _session(f"perf-{spec['batch']}-{variant.name}", variant.adaptive, variant.broadcast == "off")

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
        if variant.cache == "on":
            # zone_hourly is lazy, so it would otherwise count as the first check's time. With the
            # cache on, this count does the work that check would do, and the check then reuses it.
            begin = time.perf_counter()
            args[2].count()
            phases["zone_hourly"] = time.perf_counter() - begin
        return timed("checks", real_checks)(*args, **kwargs)

    silver.run_checks = checks
    silver.chaos_score = timed("chaos_score", silver.chaos_score)
    silver.scorecard = timed("scorecard", silver.scorecard)
    silver.write_table = timed("write", silver.write_table)
    silver.publish = timed("write", silver.publish)

    output_dir = Path(spec["output_dir"])
    exit_code = silver.run_silver(Path(spec["input_dir"]), output_dir, "bronze_chaos", spark=spark)
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
            *(f"{t}={manifest['rows'][t]}" for t in ("prices", "weather_hourly", "zone_hourly")),
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


def _read_all_domains(spark, input_dir: Path, inferred: bool = False) -> Tuple[List, float]:
    """The Bronze envelopes of both domains, and the seconds spent inferring their schemas."""
    from src.silver.bronze_reader import DOMAIN_PRICES, DOMAIN_WEATHER, ENVELOPE_SCHEMAS

    frames, inference_s = [], 0.0
    for domain in (DOMAIN_PRICES, DOMAIN_WEATHER):
        reader = spark.read.option("multiLine", True).option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record")
        begin = time.perf_counter()
        if inferred:
            frames.append(reader.json(str(input_dir / domain)))  # reads every file to learn the schema
            inference_s += time.perf_counter() - begin
        else:
            frames.append(reader.schema(ENVELOPE_SCHEMAS[domain]).json(str(input_dir / domain)))
    return frames, inference_s


def _prepare_read_child(spec: Dict) -> int:
    """Build the tables the read tests use. Not measured."""
    from src.silver import run_silver as silver
    from src.silver.bronze_reader import DOMAIN_PRICES, DOMAIN_WEATHER

    spark = _session("perf-read-prepare")
    work, input_dir = Path(spec["work_dir"]), Path(spec["input_dir"])
    code = silver.run_silver(input_dir, work / "silver", "bronze_chaos", spark=spark)
    if code != silver.EXIT_OK:
        return code
    spark.read.parquet(str(work / "silver" / "zone_hourly")).coalesce(1).write.mode("overwrite") \
        .parquet(str(work / "zone_hourly_unpartitioned"))
    frames, _ = _read_all_domains(spark, input_dir)
    for domain, frame in zip((DOMAIN_PRICES, DOMAIN_WEATHER), frames):
        frame.select("payload").coalesce(1).write.mode("overwrite").parquet(str(work / "bronze_compact" / domain))
    spark.stop()
    return 0


def _read_child(spec: Dict) -> int:
    """Run one read test's query several times. Writes spec['result_file']."""
    from pyspark.sql import functions as F

    from src.silver.bronze_reader import DOMAIN_PRICES, DOMAIN_WEATHER

    test, variant = spec["test"], spec["variant"]
    work, input_dir = Path(spec["work_dir"]), Path(spec["input_dir"])
    spark = _session(f"perf-read-{test}-{variant}")
    measured = []
    plan, files_read = "", 0
    # With several queries per process, the first one is a cold start and is thrown away.
    warmups = 1 if spec["queries"] > 1 else 0

    for query_number in range(warmups + spec["queries"]):
        begin = time.perf_counter()
        inference_s = 0.0
        if test.startswith("partition_"):
            table = "silver/zone_hourly" if variant == "partitioned" else "zone_hourly_unpartitioned"
            df = spark.read.parquet(str(work / table))
            if test == "partition_one_month":
                df = df.filter((F.col("year") == READ_MONTH[0]) & (F.col("month") == READ_MONTH[1]))
            query = df.agg(F.avg("price_nok_kwh"))
            query.first()
            plan, files_read = _plan_text(query), len(df.inputFiles())
        else:
            if variant == "compacted_parquet":
                frames = [spark.read.parquet(str(work / "bronze_compact" / d)) for d in (DOMAIN_PRICES, DOMAIN_WEATHER)]
            else:
                if variant == "fewer_tasks":
                    spark.conf.set("spark.sql.files.openCostInBytes", str(LOW_OPEN_COST_BYTES))
                frames, inference_s = _read_all_domains(spark, input_dir, inferred=variant == "inferred")
            # The same forced parse for every variant: JSON is only parsed for the columns a query needs.
            for frame in frames:
                frame.select(F.sum(F.size("payload"))).first()
            files_read = sum(len(frame.inputFiles()) for frame in frames)
        if query_number >= warmups:
            measured.append({"seconds": round(time.perf_counter() - begin, 3), "inference_s": round(inference_s, 3)})

    Path(spec["result_file"]).write_text(json.dumps({
        "measured": measured,
        "files_read": files_read,
        "plan": plan,
        "app_id": spark.sparkContext.applicationId,
    }), "utf-8")
    spark.stop()
    return 0


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


def _child_env(master: str, shuffle_partitions: int, paths: Paths) -> Dict[str, str]:
    env = dict(os.environ)
    env.pop("GITHUB_STEP_SUMMARY", None)  # Silver would add its own summary for every run
    env.update({
        "DAT535_ENV": "dev",  # this script's runs pass their own folders; prod is never the default
        "SPARK_MASTER": master,
        "SPARK_SHUFFLE_PARTITIONS": str(shuffle_partitions),
        "SPARK_EXECUTOR_MEMORY": EXECUTOR_MEMORY,
        "PYSPARK_PYTHON": os.environ.get("PYSPARK_PYTHON", sys.executable),
        "PYSPARK_SUBMIT_ARGS": (
            f"--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://{paths.eventlogs} "
            f"--conf spark.executor.memory={EXECUTOR_MEMORY} pyspark-shell"
        ),
    })
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
        "kind": "full", "batch": batch, "variant": asdict(variant), "input_dir": str(paths.input / variant.input),
        "output_dir": str(work / "silver"), "result_file": str(work / "result.json"),
    }
    code = _run_child(spec, _child_env(master, variant.shuffle_partitions, paths), time_limit, work)
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


def run_full_batch(paths: Paths, batch: str, repeats: int, cluster: str, time_limit: float) -> bool:
    variants = FULL_BATCHES[batch]
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


def run_read_batch(paths: Paths, repeats: int, cluster: str, time_limit: float) -> bool:
    session = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    paths.eventlogs.mkdir(parents=True, exist_ok=True)
    (paths.results / "plans").mkdir(parents=True, exist_ok=True)
    tables = paths.out / "read_tables"
    shutil.rmtree(tables, ignore_errors=True)
    tables.mkdir(parents=True)
    input_dir = paths.input / "full"
    env = _child_env(cluster, 8, paths)

    # Reads all of Bronze first, so this also warms the disk cache for the tests.
    print("[read] building the tables for the tests", flush=True)
    code = _run_child({"kind": "prepare_read", "work_dir": str(tables), "input_dir": str(input_dir)}, env, time_limit,
                      tables)
    if code != 0:
        print(f"[read] preparing failed: {_last_error(tables / 'child.log')}", flush=True)
        _keep_failure_log(paths, tables, "read__prepare")
        shutil.rmtree(tables, ignore_errors=True)
        return False

    commit = _git_commit()
    stopped = set()
    tests = [(test, variant) for test, variants in READ_TESTS.items() for variant in variants]
    for process in range(1, repeats + 1):
        for test, variant in tests:
            if (test, variant) in stopped:
                continue
            work = paths.out / f"read__{test}__{variant}__{process}"
            shutil.rmtree(work, ignore_errors=True)
            work.mkdir(parents=True)
            spec = {
                "kind": "read", "test": test, "variant": variant, "queries": QUERIES_PER_PROCESS[test],
                "work_dir": str(tables), "input_dir": str(input_dir), "result_file": str(work / "result.json"),
            }
            code = _run_child(spec, env, time_limit, work)
            base = {"session": session, "test": test, "variant": variant, "process": process, "git_commit": commit}
            if code != 0 or not (work / "result.json").exists():
                error = f"over {time_limit / 60:g} min" if code is None else _last_error(work / "child.log")
                _append_rows(paths.results / "read_runs.csv", READ_COLUMNS,
                             [{**base, "status": "TIMEOUT" if code is None else "FAILED", "error": error}])
                _keep_failure_log(paths, work, f"read__{test}__{variant}__{process}")
                stopped.add((test, variant))
                print(f"[read] {test}/{variant}: failed: {error}", flush=True)
            else:
                result = json.loads((work / "result.json").read_text(encoding="utf-8"))
                log_path = paths.eventlogs / result["app_id"]
                totals = parse_event_log(log_path)["totals"]
                log_path.unlink(missing_ok=True)
                queries = len(result["measured"])
                _append_rows(paths.results / "read_runs.csv", READ_COLUMNS, [
                    {**base, "status": "SUCCESS", "query": i + 1, **m, "files_read": result["files_read"],
                     "tasks_per_query": round(totals["tasks"] / queries, 1),
                     "input_mb_per_query": round(totals["input_mb"] / queries, 2)}
                    for i, m in enumerate(result["measured"])
                ])
                if result["plan"]:
                    (paths.results / "plans" / f"read__{test}__{variant}.txt").write_text(result["plan"], "utf-8")
                median = statistics.median(m["seconds"] for m in result["measured"])
                print(f"[read] {test}/{variant} round {process}/{repeats}: {median} s", flush=True)
            shutil.rmtree(work, ignore_errors=True)
    shutil.rmtree(tables, ignore_errors=True)
    return True


# ---------------------------------------------------------------------- #
# The summary
# ---------------------------------------------------------------------- #
def _floats(rows: List[Dict], column: str) -> List[float]:
    return [float(r[column]) for r in rows if r.get(column) not in (None, "")]


def _median_text(rows: List[Dict], column: str) -> str:
    values = _floats(rows, column)
    return f"{statistics.median(values):g}" if values else "–"


def _time_cells(values: List[float], reference: List[float]) -> List[str]:
    """Median, range, and the change against the reference's median."""
    if not values:
        return ["–", "–", "–"]
    median = statistics.median(values)
    base = statistics.median(reference) if reference else None
    change = f"{100 * (median - base) / base:+.0f}%" if base else "–"
    return [f"{median:.1f}", f"{min(values):.1f}–{max(values):.1f}", change]


def _latest(rows: List[Dict], key: str) -> Dict[str, List[Dict]]:
    """Rows per batch or test, from its newest session only (a rerun replaces the earlier one)."""
    grouped: Dict[str, List[Dict]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)
    return {name: [r for r in group if r["session"] == max(x["session"] for x in group)]
            for name, group in grouped.items()}


def build_summary(run_rows: List[Dict], read_rows: List[Dict]) -> str:
    lines = [
        "# Silver performance results",
        "",
        "Median and range of the measured runs. A difference from a batch's first variant counts only if the "
        "two ranges don't overlap. A run counts only if it ends with SUCCESS.",
    ]
    batches = _latest(run_rows, "batch")
    for batch in [b for b in FULL_BATCHES if b in batches]:
        rows = [r for r in batches[batch] if r["is_warmup"] != "True"]
        names = [v.name for v in FULL_BATCHES[batch]]
        by_variant = {n: [r for r in rows if r["variant"] == n] for n in names}
        reference_rows = [r for r in by_variant[names[0]] if r["status"] == "SUCCESS"]
        reference = _floats(reference_rows, "elapsed_s")
        reference_prints = {r["fingerprint"] for r in reference_rows}

        lines += ["", f"## {batch}", "",
                  "| Variant | Runs | Median (s) | Range (s) | Change | Verdict | Same output | Hosts | Tasks | "
                  "Shuffled (MB) | Spilled (MB) | Broadcast joins | Sort-merge joins | Other joins | Shuffles |",
                  "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for name in names:
            group = by_variant[name]
            good = [r for r in group if r["status"] == "SUCCESS"]
            if not good:
                problem = group[0] if group else {"status": "not run", "error": ""}
                detail = f"{problem['status']}: {problem.get('error') or ''}".strip().rstrip(":").replace("|", "/")
                lines.append(f"| {name} | 0 | – | – | – | {detail} |" + " – |" * 9)
                continue
            times = _floats(good, "elapsed_s")
            if name == names[0]:
                verdict, same = "reference", "–"
            else:
                verdict = compare(reference, times)
                if not reference_rows or good[0]["input"] != reference_rows[0]["input"]:
                    same = "–"
                else:
                    same = "yes" if {r["fingerprint"] for r in good} <= reference_prints else "**NO**"
            cells = [name, str(len(good)), *_time_cells(times, reference), verdict, same]
            cells += [_median_text(good, c) for c in ("hosts", "tasks", "shuffle_read_mb", "spill_disk_mb",
                                                      "broadcast_hash_joins", "sort_merge_joins", "other_joins",
                                                      "exchanges")]
            lines.append("| " + " | ".join(cells) + " |")

        lines += ["", "Where the time goes (median seconds):", "",
                  "| Variant | " + " | ".join(p.replace("_", " ") for p in PHASES) + " |",
                  "|---|" + "---|" * len(PHASES)]
        for name in names:
            good = [r for r in by_variant[name] if r["status"] == "SUCCESS"]
            if good:
                lines.append(f"| {name} | " + " | ".join(_median_text(good, f"{p}_s") for p in PHASES) + " |")

    tests = _latest(read_rows, "test")
    if tests:
        lines += ["", "## read", "", "| Test | Variant | Median (s) | Range (s) | Change | Verdict | Files read | "
                  "Tasks per query | Data read (MB) | Schema inference (s) |", "|---|---|---|---|---|---|---|---|---|---|"]
    for test in [t for t in READ_TESTS if t in tests]:
        names = READ_TESTS[test]
        good_rows = [r for r in tests[test] if r["status"] == "SUCCESS"]
        reference = _floats([r for r in good_rows if r["variant"] == names[0]], "seconds")
        for name in names:
            group = [r for r in good_rows if r["variant"] == name]
            if not group:
                failed = [r for r in tests[test] if r["variant"] == name]
                detail = f"{failed[0]['status']}: {failed[0].get('error', '')}".replace("|", "/") if failed else "not run"
                lines.append(f"| {test} | {name} | – | – | – | {detail} |" + " – |" * 4)
                continue
            times = _floats(group, "seconds")
            verdict = "reference" if name == names[0] else compare(reference, times)
            cells = [test, name, *_time_cells(times, reference), verdict]
            cells += [_median_text(group, c) for c in ("files_read", "tasks_per_query", "input_mb_per_query",
                                                       "inference_s")]
            lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def write_summary(paths: Paths) -> str:
    paths.results.mkdir(parents=True, exist_ok=True)
    text = build_summary(_read_rows(paths.results / "runs.csv"), _read_rows(paths.results / "read_runs.csv"))
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
        children = {"full": _full_run_child, "prepare_read": _prepare_read_child, "read": _read_child}
        return children[spec["kind"]](spec)

    parser = argparse.ArgumentParser(description="DAT535 Silver performance experiments",
                                     formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("batch", choices=["setup", *FULL_BATCHES, "read", "summary"])
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS, help="measured runs per variant")
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

    if not (paths.input / "full").is_dir():
        print(f"No frozen input in {paths.input}: run the setup batch first.", file=sys.stderr)
        return 2
    paths.results.mkdir(parents=True, exist_ok=True)
    if args.batch == "read":
        finished = run_read_batch(paths, args.repeats, args.cluster, args.time_limit)
    else:
        finished = run_full_batch(paths, args.batch, args.repeats, args.cluster, args.time_limit)
    print(write_summary(paths))
    return 0 if finished else 1


if __name__ == "__main__":
    sys.exit(main())
