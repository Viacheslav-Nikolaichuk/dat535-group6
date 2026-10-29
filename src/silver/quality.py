"""
Data-quality bookkeeping for the Silver layer (docs/plans/silver_layer.md, section 9).

The issue log (`silver/dq_issues`) gets one row per problem Silver finds, with what it did:
    rejected  the row, value or file is not used (rejected rows are the dead-letter queue)
    repaired  the value was rebuilt from other fields of the same record
    removed   a duplicate copy was dropped
    excluded  a whole file was left out on purpose (empty or synthetic test data)

Cleaning code marks problems per row as an array of issue structs (`collect_issues`), and
`issues_to_log` turns them into log rows. Nothing is dropped without a matching log row.

Before anything is written, `run_checks` tests the finished tables: a failed hard check stops the
run. `scorecard` produces the before/after numbers for `silver/dq_metrics`, `chaos_score` matches
the issue log against the errors the chaos injector recorded, and the run manifest
(`silver/_runs/run__<id>.json`) tells Gold whether and how fresh Silver is.
"""

from dataclasses import asdict, dataclass
from functools import reduce
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.bronze_writer import BronzeWriter

ACTION_REJECTED = "rejected"
ACTION_REPAIRED = "repaired"
ACTION_REMOVED = "removed"
ACTION_EXCLUDED = "excluded"

# The original record is kept with each issue, shortened so a bad file can't bloat the log.
RAW_VALUE_MAX_CHARS = 1000

# A run that rejects more than this share of a source's records fails (the chaos rate is 1%).
MAX_REJECTED_RECORD_SHARE = 0.5

ISSUE_COLUMNS = ["source", "issue", "action", "source_file", "record_index", "field", "raw_value"]


def issue_if(
    condition: Column,
    issue: str,
    action: Union[str, Column],
    field: Optional[Union[str, Column]] = None,
) -> Column:
    """An issue struct when `condition` holds, otherwise null."""
    action_col = action if isinstance(action, Column) else F.lit(action)
    field_col = field if isinstance(field, Column) else F.lit(field).cast("string")
    return F.when(
        condition,
        F.struct(F.lit(issue).alias("issue"), action_col.alias("action"), field_col.alias("field")),
    )


def collect_issues(*issues: Column) -> Column:
    """Array of the issues that apply to a row (nulls dropped)."""
    return F.filter(F.array(*issues), lambda i: i.isNotNull())


def issues_to_log(rows: DataFrame, source: str) -> DataFrame:
    """Issue-log rows from a DataFrame with `issues`, `source_file`, `record_index`, `raw_value`."""
    return rows.select(
        F.lit(source).alias("source"),
        "source_file",
        "record_index",
        "raw_value",
        F.explode("issues").alias("i"),
    ).select(
        "source",
        F.col("i.issue").alias("issue"),
        F.col("i.action").alias("action"),
        "source_file",
        "record_index",
        F.col("i.field").alias("field"),
        "raw_value",
    )


def raw_json(record: Column) -> Column:
    """The original record as (shortened) JSON for the issue log."""
    return F.substring(F.to_json(record), 1, RAW_VALUE_MAX_CHARS)


# ---------------------------------------------------------------------- #
# Checks before writing (section 9.1)
# ---------------------------------------------------------------------- #
@dataclass
class Check:
    """One check result. A hard check that fails stops the run; a soft one only warns."""

    name: str
    hard: bool
    passed: bool
    value: float
    detail: str = ""


def _count_bad(name: str, bad_rows: int, detail: str) -> Check:
    return Check(name, hard=True, passed=bad_rows == 0, value=float(bad_rows), detail=detail)


def _key_checks(table: str, df: DataFrame, keys: Sequence[str]) -> List[Check]:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    null_keys = df.filter(reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])).count()
    off_hour = df.filter(F.col("utc_hour") != F.date_trunc("hour", "utc_hour")).count()
    return [
        _count_bad(f"{table}.duplicate_keys", duplicates, f"keys sharing ({', '.join(keys)})"),
        _count_bad(f"{table}.null_keys", null_keys, "rows with an empty key"),
        _count_bad(f"{table}.off_hour", off_hour, "utc_hour not on the full hour"),
    ]


def _bounds_check(name: str, df: DataFrame, column: str, low: float, high: float, unit: str,
                  allow_null: bool = True) -> Check:
    bad = ~F.col(column).between(low, high)
    if allow_null:
        bad = F.col(column).isNotNull() & bad
    else:
        bad = F.col(column).isNull() | bad
    return _count_bad(name, df.filter(bad).count(), f"{column} outside [{low:g}, {high:g}] {unit}")


def run_checks(
    prices: DataFrame,
    weather_hourly: DataFrame,
    zone_hourly: DataFrame,
    dim_date: DataFrame,
    zones: Sequence[str],
    issues: DataFrame,
    records_checked: Dict[str, int],
    records_rejected: Dict[str, int],
    nok_eur_mismatches: int,
    price_bounds: Tuple[float, float],
    temp_bounds: Tuple[float, float],
    wind_bounds: Tuple[float, float],
) -> List[Check]:
    """Hard checks (must pass before writing) and soft checks (warnings) on the finished tables."""
    checks: List[Check] = []
    checks += _key_checks("prices", prices, ["bidding_zone", "utc_hour"])
    checks += _key_checks("weather_hourly", weather_hourly, ["station_id", "utc_hour"])
    checks += _key_checks("zone_hourly", zone_hourly, ["bidding_zone", "utc_hour"])
    checks.append(_bounds_check("prices.price_bounds", prices, "price_nok_kwh", *price_bounds, "NOK/kWh",
                                allow_null=False))
    for table, df in (("weather_hourly", weather_hourly), ("zone_hourly", zone_hourly)):
        checks.append(_bounds_check(f"{table}.temp_bounds", df, "temp_celsius", *temp_bounds, "°C"))
        checks.append(_bounds_check(f"{table}.wind_bounds", df, "wind_speed_mps", *wind_bounds, "m/s"))

    # Empty tables pass every check above, and writing them would erase the previous Silver.
    for table, df in (("prices", prices), ("weather_hourly", weather_hourly)):
        n = df.count()
        checks.append(Check(f"{table}.not_empty", True, n > 0, float(n),
                            "rows in the table; an empty input or a schema change must not erase Silver"))

    # A source that loses most of its records points at a broken input (e.g. a renamed field),
    # not at bad luck; publishing it would replace good Silver with a fragment.
    for source, n_records in records_checked.items():
        share = records_rejected.get(source, 0) / n_records if n_records else 0.0
        checks.append(Check(f"{source}.mostly_rejected", True, share <= MAX_REJECTED_RECORD_SHARE,
                            round(100 * share, 3),
                            f"rejected records per checked record (%), fails above {MAX_REJECTED_RECORD_SHARE:.0%}"))

    span_hours = prices.agg(
        ((F.max("utc_hour").cast("long") - F.min("utc_hour").cast("long")) / 3600 + 1).alias("n")
    ).first().n
    expected_rows = int(span_hours or 0) * len(zones)
    actual_rows = zone_hourly.count()
    checks.append(Check("zone_hourly.grid_rows", True, actual_rows == expected_rows, float(actual_rows),
                        f"expected {expected_rows} = {len(zones)} zones x {int(span_hours or 0)} hours"))

    # Soft: a finished day with fewer price hours than the day has (23/24/25).
    per_day = zone_hourly.groupBy("bidding_zone", "local_date").agg(F.sum(F.col("has_price").cast("int")).alias("hours"))
    incomplete = per_day.join(dim_date.select("local_date", "hours_in_day"), "local_date").filter(
        F.col("hours") < F.col("hours_in_day")
    ).count()
    checks.append(Check("zone_hourly.incomplete_price_days", False, incomplete == 0, float(incomplete),
                        "zone-days with fewer price hours than hours_in_day"))

    # Soft: weather completeness per zone, over the hours weather can exist (first to last observation).
    span = weather_hourly.agg(F.min("utc_hour").alias("w_first"), F.max("utc_hour").alias("w_last"))
    shares = {
        r.bidding_zone: r.share
        for r in zone_hourly.crossJoin(span)
        .filter(F.col("utc_hour").between(F.col("w_first"), F.col("w_last")))
        .groupBy("bidding_zone")
        .agg(F.avg(F.col("has_weather").cast("double")).alias("share"))
        .collect()
    }
    low = sorted(z for z, share in shares.items() if share < 0.99)
    checks.append(Check("zone_hourly.weather_completeness", False, not low,
                        round(100 * min(shares.values()), 2) if shares else 100.0,
                        f"lowest share of hours with a temperature (%); below 99%: {', '.join(low) or 'none'}"))

    # Soft: far more rejections than the chaos rate (1%) means a rule or a source is off.
    rejected = {r.source: r["count"] for r in issues.filter(F.col("action") == ACTION_REJECTED).groupBy("source").count().collect()}
    for source, n_records in records_checked.items():
        share = rejected.get(source, 0) / n_records if n_records else 0.0
        checks.append(Check(f"{source}.rejected_share", False, share <= 0.05, round(100 * share, 3),
                            "rejected issues per checked record (%), warning above 5%"))

    checks.append(Check("prices.nok_eur_mismatch", False, nok_eur_mismatches == 0, float(nok_eur_mismatches),
                        "rows where NOK differs from EUR x EXR by more than 0.001"))
    return checks


# ---------------------------------------------------------------------- #
# Scoring against the chaos list (section 9.3)
# ---------------------------------------------------------------------- #
# Chaos error type -> the issue Silver should log for it.
EXPECTED_ISSUE = {
    "duplicate": "duplicate",
    "null_field": "null_value",
    "out_of_range": "out_of_range",
    "bad_timestamp": "bad_timestamp",
    "corrupt_file": "corrupt_file",
}


def load_chaos_events(input_dir: Path) -> Optional[List[Dict]]:
    """Every error the chaos injector recorded for `input_dir`, or None if it is not a chaos copy."""
    events, is_chaos = [], False
    for path in sorted((Path(input_dir) / "_manifests").glob("run__*.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if "events" in manifest:  # Bronze's own ingestion manifests have no events
            is_chaos = True
            events.extend(manifest["events"])
    return events if is_chaos else None


def chaos_score(spark: SparkSession, events: List[Dict], issues: DataFrame, checked_files: DataFrame) -> Dict:
    """Share of injected errors that Silver logged as the expected issue, per chaos type."""
    rows = [(i, e["file"], e["type"], e.get("index"), e.get("field")) for i, e in enumerate(events)]
    ev = spark.createDataFrame(rows, "event_id long, file string, type string, idx int, field string")
    # Only errors in files Silver looked at count (synthetic files are left out on purpose).
    ev = ev.join(checked_files.select(F.col("source_file").alias("file")).distinct(), "file", "left_semi")
    expected = F.create_map(*[F.lit(x) for pair in EXPECTED_ISSUE.items() for x in pair])
    ev = ev.withColumn("expected", expected[F.col("type")])

    logged = issues.filter(F.col("action") != ACTION_EXCLUDED).select(
        F.col("source_file").alias("i_file"),
        F.col("issue").alias("i_issue"),
        F.col("record_index").alias("i_index"),
        F.col("field").alias("i_field"),
    )
    # Same file and the expected issue; for record errors also the same record (and field, if any).
    match = (
        (ev["file"] == logged["i_file"])
        & (ev["expected"] == logged["i_issue"])
        & (
            (ev["type"] == "corrupt_file")
            | ((ev["idx"] == logged["i_index"]) & (ev["field"].isNull() | (ev["field"] == logged["i_field"])))
        )
    )
    per_event = ev.join(logged, match, "left").groupBy("event_id", "type").agg(
        F.max(F.col("i_issue").isNotNull()).alias("found")
    )
    per_type = {
        r.type: (r.events, r.found)
        for r in per_event.groupBy("type")
        .agg(F.count("*").alias("events"), F.sum(F.col("found").cast("int")).alias("found"))
        .collect()
    }
    # Logged problems with no chaos event: false alarms, or genuine problems in the source data.
    unexplained = {
        r.i_issue: r["count"] for r in logged.join(ev, match, "left_anti").groupBy("i_issue").count().collect()
    }
    total = sum(n for n, _ in per_type.values())
    found_total = sum(f for _, f in per_type.values())
    return {
        "events": total,
        "found": found_total,
        "recall": {t: round(f / n, 4) for t, (n, f) in sorted(per_type.items())},
        "recall_all": round(found_total / total, 4) if total else None,
        "unexplained_issues": sum(unexplained.values()),
        "unexplained_by_issue": dict(sorted(unexplained.items())),
    }


# ---------------------------------------------------------------------- #
# Scorecard (dq_metrics) and the run manifest
# ---------------------------------------------------------------------- #
def scorecard(
    files: Dict[str, DataFrame],
    records_checked: Dict[str, int],
    issues: DataFrame,
    tables: Dict[str, DataFrame],
    checks: List[Check],
    chaos: Optional[Dict],
) -> List[Tuple[str, str, float]]:
    """(source, metric, value) rows: files, records, issues found and what was done, rows written,
    completeness, check values and the chaos score. "Before" is what the issue log found; "after"
    is what the hard checks measure on the written tables (0 duplicates, 0 impossible values)."""
    rows: List[Tuple[str, str, float]] = []
    for source, df in files.items():
        rows += [(source, f"files_{r.status}", float(r["count"])) for r in df.groupBy("status").count().collect()]
    rows += [(source, "records_checked", float(n)) for source, n in records_checked.items()]
    rows += [
        (r.source, f"{r.issue}_{r.action}", float(r["count"]))
        for r in issues.groupBy("source", "issue", "action").count().collect()
    ]
    rows += [(name, "rows", float(df.count())) for name, df in tables.items()]
    rows.append(("prices", "rows_repaired", float(tables["prices"].filter(F.col("is_repaired")).count())))
    rows.append(("weather_hourly", "hours_imputed", float(
        tables["weather_hourly"].filter(F.col("temp_is_imputed") | F.col("wind_is_imputed")).count()
    )))
    shares = tables["zone_hourly"].agg(
        F.avg(F.col("has_price").cast("double")).alias("price"),
        F.avg(F.col("has_weather").cast("double")).alias("weather"),
    ).first()
    rows.append(("zone_hourly", "price_completeness_pct", round(100 * (shares.price or 0.0), 3)))
    rows.append(("zone_hourly", "weather_completeness_pct", round(100 * (shares.weather or 0.0), 3)))
    rows += [(c.name.split(".")[0], c.name.split(".", 1)[1], float(c.value)) for c in checks]
    if chaos:
        rows.append(("chaos", "events", float(chaos["events"])))
        rows += [("chaos", f"recall_{t}", float(r)) for t, r in chaos["recall"].items()]
        rows.append(("chaos", "unexplained_issues", float(chaos["unexplained_issues"])))
    return rows


def write_run_manifest(output_dir: Path, manifest: Dict) -> Path:
    """Write silver/_runs/run__<run_id>.json atomically (temp file + rename)."""
    runs = Path(output_dir) / "_runs"
    runs.mkdir(parents=True, exist_ok=True)
    path = runs / f"run__{manifest['run_id']}.json"
    BronzeWriter._replace_json(path, manifest)
    return path


def step_summary(manifest: Dict) -> str:
    """Markdown for the GitHub Actions step summary."""
    lines = [
        f"## Silver Layer: {manifest['status']}",
        "",
        "| Parameter | Value |",
        "|---|---|",
        f"| Environment | {manifest['environment']} |",
        f"| Input | {manifest['input']} (`{manifest['input_dir']}`) |",
        f"| Elapsed | {manifest['elapsed_seconds']} s |",
    ]
    lines += [f"| Rows: {name} | {n} |" for name, n in manifest["rows"].items()]
    chaos = manifest.get("chaos")
    if chaos and chaos["events"]:
        lines.append(f"| Chaos errors found | {chaos['found']} of {chaos['events']} ({100 * chaos['recall_all']:.1f}%) |")
        lines.append(f"| Issues not on the chaos list | {chaos['unexplained_issues']} |")
    lines += ["", "| Check | Hard | Passed | Value | Detail |", "|---|---|---|---|---|"]
    lines += [
        f"| {c['name']} | {'yes' if c['hard'] else 'no'} | {'✅' if c['passed'] else ('❌' if c['hard'] else '⚠️')} "
        f"| {c['value']:g} | {c['detail']} |"
        for c in manifest["checks"]
    ]
    lines += ["", "| Source | Issue | Action | Count |", "|---|---|---|---|"]
    lines += [f"| {i['source']} | {i['issue']} | {i['action']} | {i['count']} |" for i in manifest["issues"]]
    return "\n".join(lines) + "\n"


def check_dicts(checks: List[Check]) -> List[Dict]:
    return [asdict(c) for c in checks]
