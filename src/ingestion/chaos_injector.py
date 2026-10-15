"""
Chaos injection for Bronze data.
The APIs return very clean data, so for Silver cleaning/DLQ exercises this module derives a
corrupted copy of Bronze (`bronze_chaos/`) with roughly `rate` (default 1%) of records damaged.
The original Bronze files are only read, never modified.

Record-level error types (each also listed in a ground-truth log so Silver can be scored):
    duplicate      -> the record is emitted twice
    null_field     -> the essential value (price / observation value) becomes null
    out_of_range   -> the essential value becomes an impossible number (e.g. 999.9 degC)
    bad_timestamp  -> the record timestamp becomes unparseable
File-level error type:
    corrupt_file   -> the whole file is truncated mid-JSON (all its records are lost)

The `rate` budget is shared: FILE_CORRUPTION_SHARE of it goes to corrupt files, the rest to
record-level errors, so the expected fraction of damaged records stays ~`rate` overall.
`_metadata` is copied unchanged, so a modified payload no longer matches `_payload_checksum`.

Every run writes a ground-truth manifest `bronze_chaos/_manifests/run__{batch_id}.json` listing
each injected error. Files already present in `bronze_chaos/` are skipped, so reruns only process
new Bronze files; the union of all chaos manifests is the full ground truth.

Usage:
    python src/ingestion/chaos_injector.py                          # 1% errors, seed 42
    python src/ingestion/chaos_injector.py --rate 0.02 --seed 7
"""

import argparse
from collections import Counter
import copy
import json
import logging
from pathlib import Path
import random
import sys
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is on sys.path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import BRONZE_CHAOS_DIR, BRONZE_DIR
from src.ingestion.bronze_writer import BronzeWriter
from src.utils.hashing import generate_batch_id, get_utc_now_iso

logger = logging.getLogger("chaos_injector")

DEFAULT_RATE = 0.01
DEFAULT_SEED = 42
FILE_CORRUPTION_SHARE = 0.2

ERROR_DUPLICATE = "duplicate"
ERROR_NULL_FIELD = "null_field"
ERROR_OUT_OF_RANGE = "out_of_range"
ERROR_BAD_TIMESTAMP = "bad_timestamp"
RECORD_ERROR_TYPES = [ERROR_DUPLICATE, ERROR_NULL_FIELD, ERROR_OUT_OF_RANGE, ERROR_BAD_TIMESTAMP]
ERROR_CORRUPT_FILE = "corrupt_file"

# Outside the Silver outlier bounds (-15..20 NOK/kWh, -50..45 degC) from the project definition.
_OUT_OF_RANGE_VALUE = {"electricity": 999.0, "weather": 999.9}
_TIMESTAMP_FIELD = {"electricity": "time_start", "weather": "referenceTime"}
_BAD_TIMESTAMP = "not-a-timestamp"


def _value_location(record: Dict[str, Any], domain: str, rng: random.Random):
    """Return (container, key, field_label) of the record's essential value, or None if absent."""
    if domain == "electricity":
        return (record, "NOK_per_kWh", "NOK_per_kWh") if "NOK_per_kWh" in record else None
    observations = record.get("observations") or []
    if not observations:
        return None
    i = rng.randrange(len(observations))
    return observations[i], "value", f"observations[{i}].value"


def inject_record_errors(
    payload: List[Dict[str, Any]],
    domain: str,
    rng: random.Random,
    rate: float = DEFAULT_RATE,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Corrupt each record with probability `rate` using one randomly chosen error type.

    Returns:
        (new_payload, events). `payload` is not modified. Each event has the error `type`, the
        `index` of the affected record in new_payload, and for value/timestamp errors the
        `field` and its `original` value.
    """
    out: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []

    for record in payload:
        record = copy.deepcopy(record)
        if rng.random() >= rate:
            out.append(record)
            continue

        error_type = rng.choice(RECORD_ERROR_TYPES)
        location = _value_location(record, domain, rng)
        if error_type in (ERROR_NULL_FIELD, ERROR_OUT_OF_RANGE) and location is None:
            error_type = ERROR_BAD_TIMESTAMP  # nothing to null/overflow in this record

        if error_type == ERROR_DUPLICATE:
            out.append(record)
            out.append(copy.deepcopy(record))
            events.append({"type": error_type, "index": len(out) - 1})
            continue

        if error_type == ERROR_BAD_TIMESTAMP:
            container, key, field = record, _TIMESTAMP_FIELD[domain], _TIMESTAMP_FIELD[domain]
            new_value = _BAD_TIMESTAMP
        else:
            container, key, field = location
            new_value = None if error_type == ERROR_NULL_FIELD else _OUT_OF_RANGE_VALUE[domain]

        events.append({
            "type": error_type,
            "index": len(out),
            "field": field,
            "original": container.get(key),
        })
        container[key] = new_value
        out.append(record)

    return out, events


def _is_bronze_data_file(rel_path: Path) -> bool:
    """Skip run manifests (`_manifests/`) and hidden temp files, like Spark does."""
    return not any(part.startswith(("_", ".")) for part in rel_path.parts)


def chaos_copy_file(
    src: Path,
    target: Path,
    domain: str,
    rng: random.Random,
    rate: float = DEFAULT_RATE,
) -> List[Dict[str, Any]]:
    """Write a (possibly corrupted) copy of one Bronze file to `target`; return injected events."""
    text = src.read_text(encoding="utf-8")
    envelope = json.loads(text)
    target.parent.mkdir(parents=True, exist_ok=True)

    if rng.random() < rate * FILE_CORRUPTION_SHARE:
        # Any proper prefix of a JSON object is invalid JSON.
        cut = rng.randint(1, len(text) - 1)
        target.write_text(text[:cut], encoding="utf-8")
        return [{"type": ERROR_CORRUPT_FILE, "records": len(envelope.get("payload", []))}]

    payload, events = inject_record_errors(
        envelope.get("payload", []), domain, rng, rate * (1 - FILE_CORRUPTION_SHARE)
    )
    BronzeWriter._replace_json(target, {"_metadata": envelope.get("_metadata", {}), "payload": payload})
    return events


def inject_bronze_tree(
    bronze_dir: Path,
    output_dir: Path,
    rate: float = DEFAULT_RATE,
    seed: int = DEFAULT_SEED,
) -> Dict[str, Any]:
    """
    Mirror every Bronze data file not yet in `output_dir` into it with injected errors.

    Each file gets its own RNG seeded from (seed, relative path), so a file's corruption does
    not depend on which other files exist (daily runs and historical backfills agree).
    Returns files_processed, files_skipped, and all events tagged with the relative `file` path.
    """
    bronze_dir, output_dir = Path(bronze_dir), Path(output_dir)
    result = {"files_processed": 0, "files_skipped": 0, "events": []}

    for src in sorted(bronze_dir.rglob("*.json")):
        rel = src.relative_to(bronze_dir)
        if not _is_bronze_data_file(rel):
            continue
        target = output_dir / rel
        if target.exists():
            result["files_skipped"] += 1
            continue
        rng = random.Random(f"{seed}:{rel.as_posix()}")
        events = chaos_copy_file(src, target, rel.parts[0], rng, rate)
        for e in events:
            e["file"] = rel.as_posix()
        result["events"] += events
        result["files_processed"] += 1

    logger.info(
        f"Chaos copy: {result['files_processed']} files processed, {result['files_skipped']} "
        f"already present, {len(result['events'])} errors injected -> {output_dir}"
    )
    return result


def run_chaos_injection(
    bronze_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    rate: float = DEFAULT_RATE,
    seed: int = DEFAULT_SEED,
) -> Path:
    """Build/extend the chaos copy and persist its ground-truth manifest; returns the manifest path."""
    bronze_dir = Path(bronze_dir or BRONZE_DIR)
    output_dir = Path(output_dir or BRONZE_CHAOS_DIR)
    if output_dir.resolve().is_relative_to(bronze_dir.resolve()):
        raise ValueError(f"Chaos output {output_dir} must not be inside Bronze {bronze_dir}")
    result = inject_bronze_tree(bronze_dir, output_dir, rate=rate, seed=seed)

    manifest = {
        "batch_id": generate_batch_id(),
        "created_at_utc": get_utc_now_iso(),
        "bronze_dir": str(bronze_dir),
        "rate": rate,
        "seed": seed,
        "files_processed": result["files_processed"],
        "files_skipped": result["files_skipped"],
        "counts": dict(Counter(e["type"] for e in result["events"])),
        "events": result["events"],
    }
    return BronzeWriter(bronze_dir=output_dir).write_manifest(manifest)


def _rate_arg(value: str) -> float:
    rate = float(value)
    if not 0.0 <= rate <= 1.0:
        raise argparse.ArgumentTypeError(f"rate must be between 0 and 1, got {value}")
    return rate


def main() -> int:
    """CLI Argument Parser."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="DAT535 chaos injection: corrupted copy of Bronze for Silver cleaning exercises",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--rate", type=_rate_arg, default=DEFAULT_RATE, help="Fraction of records to damage (default 0.01)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed for reproducible errors (default 42)")
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR, help=f"Source Bronze dir (default {BRONZE_DIR})")
    parser.add_argument(
        "--output-dir", type=Path, default=BRONZE_CHAOS_DIR, help=f"Chaos copy dir (default {BRONZE_CHAOS_DIR})"
    )
    args = parser.parse_args()

    if not args.bronze_dir.is_dir():
        logger.error(f"Bronze directory not found: {args.bronze_dir}")
        return 2

    run_chaos_injection(args.bronze_dir, args.output_dir, rate=args.rate, seed=args.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
