"""
Bronze layer persistence engine.
Wraps raw API payloads with immutable audit metadata envelopes and writes
partitioned JSON files into Bronze storage as defined in Section 3 of the project definition.

Immutability & idempotency model
--------------------------------
Bronze files are never overwritten. Each (partition, date) can hold several *versions*:

    bronze/electricity/zone=NO1/year=2024/month=01/
        2024_01_15.json                                  <- legacy (pre-versioning) file
        2024_01_15__20261015T082112Z_2b8c7998.json       <- version written by batch 2b8c7998...

On every write the SHA-256 checksum of the new payload is compared with the checksum of
the latest existing version for that date:
  * identical  -> nothing is written ("unchanged"); reruns are idempotent.
  * different  -> a new version file is written ("new_version"), e.g. when MET Norway
                  revises observations after quality control (late data).
Silver selects the latest version per key using `_ingestion_timestamp_utc`.
"""

from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from config.settings import BRONZE_DIR
from src.utils.hashing import (
    compute_payload_checksum,
    generate_batch_id,
    get_utc_now_iso,
    parse_date_components,
)

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "v1.1"  # v1.1 adds `_is_synthetic`; v1.0 files lack it (treat as False)

STATUS_CREATED = "created"          # first version for this partition/date
STATUS_NEW_VERSION = "new_version"  # payload differs from the latest existing version
STATUS_UNCHANGED = "unchanged"      # payload identical to the latest version; nothing written

# domain -> partition key name used in the folder layout
_DOMAIN_PARTITION_KEY = {
    "electricity": "zone",
    "weather": "station",
}


@dataclass(frozen=True)
class WriteResult:
    """Outcome of a Bronze write attempt."""

    path: Path
    status: str
    checksum: str

    @property
    def written(self) -> bool:
        """True if a new file was persisted by this call."""
        return self.status != STATUS_UNCHANGED


class BronzeWriter:
    """Manages writing raw, immutable data into partitioned Bronze storage."""

    def __init__(self, bronze_dir: Optional[Union[str, Path]] = None):
        self.bronze_dir = Path(bronze_dir if bronze_dir is not None else BRONZE_DIR)

    # ------------------------------------------------------------------ #
    # Envelope & layout helpers
    # ------------------------------------------------------------------ #
    def _build_envelope(
        self,
        domain: str,
        partition_keys: Dict[str, str],
        source_uri: str,
        payload: List[Dict[str, Any]],
        batch_id: Optional[str] = None,
        is_synthetic: bool = False,
    ) -> Dict[str, Any]:
        """Construct the immutable metadata audit envelope around raw payload."""
        return {
            "_metadata": {
                "_batch_id": batch_id or generate_batch_id(),
                "_ingestion_timestamp_utc": get_utc_now_iso(),
                "_source_uri": source_uri,
                "_payload_checksum": compute_payload_checksum(payload),
                "_domain": domain,
                "_partition_keys": partition_keys,
                "_record_count": len(payload),
                "_is_synthetic": bool(is_synthetic),
                "_schema_version": SCHEMA_VERSION,
            },
            "payload": payload,
        }

    def _partition_dir(self, domain: str, partition_value: str, year: str, month: str) -> Path:
        key = _DOMAIN_PARTITION_KEY[domain]
        return self.bronze_dir / domain / f"{key}={partition_value}" / f"year={year}" / f"month={month}"

    @staticmethod
    def _version_filename(date_prefix: str, metadata: Dict[str, Any]) -> str:
        """e.g. 2024_01_15__20261015T082112Z_2b8c7998.json"""
        ts_compact = metadata["_ingestion_timestamp_utc"].replace("-", "").replace(":", "")
        batch_short = str(metadata["_batch_id"]).replace("-", "")[:8]
        return f"{date_prefix}__{ts_compact}_{batch_short}.json"

    @staticmethod
    def _read_metadata(path: Path) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("_metadata", {})

    def _list_versions_in(self, partition_dir: Path, date_prefix: str) -> List[Tuple[Path, Dict[str, Any]]]:
        """Return all versions for a date, sorted oldest -> newest."""
        if not partition_dir.exists():
            return []

        candidates = list(partition_dir.glob(f"{date_prefix}.json"))  # legacy layout
        candidates += list(partition_dir.glob(f"{date_prefix}__*.json"))

        versions = []
        for path in candidates:
            try:
                meta = self._read_metadata(path)
            except (OSError, json.JSONDecodeError) as e:
                logger.warning(f"Ignoring unreadable Bronze file {path}: {e}")
                continue
            versions.append((path, meta))

        versions.sort(
            key=lambda pm: (
                pm[1].get("_ingestion_timestamp_utc", ""),
                pm[0].stat().st_mtime_ns,
                pm[0].name,
            )
        )
        return versions

    # ------------------------------------------------------------------ #
    # Write path
    # ------------------------------------------------------------------ #
    @staticmethod
    def _atomic_write_json(target_file: Path, data: Dict[str, Any]) -> Path:
        """Write JSON data to disk atomically to prevent partial reads. Never overwrites."""
        target_file.parent.mkdir(parents=True, exist_ok=True)

        # Guard against a name collision (same batch, same second) without overwriting.
        final = target_file
        n = 1
        while final.exists():
            final = target_file.with_name(f"{target_file.stem}_{n}{target_file.suffix}")
            n += 1

        tmp_file = final.with_name(final.name + ".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp_file.replace(final)
        return final

    def _write_versioned(
        self,
        domain: str,
        partition_value: str,
        date_str: str,
        payload: List[Dict[str, Any]],
        source_uri: str,
        batch_id: Optional[str],
        is_synthetic: bool,
    ) -> WriteResult:
        year, month, day = parse_date_components(date_str)
        date_prefix = f"{year}_{month}_{day}"
        partition_keys = {
            _DOMAIN_PARTITION_KEY[domain]: partition_value,
            "year": year,
            "month": month,
            "day": day,
        }

        envelope = self._build_envelope(
            domain=domain,
            partition_keys=partition_keys,
            source_uri=source_uri,
            payload=payload,
            batch_id=batch_id,
            is_synthetic=is_synthetic,
        )
        new_checksum = envelope["_metadata"]["_payload_checksum"]

        partition_dir = self._partition_dir(domain, partition_value, year, month)
        versions = self._list_versions_in(partition_dir, date_prefix)

        if versions:
            latest_path, latest_meta = versions[-1]
            if latest_meta.get("_payload_checksum") == new_checksum:
                logger.info(f"Unchanged payload (checksum match) -> skipping write: {latest_path}")
                return WriteResult(path=latest_path, status=STATUS_UNCHANGED, checksum=new_checksum)
            status = STATUS_NEW_VERSION
        else:
            status = STATUS_CREATED

        target = partition_dir / self._version_filename(date_prefix, envelope["_metadata"])
        written = self._atomic_write_json(target, envelope)
        logger.info(f"Wrote Bronze payload ({len(payload)} items, {status}) to {written}")
        return WriteResult(path=written, status=status, checksum=new_checksum)

    def write_electricity_prices(
        self,
        zone: str,
        date_str: str,
        payload: List[Dict[str, Any]],
        source_uri: str,
        batch_id: Optional[str] = None,
        is_synthetic: bool = False,
    ) -> WriteResult:
        """
        Write raw hourly electricity spot prices to Bronze storage.
        Path: bronze/electricity/zone={zone}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}__{ts}_{batch}.json
        """
        return self._write_versioned(
            domain="electricity",
            partition_value=zone.upper(),
            date_str=date_str,
            payload=payload,
            source_uri=source_uri,
            batch_id=batch_id,
            is_synthetic=is_synthetic,
        )

    def write_weather_observations(
        self,
        station_id: str,
        date_str: str,
        payload: List[Dict[str, Any]],
        source_uri: str,
        batch_id: Optional[str] = None,
        is_synthetic: bool = False,
    ) -> WriteResult:
        """
        Write raw weather observations to Bronze storage.
        Path: bronze/weather/station={station_id}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}__{ts}_{batch}.json
        """
        return self._write_versioned(
            domain="weather",
            partition_value=station_id,
            date_str=date_str,
            payload=payload,
            source_uri=source_uri,
            batch_id=batch_id,
            is_synthetic=is_synthetic,
        )

    # ------------------------------------------------------------------ #
    # Read path
    # ------------------------------------------------------------------ #
    def list_versions(self, domain: str, partition_value: str, date_str: str) -> List[Path]:
        """List all Bronze versions for a domain/partition/date, oldest -> newest."""
        year, month, day = parse_date_components(date_str)
        partition_dir = self._partition_dir(domain, partition_value, year, month)
        return [p for p, _ in self._list_versions_in(partition_dir, f"{year}_{month}_{day}")]

    @staticmethod
    def read_bronze_file(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Read and validate a Bronze envelope from disk."""
        path = Path(file_path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "_metadata" not in data or "payload" not in data:
            raise ValueError(f"File {path} does not match the Bronze envelope specification.")

        return data

    def write_manifest(self, manifest: Dict[str, Any]) -> Path:
        """Write an execution audit manifest for observability and downstream quality scoring."""
        manifest_dir = self.bronze_dir / "_manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        batch_id = manifest.get("batch_id") or generate_batch_id()
        target_file = manifest_dir / f"run__{batch_id}.json"

        tmp_file = target_file.with_name(target_file.name + ".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        tmp_file.replace(target_file)
        logger.info(f"Persisted ingestion run manifest to {target_file}")
        return target_file

