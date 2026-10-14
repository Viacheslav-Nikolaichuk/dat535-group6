"""
Bronze layer persistence engine.
Wraps raw API payloads with immutable audit metadata envelopes and writes
partitioned JSON files into Bronze storage as defined in Section 3 of the project definition.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from config.settings import BRONZE_DIR
from src.utils.hashing import (
    compute_payload_checksum,
    generate_batch_id,
    get_utc_now_iso,
    parse_date_components,
)

logger = logging.getLogger(__name__)


class BronzeWriter:
    """Manages writing raw, immutable data into partitioned Bronze storage."""

    def __init__(self, bronze_dir: Optional[Union[str, Path]] = None):
        self.bronze_dir = Path(bronze_dir if bronze_dir is not None else BRONZE_DIR)

    def _build_envelope(
        self,
        domain: str,
        partition_keys: Dict[str, str],
        source_uri: str,
        payload: List[Dict[str, Any]],
        batch_id: Optional[str] = None,
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
                "_schema_version": "v1.0",
            },
            "payload": payload,
        }

    def _atomic_write_json(self, target_file: Path, data: Dict[str, Any], overwrite: bool) -> Path:
        """Write JSON data to disk atomically to prevent partial reads."""
        target_file.parent.mkdir(parents=True, exist_ok=True)

        if target_file.exists() and not overwrite:
            logger.info(f"Bronze file already exists (skipping overwrite): {target_file}")
            return target_file

        tmp_file = target_file.with_suffix(".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        tmp_file.replace(target_file)
        logger.info(f"Wrote Bronze payload ({len(data['payload'])} items) to {target_file}")
        return target_file

    def write_electricity_prices(
        self,
        zone: str,
        date_str: str,
        payload: List[Dict[str, Any]],
        source_uri: str,
        batch_id: Optional[str] = None,
        overwrite: bool = True,
    ) -> Path:
        """
        Write raw hourly electricity spot prices to Bronze storage.
        Path: bronze/electricity/zone={zone}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}.json
        """
        year, month, day = parse_date_components(date_str)
        partition_keys = {"zone": zone.upper(), "year": year, "month": month, "day": day}

        envelope = self._build_envelope(
            domain="electricity",
            partition_keys=partition_keys,
            source_uri=source_uri,
            payload=payload,
            batch_id=batch_id,
        )

        partition_dir = (
            self.bronze_dir
            / "electricity"
            / f"zone={zone.upper()}"
            / f"year={year}"
            / f"month={month}"
        )
        target_file = partition_dir / f"{year}_{month}_{day}.json"

        return self._atomic_write_json(target_file, envelope, overwrite=overwrite)

    def write_weather_observations(
        self,
        station_id: str,
        date_str: str,
        payload: List[Dict[str, Any]],
        source_uri: str,
        batch_id: Optional[str] = None,
        overwrite: bool = True,
    ) -> Path:
        """
        Write raw weather observations to Bronze storage.
        Path: bronze/weather/station={station_id}/year={YYYY}/month={MM}/{YYYY}_{MM}_{DD}.json
        """
        year, month, day = parse_date_components(date_str)
        partition_keys = {"station": station_id, "year": year, "month": month, "day": day}

        envelope = self._build_envelope(
            domain="weather",
            partition_keys=partition_keys,
            source_uri=source_uri,
            payload=payload,
            batch_id=batch_id,
        )

        partition_dir = (
            self.bronze_dir
            / "weather"
            / f"station={station_id}"
            / f"year={year}"
            / f"month={month}"
        )
        target_file = partition_dir / f"{year}_{month}_{day}.json"

        return self._atomic_write_json(target_file, envelope, overwrite=overwrite)

    @staticmethod
    def read_bronze_file(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Read and validate a Bronze envelope from disk."""
        path = Path(file_path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "_metadata" not in data or "payload" not in data:
            raise ValueError(f"File {path} does not match the Bronze envelope specification.")

        return data
