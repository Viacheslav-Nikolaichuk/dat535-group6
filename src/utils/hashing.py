"""
Hashing, batch identification, and audit metadata helper functions.
Ensures idempotency, tamper evidence, and metadata wrapping required
by Section 3 (Bronze Layer) of the project definition.
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Tuple


def generate_batch_id() -> str:
    """Generate a unique UUID4 string for ingestion batch traceability."""
    return str(uuid.uuid4())


def get_utc_now_iso() -> str:
    """Return the current UTC timestamp formatted as ISO-8601 with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_payload_checksum(payload: Any) -> str:
    """Compute SHA-256 hex digest for an input string or JSON-serializable object."""
    if isinstance(payload, str):
        data_bytes = payload.encode("utf-8")
    else:
        data_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(data_bytes).hexdigest()


def parse_date_components(date_str: str) -> Tuple[str, str, str]:
    """
    Parse a YYYY-MM-DD date string into (year, month, day) string tokens.
    
    Raises:
        ValueError: If date_str is not in valid YYYY-MM-DD format.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
