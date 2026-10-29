"""
Project configuration and environment settings for DAT535.
Loads environment variables from .env if present and sets canonical
paths for Bronze, Silver, and Gold storage tiers, separated per environment (dev/prod).
"""

import os
from pathlib import Path
from typing import Mapping, Tuple


def _load_env_file(project_root: Path) -> None:
    """Manually parse .env file to support key=val or key:val formats."""
    env_file = project_root / ".env"
    if not env_file.exists():
        return

    try:
        content = env_file.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = env_file.read_text(encoding="latin-1")

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        separator = "=" if "=" in line else (":" if ":" in line else None)
        if separator:
            key, val = line.split(separator, 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            # Set in os.environ if not already defined
            if key not in os.environ:
                os.environ[key] = val
            # Also normalize uppercase variants
            upper_key = key.upper()
            if upper_key not in os.environ:
                os.environ[upper_key] = val


# Project root path
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load local .env if available
_load_env_file(PROJECT_ROOT)

# Deployment environments. Each one gets its own data tree under the data root,
# so dev runs can never overwrite prod data. Only main's workflows set "prod".
ENVIRONMENTS = ("dev", "prod")
DEFAULT_ENVIRONMENT = "dev"


def resolve_environment(environ: Mapping[str, str]) -> Tuple[str, Path, Path]:
    """Return (env, data_root, data_dir) from DAT535_ENV and DAT535_DATA_DIR.

    DAT535_ENV defaults to "dev", so prod must always be requested explicitly.
    DAT535_DATA_DIR is the data root; the environment name is appended to it.
    """
    env = environ.get("DAT535_ENV", DEFAULT_ENVIRONMENT).strip().lower()
    if env not in ENVIRONMENTS:
        raise ValueError(f"DAT535_ENV must be one of {ENVIRONMENTS}, got {env!r}")

    default_root = Path.home() / "spark-project-data" if os.name != "nt" else PROJECT_ROOT / "data"
    data_root = Path(environ.get("DAT535_DATA_DIR") or default_root)
    return env, data_root, data_root / env


# Medallion data directory: <data root>/<env>/{bronze,bronze_chaos,silver,gold}
DAT535_ENV, DATA_ROOT, DATA_DIR = resolve_environment(os.environ)
BRONZE_DIR = DATA_DIR / "bronze"
BRONZE_CHAOS_DIR = DATA_DIR / "bronze_chaos"  # Bronze copy with injected errors (never the source of truth)
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Which Bronze tree Silver cleans. The chaos copy is the default, so cleaning is exercised and
# scored on every run; "bronze" gives the clean baseline (docs/plans/silver_layer.md, D1).
SILVER_INPUTS = ("bronze_chaos", "bronze")
SILVER_INPUT = os.environ.get("SILVER_INPUT", "bronze_chaos").strip().lower()

# First day with day-ahead prices for all five zones (NO4 has none in October 2021).
HISTORY_START = "2021-11-01"

# MET Norway Frost API Configuration
FROST_BASE_URL = os.environ.get("FROST_BASE_URL", "https://frost.met.no")
FROST_CLIENT_ID = (
    os.environ.get("FROST_CLIENT_ID")
    or os.environ.get("CLIENT_ID")
    or os.environ.get("client_id", "")
)
FROST_CLIENT_SECRET = (
    os.environ.get("FROST_CLIENT_SECRET")
    or os.environ.get("CLIENT_SECRET")
    or os.environ.get("client_secret", "")
)

# Strømpris API Configuration (Hva Koster Strømmen)
STROMPRIS_BASE_URL = os.environ.get(
    "STROMPRIS_BASE_URL",
    "https://www.hvakosterstrommen.no/api/v1/prices"
)

# Norwegian Electricity Bidding Zones
PRICE_ZONES = ["NO1", "NO2", "NO3", "NO4", "NO5"]

# Config File Paths
ZONES_CONFIG_FILE = PROJECT_ROOT / "config" / "zones_and_stations.json"
