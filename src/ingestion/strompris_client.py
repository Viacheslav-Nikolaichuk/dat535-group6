"""
Client for the Strømpris API (https://www.hvakosterstrommen.no/strompris-api).
Fetches hourly Norwegian electricity spot prices for bidding zones NO1 through NO5.
"""

import logging
from typing import Any, Dict, List, Optional
import requests

from config.settings import PRICE_ZONES, STROMPRIS_BASE_URL
from src.utils.hashing import parse_date_components
from src.utils.http import build_session

logger = logging.getLogger(__name__)


class StromprisClient:
    """HTTP client for extracting electricity spot prices from hvakosterstrommen.no."""

    def __init__(self, base_url: str = STROMPRIS_BASE_URL, session: Optional[requests.Session] = None):
        self.base_url = base_url.rstrip("/")
        self.session = session or build_session()

    def build_url(self, date_str: str, zone: str) -> str:
        """
        Build API endpoint URL for given date and zone.
        Format: https://www.hvakosterstrommen.no/api/v1/prices/{year}/{month}-{day}_{zone}.json
        """
        year, month, day = parse_date_components(date_str)
        return f"{self.base_url}/{year}/{month}-{day}_{zone}.json"

    def fetch_prices(self, date_str: str, zone: str, timeout: int = 15) -> Dict[str, Any]:
        """
        Extract 24 hourly spot price records for a specific date and zone.

        Args:
            date_str: Date formatted as 'YYYY-MM-DD'.
            zone: Price zone code ('NO1', 'NO2', 'NO3', 'NO4', 'NO5').
            timeout: HTTP request timeout in seconds.

        Returns:
            Dictionary containing metadata, raw record list, and source URI.
        """
        zone_norm = zone.upper()
        if zone_norm not in PRICE_ZONES:
            raise ValueError(f"Invalid bidding zone '{zone}'. Must be one of {PRICE_ZONES}")

        url = self.build_url(date_str, zone_norm)
        logger.info(f"Fetching electricity prices for {zone_norm} on {date_str} from {url}")

        try:
            response = self.session.get(url, timeout=timeout)
        except requests.RequestException as e:
            logger.error(f"Network error querying Strømpris API ({url}): {e}")
            raise

        if response.status_code == 404:
            raise FileNotFoundError(
                f"Electricity prices for date {date_str} and zone {zone_norm} were not found (404). "
                "Prices for tomorrow are published around 13:00 CET daily."
            )

        response.raise_for_status()
        data: List[Dict[str, Any]] = response.json()

        if not isinstance(data, list):
            raise ValueError(f"Unexpected response payload type: expected list, got {type(data)}")

        return {
            "zone": zone_norm,
            "date": date_str,
            "record_count": len(data),
            "source_uri": url,
            "records": data
        }
