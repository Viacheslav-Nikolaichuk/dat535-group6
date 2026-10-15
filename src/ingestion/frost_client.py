"""
Client for the MET Norway Frost API (https://frost.met.no/).
Fetches meteorological observations (temperature, wind speed) from official weather stations.
"""

from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List, Optional
import requests

from config.settings import FROST_BASE_URL, FROST_CLIENT_ID
from src.utils.http import build_session

logger = logging.getLogger(__name__)


class FrostClient:
    """HTTP client for extracting weather observations from MET Norway Frost API."""

    def __init__(
        self,
        client_id: Optional[str] = None,
        base_url: str = FROST_BASE_URL,
        session: Optional[requests.Session] = None,
    ):
        self.client_id = (client_id if client_id is not None else FROST_CLIENT_ID).strip()
        self.base_url = base_url.rstrip("/")
        self.session = session or build_session()

    def fetch_observations(
        self,
        date_str: str,
        station_id: str,
        elements: Optional[List[str]] = None,
        timeout: int = 25,
        allow_mock_fallback: bool = False,
    ) -> Dict[str, Any]:
        """
        Extract weather observations for a 24-hour UTC window [date_str, date_str + 1 day).

        Args:
            date_str: Date formatted as 'YYYY-MM-DD'.
            station_id: MET station identifier (e.g. 'SN18700').
            elements: List of weather elements (default: ['air_temperature', 'wind_speed']).
            timeout: Request timeout in seconds.
            allow_mock_fallback: If True and client_id is missing, returns synthetic records
                flagged with is_synthetic=True. Must be explicitly opted into by the caller;
                never enable this implicitly in production.

        Returns:
            Dictionary containing metadata, raw observations, source URI, and is_synthetic flag.
        """
        if elements is None:
            elements = ["air_temperature", "wind_speed"]

        if not self.client_id:
            msg = (
                "MET Norway Frost API requires a Client ID. "
                "Register at https://frost.met.no/auth/requestCredentials.html and "
                "set FROST_CLIENT_ID in your .env file or environment."
            )
            if allow_mock_fallback:
                logger.warning(f"{msg} Generating mock observations for testing.")
                return self._generate_mock_observations(date_str, station_id, elements)
            raise ValueError(msg)

        # Calculate referencetime interval: [date_str, next_day)
        start_dt = datetime.strptime(date_str, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=1)
        referencetime = f"{start_dt.strftime('%Y-%m-%d')}/{end_dt.strftime('%Y-%m-%d')}"

        url = f"{self.base_url}/observations/v0.jsonld"
        params = {
            "sources": station_id,
            "referencetime": referencetime,
            "elements": ",".join(elements),
        }

        logger.info(
            f"Fetching Frost observations for station {station_id} on {date_str} (interval {referencetime})"
        )

        try:
            # Frost uses HTTP Basic Auth with client_id as username and empty password
            response = self.session.get(
                url,
                params=params,
                auth=(self.client_id, ""),
                timeout=timeout,
            )
        except requests.RequestException as e:
            logger.error(f"Network error querying Frost API ({url}): {e}")
            raise

        if response.status_code == 401:
            raise PermissionError(
                "Frost API authentication failed (401 Unauthorized). "
                "Please verify your FROST_CLIENT_ID credentials."
            )

        if response.status_code == 404:
            logger.warning(
                f"No Frost observations found for station {station_id} on {date_str} (404 Not Found)."
            )
            return {
                "station_id": station_id,
                "date": date_str,
                "elements": elements,
                "record_count": 0,
                "source_uri": response.url,
                "records": [],
                "is_synthetic": False,
            }

        response.raise_for_status()
        payload = response.json()
        records: List[Dict[str, Any]] = payload.get("data", [])

        return {
            "station_id": station_id,
            "date": date_str,
            "elements": elements,
            "record_count": len(records),
            "source_uri": response.url,
            "records": records,
            "is_synthetic": False,
        }

    def _generate_mock_observations(
        self, date_str: str, station_id: str, elements: List[str]
    ) -> Dict[str, Any]:
        """Generate deterministic synthetic observation records for offline testing."""
        records = []
        for hour in range(24):
            obs_list = []
            if "air_temperature" in elements:
                obs_list.append({
                    "elementId": "air_temperature",
                    "value": round(2.0 + (hour % 5) - 2.5, 1),
                    "unit": "degC",
                    "timeResolution": "PT1H",
                    "qualityCode": 0,
                })
            if "wind_speed" in elements:
                obs_list.append({
                    "elementId": "wind_speed",
                    "value": round(4.0 + (hour % 3), 1),
                    "unit": "m/s",
                    "timeResolution": "PT1H",
                    "qualityCode": 0,
                })

            records.append({
                "sourceId": f"{station_id}:0",
                "referenceTime": f"{date_str}T{hour:02d}:00:00.000Z",
                "observations": obs_list,
            })

        return {
            "station_id": station_id,
            "date": date_str,
            "elements": elements,
            "record_count": len(records),
            "source_uri": f"{self.base_url}/observations/mock?station={station_id}&date={date_str}",
            "records": records,
            "is_synthetic": True,
        }
