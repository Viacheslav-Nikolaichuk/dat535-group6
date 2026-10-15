"""
Shared HTTP session factory with retry/backoff for all API clients.
Transient failures (timeouts, connection resets, 429 and 5xx responses) are retried
with exponential backoff so a single network hiccup does not lose a day of data.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "DAT535-group6-ingestion/1.0 (University of Stavanger student project)"

RETRY_STATUS_CODES = (429, 500, 502, 503, 504)


def build_session(
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    user_agent: str = USER_AGENT,
) -> requests.Session:
    """
    Create a requests.Session that retries idempotent GETs on transient errors.

    Backoff sleeps roughly backoff_factor * 2^(n-1) seconds between attempts
    (1s, 2s, 4s, 8s, ...) and honours Retry-After headers on 429/503.
    After retries are exhausted the last response is returned (not raised), so callers
    keep their own status-code handling via response.raise_for_status().
    """
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session
