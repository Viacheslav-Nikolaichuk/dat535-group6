"""
The Gold tables behind the API, and every answer its endpoints give. Plain pandas, no FastAPI: api/main.py
only turns an Answer into an HTTP response, and the dashboard's API page (dashboard/views/api_docs.py)
calls the same functions, so the page always shows exactly what the API answers.

Reads the use case 1 tables consumer_prices_hourly and cheapest_hours with the dashboard's readers
(dashboard/data.py), from <data root>/<env>/gold or DAT535_GOLD_DIR.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config.settings import PRICE_ZONES
from dashboard import data

TABLES = ("consumer_prices_hourly", "cheapest_hours")
TIMEZONE = "Europe/Oslo"
UNITS = "NOK/kWh"
BLOCK_HOURS = (1, 3, 6)  # as src/gold/cheapest_hours.BLOCK_HOURS (importing it would need Spark)
RETRY_AFTER_SECONDS = 1800  # the day-ahead run tries every 30 minutes
DAY_WORDS = ("today", "tomorrow")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class GoldTables:
    """The newest Gold's use case 1 tables, kept in memory.

    Each request reads only the small status file. The tables are read again when a new Gold run has been
    published (its built_at_utc changes), so the API follows the nightly and afternoon runs without a restart.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._key: Optional[Tuple[str, Optional[str]]] = None
        self._tables: Optional[Dict[str, pd.DataFrame]] = None

    def get(self) -> Tuple[Optional[Dict], Optional[Dict[str, pd.DataFrame]]]:
        """(status, tables), or (None, None) before the first Gold run."""
        gold = data.gold_dir()
        status = data.read_status(gold)
        if status is None:
            return None, None
        key = (str(gold), status.get("built_at_utc"))
        with self._lock:
            if key != self._key:
                self._tables = {name: data.read_table(gold / name) for name in TABLES}
                self._key = key
            return status, self._tables


def _iso_utc(value) -> str:
    ts = pd.Timestamp(value)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _number(value) -> Optional[float]:
    return None if pd.isna(value) else round(float(value), 5)


def block_document(block) -> Dict:
    """One cheapest block: when it starts and ends, and what it costs and saves per kWh."""
    return {
        "block_hours": int(block["block_hours"]),
        "start_utc": _iso_utc(block["start_utc"]),
        "end_utc": _iso_utc(block["end_utc"]),
        "start_time": block["start_time"],
        "end_time": block["end_time"],
        "avg_price_nok_kwh": _number(block["avg_price_nok_kwh"]),
        "evening_avg_price_nok_kwh": _number(block["evening_avg_price_nok_kwh"]),
        "saving_vs_17_nok_kwh": _number(block["saving_nok_kwh"]),
    }


def day_document(tables: Dict[str, pd.DataFrame], zone: str, day: date) -> Optional[Dict]:
    """A zone's day: every hour's prices and the cheapest 1-, 3- and 6-hour blocks.

    None when Gold doesn't have the whole day (not published yet, or outside the history).
    """
    blocks = data.blocks_for(tables["cheapest_hours"], zone, day)
    if blocks.empty:
        return None
    consumer = tables["consumer_prices_hourly"]
    rows = consumer[(consumer["bidding_zone"] == zone) & (consumer["local_date"].dt.date == day)]
    rows = rows.sort_values("utc_hour")
    hours: List[Dict] = [
        {
            "start_utc": _iso_utc(r.utc_hour),
            "end_utc": _iso_utc(pd.Timestamp(r.utc_hour) + pd.Timedelta(hours=1)),
            "local_time": f"{r.local_hour:02d}:00",
            "spot_nok_kwh": _number(r.spot_nok_kwh),
            "price_with_vat_nok_kwh": _number(r.price_with_vat_nok_kwh),
            "price_with_stromstotte_nok_kwh": _number(r.price_with_stromstotte_nok_kwh),
            "norgespris_nok_kwh": _number(r.norgespris_nok_kwh),
        }
        for r in rows.itertuples()
    ]
    return {
        "zone": zone,
        "date": day.isoformat(),
        "timezone": TIMEZONE,
        "units": UNITS,
        "hours": hours,
        "cheapest": [block_document(b) for _, b in blocks.iterrows()],
    }


# ---------------------------------------------------------------------- #
# The endpoints' answers
# ---------------------------------------------------------------------- #
@dataclass
class Answer:
    """What an endpoint answers: an HTTP status, a JSON body, and any extra headers."""

    status: int
    body: Any
    headers: Dict[str, str] = field(default_factory=dict)


def _today_tomorrow(now_utc: datetime) -> Tuple[date, date]:
    today = data.oslo_today(now_utc)
    return today, today + timedelta(days=1)


def _check(zone: str, day_text: str, now_utc: datetime) -> Tuple[Optional[Answer], Optional[date]]:
    """The requested zone and day, or a 422 answer saying what's allowed."""
    if zone not in PRICE_ZONES:
        return Answer(422, {"detail": f"Unknown zone {zone!r}. Use one of {', '.join(PRICE_ZONES)}."}), None
    today, tomorrow = _today_tomorrow(now_utc)
    if day_text in DAY_WORDS:
        return None, today if day_text == "today" else tomorrow
    try:
        if ISO_DATE.match(day_text):
            return None, date.fromisoformat(day_text)
    except ValueError:
        pass
    return Answer(422, {"detail": f"Unknown day {day_text!r}. Use today, tomorrow or a date like 2026-11-04."}), None


def _missing(zone: str, day: date, now_utc: datetime) -> Answer:
    """Tomorrow before publication: 503 and when to retry. Any other day Gold doesn't have: 404."""
    _, tomorrow = _today_tomorrow(now_utc)
    if day != tomorrow:
        return Answer(404, {"detail": f"No complete prices for {zone} on {day.isoformat()}.",
                            "zone": zone, "date": day.isoformat()})
    late = data.tomorrow_is_late(now_utc)
    return Answer(503, {
        "detail": (f"Tomorrow's prices for {zone} are late: they should have been here by "
                   f"{data.TOMORROW_READY_BY:%H:%M}." if late else
                   f"Tomorrow's prices for {zone} are not published yet; they usually arrive around "
                   f"{data.TOMORROW_PUBLISHED_AROUND}."),
        "zone": zone,
        "date": day.isoformat(),
        "published": False,
        "expected_by": f"{data.TOMORROW_READY_BY:%H:%M} {TIMEZONE}",
    }, {"Retry-After": str(RETRY_AFTER_SECONDS)})


def day_answer(tables: Dict[str, pd.DataFrame], zone: str, day_text: str, now_utc: datetime) -> Answer:
    """GET /api/v1/cheapest-hours/{zone}?day=...: every hour and the cheapest blocks."""
    problem, day = _check(zone, day_text, now_utc)
    if problem:
        return problem
    document = day_document(tables, zone, day)
    return Answer(200, document) if document is not None else _missing(zone, day, now_utc)


def block_answer(tables: Dict[str, pd.DataFrame], zone: str, hours: int, day_text: str,
                 now_utc: datetime) -> Answer:
    """GET /api/v1/cheapest-hours/{zone}/block?hours=...&day=...: only the cheapest block of that length."""
    if hours not in BLOCK_HOURS:
        return Answer(422, {"detail": f"hours must be one of {', '.join(map(str, BLOCK_HOURS))}, not {hours}."})
    problem, day = _check(zone, day_text, now_utc)
    if problem:
        return problem
    blocks = data.blocks_for(tables["cheapest_hours"], zone, day)
    chosen = blocks[blocks["block_hours"] == hours]
    if chosen.empty:
        return _missing(zone, day, now_utc)
    return Answer(200, {"zone": zone, "date": day.isoformat(), "timezone": TIMEZONE,
                        **block_document(chosen.iloc[0])})


def health_answer(status: Dict, tables: Dict[str, pd.DataFrame], now_utc: datetime) -> Answer:
    """GET /api/v1/health: when Gold was built, how far it reaches, and which zones have tomorrow."""
    _, tomorrow = _today_tomorrow(now_utc)
    fresh = status.get("freshness") or {}
    return Answer(200, {
        "status": "ok",
        "environment": status.get("environment"),
        "gold_built_at_utc": status.get("built_at_utc"),
        "prices_through": fresh.get("prices_through"),
        "data_through": fresh.get("data_through"),
        "tomorrow": {"date": tomorrow.isoformat(),
                     "published_zones": data.published_zones(tables["cheapest_hours"], tomorrow)},
        "is_sample": bool(status.get("is_sample")),
    })


def zones_answer() -> Answer:
    """GET /api/v1/zones: the bidding zones and their names."""
    names = data.zone_names()
    return Answer(200, [{"zone": zone, "name": names[zone]} for zone in PRICE_ZONES])
