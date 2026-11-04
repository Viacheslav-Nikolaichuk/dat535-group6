"""
HTTP API for use case 1, cheapest hours (docs/plans/use_cases.md, section 5.1; guide: docs/api.md).

Apps, home automation and smart chargers ask it when electricity is cheapest, instead of reading the
dashboard. An open service: no key, read-only, JSON only. It serves the Gold tables consumer_prices_hourly
and cheapest_hours, for tomorrow (after the afternoon day-ahead run), today, or any day in Gold.

    GET /                                                this list, as JSON
    GET /api/v1/cheapest-hours/{zone}/block?hours=6      one block: when to start and stop
    GET /api/v1/cheapest-hours/{zone}?day=tomorrow       every hour's price and the cheapest 1/3/6-hour blocks
    GET /api/v1/health                                   Gold's age, and which zones have tomorrow
    GET /api/v1/zones                                    the bidding zones

Before tomorrow is published (~13:00 Norwegian time), tomorrow answers 503 with a Retry-After header.
The answers themselves are built in api/gold.py; the documentation for people is the dashboard's
"API for apps" page, which calls the same functions.

Usage (from the repository root):
    uvicorn api.main:app --host 127.0.0.1 --port 8000
"""

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from api.gold import (
    RETRY_AFTER_SECONDS,
    Answer,
    GoldTables,
    block_answer,
    day_answer,
    health_answer,
    zones_answer,
)

ENDPOINTS = {
    "/api/v1/cheapest-hours/{zone}/block?hours=6&day=tomorrow": "The cheapest block of 1, 3 or 6 hours: when to start and stop",
    "/api/v1/cheapest-hours/{zone}?day=tomorrow": "Every hour's price and the cheapest 1-, 3- and 6-hour blocks",
    "/api/v1/health": "When Gold was built, and which zones already have tomorrow",
    "/api/v1/zones": "The bidding zones NO1-NO5 and their names",
}


def now_utc() -> datetime:
    """The clock the API uses (replaced in tests)."""
    return datetime.now(timezone.utc)


# No generated docs: people read the dashboard's "API for apps" page instead.
app = FastAPI(title="DAT535 cheapest hours API", version="1", docs_url=None, redoc_url=None, openapi_url=None)
gold = GoldTables()


def _respond(answer: Answer) -> JSONResponse:
    return JSONResponse(answer.body, status_code=answer.status, headers=answer.headers)


def _gold():
    """(status, tables), or a 503 answer while Gold isn't there or is being swapped."""
    try:
        status, tables = gold.get()
    except Exception:  # a read during the swap of a new Gold run
        return None, None, Answer(503, {"detail": "Gold is being updated; try again in a minute."}, {"Retry-After": "60"})
    if status is None:
        return None, None, Answer(503, {"detail": "Gold has not been built yet."},
                                  {"Retry-After": str(RETRY_AFTER_SECONDS)})
    return status, tables, None


@app.get("/")
def index():
    return {
        "name": "DAT535 cheapest hours API",
        "about": "When electricity is cheapest in each Norwegian bidding zone, as a household pays for it. "
                 "Prices in NOK/kWh; day is today, tomorrow or YYYY-MM-DD (Norwegian dates).",
        "endpoints": ENDPOINTS,
        "documentation": "The dashboard's 'API for apps' page, and docs/api.md in the project repository",
    }


@app.get("/api/v1/cheapest-hours/{zone}/block")
def cheapest_block(zone: str, hours: int = 3, day: str = "tomorrow"):
    _, tables, problem = _gold()
    return _respond(problem or block_answer(tables, zone, hours, day, now_utc()))


@app.get("/api/v1/cheapest-hours/{zone}")
def cheapest_hours(zone: str, day: str = "tomorrow"):
    _, tables, problem = _gold()
    return _respond(problem or day_answer(tables, zone, day, now_utc()))


@app.get("/api/v1/health")
def health():
    status, tables, problem = _gold()
    return _respond(problem or health_answer(status, tables, now_utc()))


@app.get("/api/v1/zones")
def zones():
    return _respond(zones_answer())
