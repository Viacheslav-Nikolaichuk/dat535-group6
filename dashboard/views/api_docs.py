"""
The documentation page for the cheapest hours API (api/main.py): what it offers, a try-it panel, the
status codes and fields, and an example for a smart charger.

The try-it panel calls the same functions as the API (api/gold.py) on the same Gold tables, so it shows
exactly what the API answers, also when the API itself isn't running where the dashboard runs.
"""

from datetime import datetime, timezone
import os

import streamlit as st

from api.gold import BLOCK_HOURS, RETRY_AFTER_SECONDS, block_answer, day_answer, health_answer, zones_answer
from config.settings import PRICE_ZONES
from dashboard import data, ui
from dashboard.registry import USE_CASES

USE_CASE = next(uc for uc in USE_CASES if uc.key == "api")
TABLES = ["consumer_prices_hourly", "cheapest_hours"]

# Where the API answers: through the SSH tunnel by default (docs/api.md, section 2).
API_URL = os.environ.get("DAT535_API_URL", "http://localhost:8000")

ENDPOINT_BLOCK, ENDPOINT_DAY, ENDPOINT_HEALTH, ENDPOINT_ZONES = "Cheapest block", "Whole day", "Health", "Zones"
STATUS_TEXT = {200: "OK", 404: "Not Found", 422: "Unprocessable Content", 503: "Service Unavailable"}
STATUS_COLOR = {200: "green", 404: "gray", 422: "red", 503: "orange"}

CHARGER_EXAMPLE = '''import time
import requests

URL = "{api}/api/v1/cheapest-hours/NO1/block"

while True:
    response = requests.get(URL, params={{"hours": 6, "day": "tomorrow"}}, timeout=10)
    if response.status_code == 503:  # tomorrow isn't published yet
        time.sleep(int(response.headers.get("Retry-After", {retry})))
        continue
    response.raise_for_status()
    block = response.json()
    print(f"Charge from {{block['start_utc']}} to {{block['end_utc']}}")
    break
'''


def _endpoints() -> None:
    st.markdown(
        "| Endpoint | What it answers |\n|---|---|\n"
        "| `GET /api/v1/cheapest-hours/{zone}/block?hours=6&day=tomorrow` | The cheapest block of 1, 3 or 6 hours "
        "(default 3): when to start and stop. **The one a charger needs.** |\n"
        "| `GET /api/v1/cheapest-hours/{zone}?day=tomorrow` | Every hour of the day with its prices, and the "
        "cheapest 1-, 3- and 6-hour blocks |\n"
        "| `GET /api/v1/health` | When Gold was built, and which zones already have tomorrow |\n"
        "| `GET /api/v1/zones` | The zones `NO1`–`NO5` and their names |\n"
    )
    st.caption("`day` is `today`, `tomorrow` (the default) or a date like `2026-11-04`, on the Norwegian calendar. "
               "Prices are in NOK/kWh.")


def _try_it(tables, status) -> None:
    names = data.zone_names()
    endpoint_col, zone_col, day_col, hours_col = st.columns([2.6, 1.6, 1.8, 1.2])
    with endpoint_col:
        endpoint = st.pills("Endpoint", [ENDPOINT_BLOCK, ENDPOINT_DAY, ENDPOINT_HEALTH, ENDPOINT_ZONES],
                            default=ENDPOINT_BLOCK) or ENDPOINT_BLOCK
    needs_zone = endpoint in (ENDPOINT_BLOCK, ENDPOINT_DAY)
    with zone_col:
        zone = st.selectbox("zone", PRICE_ZONES, format_func=lambda z: f"{z} {names[z]}", disabled=not needs_zone)
    with day_col:
        day_choice = st.segmented_control("day", ["tomorrow", "today", "a date"], default="tomorrow",
                                          disabled=not needs_zone) or "tomorrow"
        if day_choice == "a date" and needs_zone:
            day = st.date_input("Date", value=data.oslo_today(datetime.now(timezone.utc)),
                                label_visibility="collapsed").isoformat()
        else:
            day = day_choice if day_choice != "a date" else "tomorrow"
    with hours_col:
        hours = st.segmented_control("hours", list(BLOCK_HOURS), default=6,
                                     disabled=endpoint != ENDPOINT_BLOCK) or 6

    now = datetime.now(timezone.utc)
    if endpoint == ENDPOINT_BLOCK:
        path, answer = f"/api/v1/cheapest-hours/{zone}/block?hours={hours}&day={day}", block_answer(tables, zone, hours, day, now)
    elif endpoint == ENDPOINT_DAY:
        path, answer = f"/api/v1/cheapest-hours/{zone}?day={day}", day_answer(tables, zone, day, now)
    elif endpoint == ENDPOINT_HEALTH:
        path, answer = "/api/v1/health", health_answer(status, tables, now)
    else:
        path, answer = "/api/v1/zones", zones_answer()

    st.code(f'curl "{API_URL}{path}"', language="bash")
    headers = "".join(f" · `{k}: {v}`" for k, v in answer.headers.items())
    st.markdown(f":{STATUS_COLOR.get(answer.status, 'gray')}-badge[{answer.status} "
                f"{STATUS_TEXT.get(answer.status, '')}]{headers}")
    st.json(answer.body, expanded=1 if endpoint == ENDPOINT_DAY else 2)


def render() -> None:
    st.title(USE_CASE.title)
    st.markdown(
        "The answers of the *Cheapest hours tomorrow* page as JSON, for a car charger, a home-automation system "
        "or any script. It is open (no key) and read-only, and it follows Gold: tomorrow's prices are here by "
        f"{data.TOMORROW_READY_BY:%H:%M}. Address: `{API_URL}`."
    )

    st.subheader("Endpoints")
    _endpoints()

    st.subheader("Try it")
    tables = ui.load_tables(TABLES)
    if tables is not None:
        st.caption("Built here by the same functions as the API, on the same Gold, so this is exactly what the "
                   "API answers right now.")
        _try_it(tables, data.read_status(data.gold_dir()))

    st.subheader("Status codes")
    st.markdown(
        "| Code | When | What to do |\n|---|---|---|\n"
        "| `200` | The day is there | Use it |\n"
        f"| `503` | `day=tomorrow` before tomorrow's prices are here (they arrive around "
        f"{data.TOMORROW_PUBLISHED_AROUND}; after {data.TOMORROW_READY_BY:%H:%M} the answer says they are late). "
        f"Also while Gold is being built. | Wait the seconds in the `Retry-After` header ({RETRY_AFTER_SECONDS}), "
        "then ask again |\n"
        "| `404` | Any other day without prices: before 2024, or after tomorrow | Don't retry |\n"
        "| `422` | An unknown zone, day or block length; `detail` says what's allowed | Fix the request |\n"
    )

    st.subheader("A smart charger")
    st.markdown("Ask after 13:00 for tomorrow's cheapest 6 hours, wait while the answer is `503`, then charge "
                "from `start_utc` to `end_utc`:")
    st.code(CHARGER_EXAMPLE.format(api=API_URL, retry=RETRY_AFTER_SECONDS), language="python")

    with st.expander("Fields"):
        st.markdown(
            "| Field | Meaning |\n|---|---|\n"
            "| `start_utc`, `end_utc` | The hour or block in UTC. Schedule devices with these: they are "
            "unambiguous, also on daylight saving days. |\n"
            "| `local_time`, `start_time`, `end_time` | The same on the Norwegian clock, for people |\n"
            "| `spot_nok_kwh` | The day-ahead spot price, without VAT |\n"
            "| `price_with_vat_nok_kwh` | Spot plus VAT (25 %; NO4 none): no support |\n"
            "| `price_with_stromstotte_nok_kwh` | After strømstønad: what a household without Norgespris pays. "
            "Blocks are ranked by it. |\n"
            "| `norgespris_nok_kwh` | The fixed Norgespris price with VAT; every hour costs the same |\n"
            "| `block_hours` | The block's length: 1, 3 or 6 hours, within one Norwegian day |\n"
            "| `avg_price_nok_kwh` | The block's average price after strømstønad |\n"
            "| `evening_avg_price_nok_kwh` | The same length of block starting at 17:00 |\n"
            "| `saving_vs_17_nok_kwh` | The evening block minus the cheapest block, per kWh moved |\n"
        )
        st.caption("Prices are for energy only; the grid tariff and the supplier's fees come on top.")

    st.caption("The full guide, with how to run the API on the VM and a Home Assistant example, is docs/api.md "
               "in the repository.")
