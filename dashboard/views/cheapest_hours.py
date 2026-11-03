"""
Use case 1, application: cheapest hours tomorrow (docs/plans/use_cases.md, section 5.1).

"When should I charge the car or run the dishwasher tomorrow, and what will it cost?"
  * Tomorrow's hours as a household pays them, the cheapest 1-, 3- and 6-hour blocks, and what moving
    usage there saves compared with starting at 17:00.
  * Before tomorrow is published (~13:00) the page says so and shows today; after the 14:30 deadline it
    warns that tomorrow is late.
  * The same answer for machines comes from the API (api/main.py); the page links to its documentation page.

Reads the Gold tables consumer_prices_hourly and cheapest_hours.
"""

from datetime import datetime, timedelta, timezone

import streamlit as st

from config.price_rules import (
    NO_VAT_ZONES,
    NORGESPRIS_FROM,
    NORGESPRIS_NOK,
    NORGESPRIS_TO,
    SCHEME_NORGESPRIS,
    SCHEME_STROMSTOTTE,
    STROMSTOTTE_SHARE,
    STROMSTOTTE_THRESHOLD_NOK,
    VAT,
)
from config.settings import PRICE_ZONES
from dashboard import charts, data, ui
from dashboard.registry import USE_CASES

TABLES = ["consumer_prices_hourly", "cheapest_hours"]
USE_CASE = next(uc for uc in USE_CASES if uc.key == "cheapest-hours")

DAY_TOMORROW, DAY_TODAY = "Tomorrow", "Today"
SCHEME_CHOICES = {"Strømstønad": SCHEME_STROMSTOTTE, "Norgespris": SCHEME_NORGESPRIS}
BLOCK_USES = {1: "a dishwasher", 3: "a washing machine", 6: "charging the car"}
ROLES = [data.ROLE_CHEAPEST, data.ROLE_EXPENSIVE, data.ROLE_OTHER]


def _kr(nok: float) -> str:
    return f"{nok:.2f} kr"


def _availability(has_tomorrow: bool, tomorrow, now, built) -> None:
    """Say whether tomorrow is here, on its way, or late."""
    if has_tomorrow:
        return
    checked = f" Gold was last updated at {built:%H:%M}." if built else ""
    if data.tomorrow_is_late(now):
        st.warning(
            f"Tomorrow's prices ({tomorrow:%A %d %B}) are late: they should be here by "
            f"{data.TOMORROW_READY_BY:%H:%M}. The afternoon run may have started late (GitHub sometimes delays "
            f"scheduled runs) or failed.{checked} Showing today.",
            icon=":material/schedule:",
        )
    else:
        st.info(
            f"Tomorrow's prices are published around {data.TOMORROW_PUBLISHED_AROUND} and are here by "
            f"{data.TOMORROW_READY_BY:%H:%M}.{checked} Until then, this page shows today.",
            icon=":material/hourglass_top:",
        )


def _block_box(col, block, kwh: float) -> None:
    """One cheapest block (strømstønad prices): when, its average price, and the saving for `kwh`."""
    n = int(block.block_hours)
    with col.container(border=True):
        st.metric(f"Cheapest {n} hour{'s' if n > 1 else ''}", f"{block.start_time}–{block.end_time}",
                  help=f"For example {BLOCK_USES.get(n, 'an appliance')}. The block stays within the day.")
        st.caption(
            f"Average {block.avg_price_nok_kwh * 100:.1f} øre/kWh, against "
            f"{block.evening_avg_price_nok_kwh * 100:.1f} øre starting at 17:00. "
            f"Moving {kwh:g} kWh here saves **{_kr(block.saving_nok_kwh * kwh)}**."
        )


def render() -> None:
    tables = ui.load_tables(TABLES)
    st.title(USE_CASE.label)
    st.markdown(f"*{USE_CASE.question}*  \n:blue-badge[{USE_CASE.kind}] For: {USE_CASE.user}. "
                "The energy price with VAT; the grid tariff and the supplier's fees come on top.")
    if tables is None:
        return
    consumer, cheapest = tables["consumer_prices_hourly"], tables["cheapest_hours"]
    if cheapest.empty:
        st.info("Gold has no complete days of prices yet. They start in January 2024.")
        return
    names = data.zone_names()
    now = datetime.now(timezone.utc)
    today = data.oslo_today(now)
    tomorrow = today + timedelta(days=1)
    status = data.read_status(data.gold_dir())
    built = data.to_oslo(status.get("built_at_utc")) if status else None

    # Choices, one row above everything they scope.
    zone_col, day_col, scheme_col, kwh_col = st.columns([2, 1.3, 1.7, 1])
    with zone_col:
        zone = st.selectbox("Zone", PRICE_ZONES, format_func=lambda z: f"{z} {names[z]}")
    has_tomorrow = zone in data.published_zones(cheapest, tomorrow)
    with day_col:
        day_choice = st.segmented_control("Day", [DAY_TOMORROW, DAY_TODAY], default=DAY_TOMORROW) or DAY_TOMORROW
    with scheme_col:
        scheme_label = st.segmented_control("Your scheme", list(SCHEME_CHOICES), default="Strømstønad") or "Strømstønad"
    with kwh_col:
        kwh = st.number_input("kWh to move", min_value=1.0, max_value=100.0, value=data.CAR_CHARGE_KWH, step=1.0,
                              format="%.0f", help="20 kWh is about a car charge; a dishwasher run is about 1 kWh.")
    scheme = SCHEME_CHOICES[scheme_label]

    if day_choice == DAY_TOMORROW:
        _availability(has_tomorrow, tomorrow, now, built)
    day = tomorrow if day_choice == DAY_TOMORROW and has_tomorrow else today
    blocks = data.blocks_for(cheapest, zone, day)
    if blocks.empty:
        st.info(f"Gold has no complete prices for {zone} on {day:%A %d %B} yet.")
        return
    prices = data.day_prices(consumer, zone, day, scheme)

    st.subheader(f"{day:%A %d %B %Y} in {zone} · {names[zone]}")
    if scheme == SCHEME_NORGESPRIS and prices["price_ore"].isna().all():
        st.info(f"Norgespris runs from {NORGESPRIS_FROM:%d %B %Y} to {NORGESPRIS_TO:%d %B %Y}; "
                "it doesn't cover this day. Choose strømstønad.")
        return
    block = None
    if scheme == SCHEME_NORGESPRIS:
        # Every hour costs the same: no block to recommend, nothing to highlight.
        st.info(f"With Norgespris every hour costs the same, {prices['price_ore'].iloc[0]:.1f} øre/kWh: moving "
                "usage saves nothing on the energy price. Choose strømstønad to see the cheapest hours.",
                icon=":material/info:")
        st.markdown("**Every hour, as a household with Norgespris pays it**")
    else:
        # The answer: the cheapest blocks.
        for col, row in zip(st.columns(len(blocks)), blocks.itertuples()):
            _block_box(col, row, kwh)

        # Every hour of the day.
        cheapest_hour = prices.loc[prices["price_ore"].idxmin()]
        dearest_hour = prices.loc[prices["price_ore"].idxmax()]
        st.markdown(f"**Every hour, as a household with {scheme_label} pays it**")
        st.caption(f"Day average {prices['price_ore'].mean():.1f} øre/kWh. Cheapest hour {cheapest_hour['time']} "
                   f"({cheapest_hour['price_ore']:.1f} øre), dearest {dearest_hour['time']} "
                   f"({dearest_hour['price_ore']:.1f} øre). Norwegian time.")
        lengths = list(blocks["block_hours"])
        highlight = st.segmented_control("Highlight in the chart", lengths, default=3 if 3 in lengths else lengths[0],
                                         format_func=lambda n: f"Cheapest {n} h") or lengths[0]
        block = blocks[blocks["block_hours"] == highlight].iloc[0]
    prices["role"] = data.hour_roles(prices, block)
    st.altair_chart(charts.hour_bars(prices, ROLES, "øre/kWh"), use_container_width=True)
    with st.expander("Table: every hour"):
        st.dataframe(
            prices[["time", "price_ore", "spot_ore", "with_vat_ore", "role"]], hide_index=True,
            use_container_width=True,
            column_config={
                "time": "Hour",
                "price_ore": st.column_config.NumberColumn(f"{scheme_label} (øre/kWh)", format="%.1f"),
                "spot_ore": st.column_config.NumberColumn("Spot, without VAT", format="%.1f"),
                "with_vat_ore": st.column_config.NumberColumn("With VAT, no support", format="%.1f"),
                "role": "This hour",
            },
        )

    # The same answer for machines: the API (api/main.py), documented on its own page.
    st.divider()
    st.markdown("**For apps and smart chargers:** the same answers come as JSON from the project's API, "
                "with no key needed.")
    st.page_link(ui.PAGES["api"], label="API for apps: endpoints, a try-it panel and an example for a charger",
                 icon=":material/api:")

    with st.expander("Assumptions and definitions"):
        thresholds = ", ".join(f"{v * 100:.0f} øre in {y}" for y, v in STROMSTOTTE_THRESHOLD_NOK.items())
        st.markdown(
            f"- **Prices** are the day-ahead spot prices plus {VAT:.0%} VAT ({', '.join(NO_VAT_ZONES)}: no VAT), "
            "per kWh of energy. The grid tariff and the supplier's fees come on top and are the same in every hour "
            "of the day here, so they don't change which hours are cheapest.\n"
            f"- **Strømstønad**: the state pays {STROMSTOTTE_SHARE:.0%} of each hour's spot price above a yearly "
            f"threshold ({thresholds}, excluding VAT), plus VAT on that support. Every household without Norgespris "
            "gets it automatically.\n"
            f"- **Norgespris**: a fixed {NORGESPRIS_NOK * 100:.0f} øre per kWh plus VAT, from "
            f"{NORGESPRIS_FROM:%d %B %Y} to {NORGESPRIS_TO:%d %B %Y}, instead of strømstønad.\n"
            "- **Cheapest block**: the run of 1, 3 or 6 whole hours within the day with the lowest average price "
            "after strømstønad. Strømstønad flattens the expensive hours, so this can differ from the lowest "
            "average spot price. Ties go to the earliest start.\n"
            "- **Saving**: the block starting at 17:00, when people come home, minus the cheapest block, times the "
            "kWh you move.\n"
            f"- **When**: prices for tomorrow are published around {data.TOMORROW_PUBLISHED_AROUND}. An afternoon "
            "run fetches them every 30 minutes and brings them here by "
            f"{data.TOMORROW_READY_BY:%H:%M}. A day is shown only when every hour has a price."
        )
