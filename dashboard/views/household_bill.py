"""
Use case 3, report: monthly household bill (docs/plans/use_cases.md, section 5.3).

"What did a typical household pay last month, and was Norgespris cheaper than strømstønad?"
  * The report: one month's bill in every zone under each scheme, which scheme was cheaper, and a CSV
    download. It opens on the newest final month, i.e. last month.
  * Month by month: one zone's bills over time, and what Norgespris saved compared with strømstønad.

Reads the Gold table household_bill_monthly. The household is modelled (config/price_rules.py).
"""

import streamlit as st

from config.price_rules import (
    BASE_LOAD_KWH,
    KWH_PER_HDH,
    NORGESPRIS_FROM,
    NORGESPRIS_NOK,
    NORGESPRIS_TO,
    SCHEMES,
    STROMSTOTTE_SHARE,
    STROMSTOTTE_THRESHOLD_NOK,
    SUPPORT_CAP_KWH,
    VAT,
)
from config.settings import PRICE_ZONES
from dashboard import charts, data, ui
from dashboard.registry import USE_CASES

TABLES = ["household_bill_monthly"]
USE_CASE = next(uc for uc in USE_CASES if uc.key == "monthly-bill")

NOK = "%.0f"
COLUMNS = {
    "bidding_zone": "Zone",
    "month_start": st.column_config.DateColumn("Month", format="MMM YYYY"),
    "kwh": st.column_config.NumberColumn("kWh used", format=NOK),
    "no_support_nok": st.column_config.NumberColumn("No support (NOK)", format=NOK),
    "stromstotte_nok": st.column_config.NumberColumn("Strømstønad (NOK)", format=NOK),
    "norgespris_nok": st.column_config.NumberColumn("Norgespris (NOK)", format=NOK),
    "norgespris_saving_nok": st.column_config.NumberColumn(
        "Saved with Norgespris (NOK)", format=NOK,
        help="The strømstønad bill minus the Norgespris bill. Below zero, Norgespris cost more."),
    "cheaper": "Cheaper scheme",
    "is_final": st.column_config.CheckboxColumn("Final", help="Every hour of the month has a price"),
}
SHOWN = ["kwh", "no_support_nok", "stromstotte_nok", "norgespris_nok", "norgespris_saving_nok", "cheaper"]


def _ore(nok: float) -> str:
    return f"{nok * 100:.0f} øre"


def _answer(report) -> str:
    """One sentence answering the use case's question for the chosen month."""
    with_norgespris = report.dropna(subset=["norgespris_nok"])
    if with_norgespris.empty:
        return "Norgespris didn't exist yet: households had strømstønad."
    cheaper = with_norgespris[with_norgespris["cheaper"] == data.SCHEME_LABELS["norgespris"]]["bidding_zone"]
    if cheaper.empty:
        return "Strømstønad was cheaper than Norgespris in every zone."
    if len(cheaper) == len(with_norgespris):
        return "Norgespris was cheaper than strømstønad in every zone."
    return f"Norgespris was cheaper than strømstønad in {', '.join(cheaper)}; strømstønad in the other zones."


def render() -> None:
    tables = ui.load_tables(TABLES)
    st.title(USE_CASE.label)
    st.markdown(f"*{USE_CASE.question}*  \n:blue-badge[{USE_CASE.kind}] For: {USE_CASE.user}. "
                "The energy price with VAT; the grid tariff and the supplier's fees are left out, as they are "
                "the same under every scheme.")
    if tables is None:
        return
    bill = tables["household_bill_monthly"]
    if bill.empty:
        st.info("Gold has no household bills yet. They start in January 2024.")
        return
    names = data.zone_names()

    # Filters, one row above everything they scope. The report opens on the newest final month.
    months = data.bill_months(bill)
    final = months.index[months["is_final"]]
    left, right = st.columns([3, 1])
    with left:
        choice = st.selectbox(
            "Month", months.index, index=int(final[0]) if len(final) else 0,
            format_func=lambda i: f"{months.at[i, 'month_start']:%B %Y}"
                                  + ("" if months.at[i, "is_final"] else " (month to date)"),
        )
    with right:
        zone = st.selectbox("Zone for the history", PRICE_ZONES, format_func=lambda z: f"{z} {names[z]}")
    month = months.loc[choice]

    # ------------------------------------------------------------------ #
    # The report: one month, every zone
    # ------------------------------------------------------------------ #
    report = data.bill_for_month(bill, month["year"], month["month"])
    st.subheader(f"The bill for {month['month_start']:%B %Y}")
    if month["is_final"]:
        st.caption("Final: every hour of the month has a price in every zone.")
    else:
        st.caption(f"Month to date: {report['billed_hours'].min()} of {report['hours_in_month'].max()} hours so far. "
                   "The final bill is ready on the 1st of the next month.")
    st.markdown(f"**{_answer(report)}**")
    st.dataframe(report, hide_index=True, use_container_width=True, column_config=COLUMNS,
                 column_order=["bidding_zone", *SHOWN])
    st.download_button(
        "Download this month as CSV", report.to_csv(index=False).encode("utf-8"),
        file_name=f"household_bill_{month['year']}-{month['month']:02d}.csv", mime="text/csv",
        icon=":material/download:",
    )

    # ------------------------------------------------------------------ #
    # Month by month, one zone
    # ------------------------------------------------------------------ #
    st.divider()
    st.header(f"Month by month in {zone} · {names[zone]}")
    history = data.bill_history(bill, zone)
    by_month = data.bill_by_month(history)

    st.markdown("**The monthly bill under each scheme**")
    st.altair_chart(
        charts.lines(history, "month_start", "cost_nok", "NOK per month", series="scheme", domain=SCHEMES,
                     colors=charts.scheme_colors(), series_labels=data.SCHEME_LABELS, x_format="%b %Y"),
        use_container_width=True,
    )

    saving = by_month.dropna(subset=["norgespris_saving_nok"])
    if not saving.empty:
        st.markdown("**What Norgespris saved compared with strømstønad**")
        st.caption(f"Above zero, Norgespris was cheaper that month; below zero, strømstønad was. "
                   f"Norgespris started in {NORGESPRIS_FROM:%B %Y}.")
        st.altair_chart(
            charts.columns(saving, "month_start", "norgespris_saving_nok", "NOK per month", charts.scheme_colors()[2]),
            use_container_width=True,
        )
    with st.expander(f"Table: monthly bills in {zone}"):
        st.dataframe(by_month, hide_index=True, use_container_width=True, column_config=COLUMNS,
                     column_order=["month_start", *SHOWN, "is_final"])

    with st.expander("Assumptions and definitions"):
        per_year = data.household_kwh_per_year(bill, zone)
        thresholds = ", ".join(f"{_ore(v)} in {y}" for y, v in STROMSTOTTE_THRESHOLD_NOK.items())
        st.markdown(
            f"- **The household** is modelled, as we have no consumption data. Every hour it uses "
            f"{BASE_LOAD_KWH} kWh plus {KWH_PER_HDH} kWh per heating degree hour (max(0, 17 °C − temperature))"
            + (f": about {per_year:,.0f} kWh a year in {zone} (its last 12 final months)." if per_year else ".")
            + " An hour without a temperature uses the zone's average heating need that month.\n"
            f"- **No support**: the spot price plus {VAT:.0%} VAT. Households in NO4 (Nordland, Troms and "
            "Finnmark) pay no VAT on electricity.\n"
            f"- **Strømstønad**: the state pays {STROMSTOTTE_SHARE:.0%} of each hour's spot price above a yearly "
            f"threshold ({thresholds}, excluding VAT), plus VAT on that support.\n"
            f"- **Norgespris**: a fixed {_ore(NORGESPRIS_NOK)} per kWh plus VAT, from {NORGESPRIS_FROM:%d %B %Y} to "
            f"{NORGESPRIS_TO:%d %B %Y}, instead of strømstønad. When the spot price is lower, the household pays "
            "more than it would without support.\n"
            f"- **Left out**: the grid tariff and the supplier's fees (the same under every scheme), and the "
            f"{SUPPORT_CAP_KWH:,} kWh monthly cap of both schemes. The household stays below the cap; Gold checks it.\n"
            "- **Final**: every hour of the month has a price. The month in progress is shown month to date."
        )
