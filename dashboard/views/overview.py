"""
Home page: what the project is, and one card per use case with its status.
"""

import streamlit as st

from dashboard import registry, ui


def render() -> None:
    st.title("Norwegian power prices and weather")
    st.markdown(
        "Hourly day-ahead spot prices for the bidding zones **NO1–NO5** (hvakosterstrommen.no) and hourly "
        "temperature and wind from MET Norway's Frost API, cleaned in the Silver layer and shaped for each "
        "use case in Gold. Each card below is one Gold use case: who it is for, the question it answers, "
        "and when it must be ready."
    )

    cards = sorted(registry.USE_CASES, key=lambda uc: (uc.number is None, uc.number or 0))
    for row_start in range(0, len(cards), 3):
        for col, uc in zip(st.columns(3), cards[row_start:row_start + 3]):
            with col.container(border=True):
                ready = uc.status == registry.READY
                st.markdown(f"**{uc.label}**  \n:{'green' if ready else 'gray'}-badge[{'Ready' if ready else 'Planned'}] "
                            f":blue-badge[{uc.kind}]")
                st.markdown(f"*{uc.question}*")
                st.caption(f"For: {uc.user}  \nReady by: {uc.ready_by} (Norwegian time)  \nRefresh: {uc.refresh}")
                st.page_link(ui.PAGES[uc.key], label="Open" if ready else "Details", icon=uc.icon)
