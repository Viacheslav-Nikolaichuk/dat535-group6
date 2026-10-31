"""
Streamlit pieces shared by every page: cached Gold loading and the status box in the sidebar.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
import streamlit as st

from config.settings import DAT535_ENV
from dashboard import data

# Filled by app.py with every st.Page, so pages can link to each other (st.page_link).
PAGES: Dict[str, "st.Page"] = {}


@st.cache_data(show_spinner="Loading Gold tables…", max_entries=32)
def _cached_table(path: str, built_at: str) -> pd.DataFrame:
    # built_at is only part of the cache key: a new Gold run has a new built_at, so its tables are
    # read again without restarting the dashboard.
    return data.read_table(Path(path))


def load_tables(names: Iterable[str]) -> Optional[Dict[str, pd.DataFrame]]:
    """The named Gold tables, or None (with a message on the page) when Gold isn't there yet."""
    gold = data.gold_dir()
    status = data.read_status(gold)
    if status is None:
        st.info(
            f"No Gold data in `{gold}` yet. On the VM, run the pipeline (or `python src/gold/run_gold.py`). "
            "To try the dashboard without real data, run `python scripts/make_sample_gold.py`."
        )
        return None
    try:
        return {name: _cached_table(str(gold / name), status.get("built_at_utc", "")) for name in names}
    except Exception as e:  # a missing table, or a read during the swap of a new Gold run
        st.error(f"Could not read the Gold tables in `{gold}`: {e}. Reload the page in a moment.")
        return None


def sidebar_status() -> None:
    """Environment, when Gold was built, how far the data reaches, and any warnings."""
    gold = data.gold_dir()
    status = data.read_status(gold)
    with st.sidebar:
        st.divider()
        if status is None:
            st.caption(f"Environment: **{DAT535_ENV}**  \nNo Gold data yet.")
            return
        built = data.to_oslo(status.get("built_at_utc"))
        fresh = status.get("freshness") or {}
        st.caption(
            f"Environment: **{status.get('environment', DAT535_ENV)}**  \n"
            f"Gold updated: **{built:%d %b %Y, %H:%M}** (Norwegian time)  \n"
            f"Weather and prices complete through: **{fresh.get('data_through') or '–'}**  \n"
            f"Prices through: **{fresh.get('prices_through') or '–'}**"
        )
        if status.get("is_sample"):
            st.warning("Sample data made by scripts/make_sample_gold.py: not real prices or weather.",
                       icon=":material/science:")
        for warning in data.freshness_warnings(status, data.read_newest_run(gold), datetime.now(timezone.utc)):
            st.warning(warning, icon=":material/schedule:")
