"""
DAT535 dashboard: Streamlit pages over the Gold layer.

The menu is built from dashboard/registry.py: ready use cases get their own page, planned ones a
placeholder that describes them. Gold is read from <data root>/<env>/gold (DAT535_ENV), or from
DAT535_GOLD_DIR when set.

Usage:
    streamlit run dashboard/app.py                                  # local, http://localhost:8501
    DAT535_ENV=prod streamlit run dashboard/app.py --server.address 127.0.0.1 --server.headless true
"""

import importlib
from pathlib import Path
import sys

# Streamlit runs this file as a script; put the project root on sys.path for `config` and `dashboard`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st  # noqa: E402

from dashboard import registry, ui  # noqa: E402
from dashboard.views import overview, planned  # noqa: E402

st.set_page_config(page_title="DAT535 · Power prices and weather", page_icon=":material/bolt:", layout="wide")


def _page(use_case: registry.UseCase) -> "st.Page":
    if use_case.status == registry.READY and use_case.view:
        render = importlib.import_module(use_case.view).render
    else:
        def render(uc=use_case):
            planned.render(uc)
    # An explicit url_path keeps pages that share a function (the placeholders) apart.
    return st.Page(render, title=use_case.label, icon=use_case.icon, url_path=use_case.key)


home = st.Page(overview.render, title="Overview", icon=":material/home:", url_path="overview", default=True)
ui.PAGES["overview"] = home
for uc in registry.USE_CASES:
    ui.PAGES[uc.key] = _page(uc)

use_cases = sorted((uc for uc in registry.USE_CASES if uc.number), key=lambda uc: uc.number)
shared = [uc for uc in registry.USE_CASES if not uc.number]
sections = {
    "": [home],
    "Use cases": [ui.PAGES[uc.key] for uc in use_cases],
    "More": [ui.PAGES[uc.key] for uc in shared],
}
navigation = st.navigation({name: pages for name, pages in sections.items() if pages})
ui.sidebar_status()
navigation.run()
