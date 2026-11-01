"""
Placeholder page for a use case that isn't built yet. It describes the use case from its entry in
dashboard/registry.py, so every planned use case already has a page with its question and plan.
"""

import streamlit as st

from dashboard.registry import UseCase


def render(use_case: UseCase) -> None:
    st.title(use_case.label)
    st.markdown(":gray-badge[Planned] " + f":blue-badge[{use_case.kind}]")
    st.markdown(f"### {use_case.question}")

    left, right = st.columns(2)
    with left.container(border=True):
        st.markdown("**Who it is for**")
        st.write(use_case.user)
        st.markdown("**Ready by** (Norwegian time)")
        st.write(use_case.ready_by)
        st.markdown("**Refresh**")
        st.write(use_case.refresh)
    with right.container(border=True):
        st.markdown("**Planned Gold tables**")
        st.markdown("\n".join(f"- `{t}`" for t in use_case.gold_tables))

    st.info(
        "Not built yet. The plan is in docs/plans/use_cases.md. To add this page, write "
        "`dashboard/views/<name>.py` with a `render()` function, then set this entry's `view` and "
        "`status` in `dashboard/registry.py`.",
        icon=":material/construction:",
    )
