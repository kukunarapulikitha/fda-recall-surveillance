"""Executive summary page — rendered markdown report with key findings."""

from __future__ import annotations

from datetime import date, timedelta

import streamlit as st

from src.analytics.queries import load_recalls
from src.analytics.reports import ExecutiveReport


def render():
    st.header("Executive Summary")
    st.markdown("A plain-English rollup of the analytics — suitable for sharing.")

    c1, c2 = st.columns(2)
    default_start = date.today() - timedelta(days=365 * 5)
    start = c1.date_input("Start date", value=default_start, key="es_start")
    end = c2.date_input("End date", value=date.today(), key="es_end")

    df = load_recalls(start_date=start, end_date=end)
    if df.empty:
        st.info("No recall data in the selected range.")
        return

    report = ExecutiveReport()
    markdown = report.render_markdown(df)

    st.download_button(
        "Download Report (Markdown)",
        data=markdown,
        file_name=f"fda-recall-executive-summary-{date.today().isoformat()}.md",
        mime="text/markdown",
    )

    st.markdown("---")
    st.markdown(markdown)
