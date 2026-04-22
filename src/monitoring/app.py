"""Streamlit monitoring + analytics dashboard — main entry point."""

import streamlit as st

st.set_page_config(
    page_title="FDA Recall Intelligence",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("FDA Recall Intelligence")
st.markdown(
    "Data quality monitoring and pattern analysis for the FDA recall "
    "surveillance system."
)

st.sidebar.markdown("### Monitoring")
st.sidebar.markdown("### Analytics")

page = st.sidebar.radio(
    "Navigation",
    [
        "Pipeline Health",
        "Data Quality",
        "Coverage",
        "Recall Pattern Analysis",
        "High-Risk Rankings",
        "Executive Summary",
    ],
)

if page == "Pipeline Health":
    from src.monitoring.pages.pipeline_health import render
elif page == "Data Quality":
    from src.monitoring.pages.data_quality import render
elif page == "Coverage":
    from src.monitoring.pages.coverage import render
elif page == "Recall Pattern Analysis":
    from src.monitoring.pages.analytics_overview import render
elif page == "High-Risk Rankings":
    from src.monitoring.pages.risk_dashboard import render
elif page == "Executive Summary":
    from src.monitoring.pages.executive_summary import render
else:
    render = lambda: st.error(f"Unknown page: {page}")

render()
