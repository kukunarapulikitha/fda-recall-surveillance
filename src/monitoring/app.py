"""Streamlit data quality monitoring dashboard — main entry point."""

import streamlit as st

st.set_page_config(
    page_title="FDA Recall Pipeline Monitor",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("FDA Recall Pipeline Monitor")
st.markdown("Data quality and pipeline health monitoring for the FDA recall surveillance system.")

# Navigation
page = st.sidebar.radio(
    "Navigation",
    ["Pipeline Health", "Data Quality", "Coverage"],
)

if page == "Pipeline Health":
    from src.monitoring.pages.pipeline_health import render
    render()
elif page == "Data Quality":
    from src.monitoring.pages.data_quality import render
    render()
elif page == "Coverage":
    from src.monitoring.pages.coverage import render
    render()
