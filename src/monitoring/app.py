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
    "End-to-end FDA recall surveillance — ingestion pipeline, analytics, "
    "intelligent alerts, and user-friendly search."
)

# ── Sidebar navigation ───────────────────────────────────────────────────────

st.sidebar.markdown("### 🔍 Explore")
st.sidebar.markdown("### 📊 Monitoring")
st.sidebar.markdown("### 📈 Analytics")
st.sidebar.markdown("### 🔔 Alerts")

page = st.sidebar.radio(
    "Navigation",
    [
        # Explore
        "Search & Explore",
        # Monitoring
        "Pipeline Health",
        "Data Quality",
        "Coverage",
        # Analytics
        "Recall Pattern Analysis",
        "High-Risk Rankings",
        "Executive Summary",
        # Alerts
        "Alerts & Subscriptions",
    ],
)

# ── Page routing ─────────────────────────────────────────────────────────────

if page == "Search & Explore":
    from src.monitoring.pages.search_explore import render
elif page == "Pipeline Health":
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
elif page == "Alerts & Subscriptions":
    from src.monitoring.pages.alerts_subscriptions import render
else:
    render = lambda: st.error(f"Unknown page: {page}")

render()
