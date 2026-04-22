"""Standalone Streamlit demo for the analytics layer — no database required.

Generates a realistic synthetic recall dataset in memory and renders the three
analytics pages against it. Use when you want to see the dashboard without
setting up PostgreSQL.

Run with:
    PYTHONPATH=. streamlit run scripts/demo_dashboard.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable when streamlit runs this file directly.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
import plotly.express as px
import streamlit as st

from src.analytics.categorize import RecallCategorizer
from src.analytics.correlation import CorrelationAnalyzer
from src.analytics.high_risk import HighRiskIdentifier
from src.analytics.reports import ExecutiveReport
from src.analytics.risk_scoring import RiskScorer
from src.analytics.temporal import TemporalAnalyzer
from scripts.demo_analytics import build_dataset

st.set_page_config(
    page_title="FDA Recall Analytics Demo",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data
def _load_demo_data() -> pd.DataFrame:
    """Generate + categorize + score the synthetic dataset (cached)."""
    df = build_dataset(300)
    df = RecallCategorizer().categorize(df)
    df = RiskScorer().score(df)
    return df


st.title("FDA Recall Analytics — Demo")
st.markdown(
    "Dashboard running against a **synthetic 300-row dataset** "
    "(2022-2024). Swap in `load_recalls()` to point at the real PostgreSQL data."
)

page = st.sidebar.radio(
    "Page",
    ["Recall Pattern Analysis", "High-Risk Rankings", "Executive Summary"],
)

df = _load_demo_data()


# ---------- Page 1: Recall Pattern Analysis ----------
def render_patterns(df: pd.DataFrame) -> None:
    st.header("Recall Pattern Analysis")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Recalls", f"{len(df):,}")
    c2.metric("Class I", int((df["classification"] == "Class I").sum()))
    c3.metric("Unique Firms", df["recalling_firm"].nunique())
    c4.metric("Avg Risk Score", f"{df['risk_score'].mean():.1f}")
    c5.metric("Critical Tier", int((df["risk_tier"] == "Critical").sum()))

    tab1, tab2, tab3 = st.tabs(["Categorization", "Temporal Patterns", "Severity & Risk"])

    with tab1:
        st.subheader("Recalls by Product Type × Classification")
        ct = pd.crosstab(df["product_type"], df["classification"].fillna("Unclassified"))
        fig = px.bar(
            ct.reset_index().melt(id_vars="product_type", var_name="classification", value_name="count"),
            x="product_type", y="count", color="classification", barmode="stack",
        )
        st.plotly_chart(fig, use_container_width=True)

        drugs = df[df["product_type"] == "Drugs"]
        if not drugs.empty:
            st.subheader("Top Therapeutic Areas (Drugs)")
            ta = RecallCategorizer.summary_by(drugs, "therapeutic_area").head(12)
            fig_ta = px.bar(
                ta, x="total_recalls", y="therapeutic_area", orientation="h",
                color="class_i_pct", color_continuous_scale="Reds",
                labels={"class_i_pct": "Class I %"},
            )
            fig_ta.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_ta, use_container_width=True)

        st.subheader("Top Recall Reasons")
        reasons = RecallCategorizer.summary_by(df, "reason_category").head(10)
        fig_r = px.bar(
            reasons, x="reason_category", y="total_recalls", color="class_i_pct",
            color_continuous_scale="Reds", labels={"class_i_pct": "Class I %"},
        )
        st.plotly_chart(fig_r, use_container_width=True)

    with tab2:
        temporal = TemporalAnalyzer()

        st.subheader("Monthly Recall Volume")
        monthly = temporal.monthly_counts(df, by="product_type")
        fig_m = px.line(
            monthly, x="month", y="count", color="product_type", markers=True,
        )
        st.plotly_chart(fig_m, use_container_width=True)

        st.subheader("Quarterly Recalls")
        quarterly = temporal.quarterly_counts(df, by="product_type")
        fig_q = px.bar(
            quarterly, x="quarter", y="count", color="product_type", barmode="group",
        )
        st.plotly_chart(fig_q, use_container_width=True)

        st.subheader("Seasonal Pattern")
        seasonal = temporal.seasonal_counts(df, by="product_type")
        seasonal["season"] = seasonal["season"].astype(str)
        fig_s = px.bar(
            seasonal, x="season", y="count", color="product_type", barmode="group",
            category_orders={"season": ["Winter", "Spring", "Summer", "Fall"]},
        )
        st.plotly_chart(fig_s, use_container_width=True)

        st.subheader("Anomaly Detection (|z| ≥ 2)")
        spikes = temporal.spike_detection(df)
        fig_sp = px.scatter(
            spikes, x="month", y="count", color="is_spike",
            color_discrete_map={True: "red", False: "steelblue"},
        )
        st.plotly_chart(fig_sp, use_container_width=True)
        flagged = spikes[spikes["is_spike"]]
        if not flagged.empty:
            st.dataframe(flagged[["month", "count", "z_score"]], use_container_width=True)

        trend = temporal.trend(df)
        direction = "increasing" if trend["slope"] > 0 else "decreasing"
        st.info(
            f"**Trend:** over {trend['n_months']} months, recalls are *{direction}* "
            f"at {abs(trend['slope']):.2f} recalls/month (mean = {trend['mean']:.1f})."
        )

    with tab3:
        st.subheader("Risk Tier Distribution")
        tier_df = RiskScorer.tier_distribution(df)
        fig_t = px.pie(
            tier_df, values="count", names="risk_tier",
            color="risk_tier",
            color_discrete_map={
                "Critical": "#b00020", "High": "#e65100",
                "Medium": "#f9a825", "Low": "#2e7d32",
            },
        )
        st.plotly_chart(fig_t, use_container_width=True)

        st.subheader("Risk Score Histogram")
        fig_h = px.histogram(df, x="risk_score", nbins=20, color="product_type")
        st.plotly_chart(fig_h, use_container_width=True)

        st.subheader("Classification × Distribution Reach")
        severity_by_reach = CorrelationAnalyzer().severity_by_reach(df)
        fig_sr = px.imshow(
            severity_by_reach.values,
            x=[str(c) for c in severity_by_reach.columns],
            y=[str(i) for i in severity_by_reach.index],
            labels={"x": "Distribution Reach", "y": "Classification", "color": "Recalls"},
            color_continuous_scale="YlOrRd",
            aspect="auto",
        )
        st.plotly_chart(fig_sr, use_container_width=True)


# ---------- Page 2: High-Risk Rankings ----------
def render_high_risk(df: pd.DataFrame) -> None:
    st.header("High-Risk Manufacturers & Products")

    c1, c2 = st.columns(2)
    min_recalls = c1.number_input("Min recalls to flag a firm", 1, 50, 3)
    top_n = c2.number_input("Top N to display", 5, 100, 15)

    identifier = HighRiskIdentifier(min_recalls=int(min_recalls), top_n=int(top_n))

    st.subheader("Top Manufacturers by Priority Score")
    top_mfrs = identifier.top_manufacturers(df)
    st.dataframe(top_mfrs, use_container_width=True)

    fig = px.bar(
        top_mfrs.head(15),
        x="priority_score", y="manufacturer", orientation="h",
        color="class_i", color_continuous_scale="Reds",
        labels={"class_i": "Class I count"},
        title="Priority score = recall volume × average severity",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Repeat Class I Offenders")
    repeat = identifier.repeat_offenders(df)
    if repeat.empty:
        st.success("No firms with multiple Class I recalls.")
    else:
        st.warning(f"{len(repeat)} firms have 2+ Class I recalls.")
        st.dataframe(repeat, use_container_width=True)

    st.subheader("Top Product Categories")
    top_cats = identifier.top_product_categories(df)
    st.dataframe(top_cats, use_container_width=True)
    fig_c = px.bar(
        top_cats.head(15),
        x="priority_score", y="product_category", orientation="h",
        color="class_i", color_continuous_scale="Reds",
    )
    fig_c.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_c, use_container_width=True)


# ---------- Page 3: Executive Summary ----------
def render_exec_summary(df: pd.DataFrame) -> None:
    st.header("Executive Summary Report")

    markdown = ExecutiveReport().render_markdown(df)

    st.download_button(
        "Download Report (Markdown)",
        data=markdown,
        file_name="fda-recall-executive-summary.md",
        mime="text/markdown",
    )
    st.markdown("---")
    st.markdown(markdown)


if page == "Recall Pattern Analysis":
    render_patterns(df)
elif page == "High-Risk Rankings":
    render_high_risk(df)
else:
    render_exec_summary(df)
