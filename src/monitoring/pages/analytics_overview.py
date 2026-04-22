"""Analytics overview — categorization, temporal patterns, and risk-tier breakdown."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from src.analytics.categorize import RecallCategorizer
from src.analytics.queries import load_recalls
from src.analytics.risk_scoring import RiskScorer
from src.analytics.temporal import TemporalAnalyzer


@st.cache_data(ttl=300)
def _load(start: date | None, end: date | None) -> pd.DataFrame:
    df = load_recalls(start_date=start, end_date=end)
    if df.empty:
        return df
    df = RecallCategorizer().categorize(df)
    df = RiskScorer().score(df)
    return df


def render():
    st.header("Recall Pattern Analysis")
    st.markdown(
        "Categorization, temporal patterns, and severity breakdown across all "
        "ingested FDA recall data."
    )

    col_a, col_b = st.columns(2)
    default_start = date.today() - timedelta(days=365 * 5)
    start = col_a.date_input("Start date", value=default_start)
    end = col_b.date_input("End date", value=date.today())

    df = _load(start, end)
    if df.empty:
        st.info("No recall data in the selected range.")
        return

    _kpi_row(df)

    tab1, tab2, tab3, tab4 = st.tabs([
        "Categorization", "Temporal Patterns", "Severity & Risk", "Raw Data",
    ])

    with tab1:
        _render_categorization(df)
    with tab2:
        _render_temporal(df)
    with tab3:
        _render_severity(df)
    with tab4:
        st.dataframe(
            df[["recall_number", "product_type", "classification", "recalling_firm",
                "therapeutic_area", "product_category", "reason_category",
                "risk_score", "risk_tier", "report_date"]],
            use_container_width=True,
            height=500,
        )


def _kpi_row(df: pd.DataFrame):
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Recalls", f"{len(df):,}")
    c2.metric("Class I", int((df["classification"] == "Class I").sum()))
    c3.metric("Unique Firms", df["recalling_firm"].nunique())
    c4.metric("Avg Risk Score", f"{df['risk_score'].mean():.1f}")
    c5.metric("Critical Tier", int((df["risk_tier"] == "Critical").sum()))


def _render_categorization(df: pd.DataFrame):
    st.subheader("By Product Type × Classification")
    ct = pd.crosstab(df["product_type"], df["classification"].fillna("Unclassified"))
    fig = px.bar(
        ct.reset_index().melt(id_vars="product_type", var_name="classification", value_name="count"),
        x="product_type", y="count", color="classification", barmode="stack",
        title="Recalls by Product Type and Classification",
    )
    st.plotly_chart(fig, use_container_width=True)

    drug_df = df[df["product_type"] == "Drugs"]
    if not drug_df.empty:
        st.subheader("Top Therapeutic Areas (Drugs)")
        ta = RecallCategorizer.summary_by(drug_df, "therapeutic_area").head(12)
        fig_ta = px.bar(
            ta, x="total_recalls", y="therapeutic_area", orientation="h",
            color="class_i_pct",
            color_continuous_scale="Reds",
            labels={"class_i_pct": "Class I %"},
            title="Drug Recalls by Therapeutic Area (color = Class I %)",
        )
        fig_ta.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_ta, use_container_width=True)

    st.subheader("Top Reason Categories")
    reasons = RecallCategorizer.summary_by(df, "reason_category").head(10)
    fig_r = px.bar(
        reasons, x="reason_category", y="total_recalls", color="class_i_pct",
        color_continuous_scale="Reds",
        labels={"class_i_pct": "Class I %"},
        title="Recalls by Reason Category",
    )
    st.plotly_chart(fig_r, use_container_width=True)


def _render_temporal(df: pd.DataFrame):
    temporal = TemporalAnalyzer()

    st.subheader("Monthly Recall Volume")
    monthly = temporal.monthly_counts(df, by="product_type")
    if monthly.empty:
        st.info("No dated records in this range.")
        return
    fig_m = px.line(
        monthly, x="month", y="count", color="product_type", markers=True,
        title="Monthly Recalls by Product Type",
    )
    st.plotly_chart(fig_m, use_container_width=True)

    st.subheader("Quarterly Recalls")
    quarterly = temporal.quarterly_counts(df, by="product_type")
    fig_q = px.bar(
        quarterly, x="quarter", y="count", color="product_type", barmode="group",
        title="Quarterly Recalls by Product Type",
    )
    st.plotly_chart(fig_q, use_container_width=True)

    st.subheader("Seasonal Pattern")
    seasonal = temporal.seasonal_counts(df, by="product_type")
    if not seasonal.empty:
        seasonal["season"] = seasonal["season"].astype(str)
        fig_s = px.bar(
            seasonal, x="season", y="count", color="product_type", barmode="group",
            title="Recall Counts by Season",
            category_orders={"season": ["Winter", "Spring", "Summer", "Fall"]},
        )
        st.plotly_chart(fig_s, use_container_width=True)

    st.subheader("Month-of-Year Profile (seasonality)")
    profile = temporal.month_of_year_profile(df)
    if not profile.empty:
        fig_p = px.bar(
            profile, x="month_name", y="mean", error_y="std",
            title="Average Recalls per Month (with std dev)",
            category_orders={"month_name": profile["month_name"].tolist()},
        )
        st.plotly_chart(fig_p, use_container_width=True)

    st.subheader("Anomaly Detection (|z|>=2)")
    spikes = temporal.spike_detection(df)
    if not spikes.empty:
        fig_sp = px.scatter(
            spikes, x="month", y="count", color="is_spike",
            color_discrete_map={True: "red", False: "steelblue"},
            title="Monthly Counts with Spikes Highlighted",
        )
        st.plotly_chart(fig_sp, use_container_width=True)
        flagged = spikes[spikes["is_spike"]]
        if not flagged.empty:
            st.dataframe(flagged[["month", "count", "z_score"]], use_container_width=True)


def _render_severity(df: pd.DataFrame):
    st.subheader("Risk Tier Distribution")
    tier_df = RiskScorer.tier_distribution(df)
    fig_t = px.pie(
        tier_df, values="count", names="risk_tier",
        title="Recalls by Risk Tier",
        color="risk_tier",
        color_discrete_map={
            "Critical": "#b00020", "High": "#e65100",
            "Medium": "#f9a825", "Low": "#2e7d32",
        },
    )
    st.plotly_chart(fig_t, use_container_width=True)

    st.subheader("Risk Score Histogram")
    fig_h = px.histogram(
        df, x="risk_score", nbins=20, color="product_type",
        title="Distribution of Risk Scores",
    )
    st.plotly_chart(fig_h, use_container_width=True)

    st.subheader("Classification × Distribution Reach")
    from src.analytics.correlation import CorrelationAnalyzer
    severity_by_reach = CorrelationAnalyzer().severity_by_reach(df)
    if not severity_by_reach.empty:
        fig_sr = px.imshow(
            severity_by_reach.values,
            x=[str(c) for c in severity_by_reach.columns],
            y=[str(i) for i in severity_by_reach.index],
            labels={"x": "Distribution Reach", "y": "Classification", "color": "Recalls"},
            title="Heatmap: Classification vs Distribution Reach",
            color_continuous_scale="YlOrRd",
            aspect="auto",
        )
        st.plotly_chart(fig_sr, use_container_width=True)
