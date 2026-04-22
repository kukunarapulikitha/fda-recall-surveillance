"""Risk dashboard — high-risk manufacturer and product category rankings."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from src.analytics.categorize import RecallCategorizer
from src.analytics.high_risk import HighRiskIdentifier
from src.analytics.queries import load_recalls
from src.analytics.risk_scoring import RiskScorer


@st.cache_data(ttl=300)
def _load(start: date | None, end: date | None, product_type: str | None) -> pd.DataFrame:
    df = load_recalls(start_date=start, end_date=end, product_type=product_type)
    if df.empty:
        return df
    df = RecallCategorizer().categorize(df)
    df = RiskScorer().score(df)
    return df


def render():
    st.header("High-Risk Manufacturers & Products")
    st.markdown(
        "Ranks manufacturers and product categories using a composite priority "
        "score (recall volume × average severity)."
    )

    c1, c2, c3 = st.columns(3)
    default_start = date.today() - timedelta(days=365 * 3)
    start = c1.date_input("Start date", value=default_start)
    end = c2.date_input("End date", value=date.today())
    product_type = c3.selectbox("Product type", ["All", "Drugs", "Devices", "Food"])
    pt_filter = None if product_type == "All" else product_type

    df = _load(start, end, pt_filter)
    if df.empty:
        st.info("No recall data in the selected range.")
        return

    c_left, c_right = st.columns(2)
    with c_left:
        min_recalls = st.number_input("Min recalls to flag a firm", 1, 50, 3)
    with c_right:
        top_n = st.number_input("Top N to display", 5, 100, 20)

    identifier = HighRiskIdentifier(min_recalls=int(min_recalls), top_n=int(top_n))

    st.subheader("Top Manufacturers by Priority Score")
    top_mfrs = identifier.top_manufacturers(df)
    if top_mfrs.empty:
        st.info(f"No manufacturers meet the minimum of {min_recalls} recalls.")
    else:
        st.dataframe(top_mfrs, use_container_width=True)
        fig = px.bar(
            top_mfrs.head(15),
            x="priority_score", y="manufacturer", orientation="h",
            color="class_i",
            color_continuous_scale="Reds",
            labels={"class_i": "Class I count"},
            title="Top Manufacturers (bar=priority score, color=Class I count)",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Repeat Class I Offenders")
    repeat = identifier.repeat_offenders(df)
    if repeat.empty:
        st.success("No firms with multiple Class I recalls in this window.")
    else:
        st.warning(f"{len(repeat)} firms have 2+ Class I recalls.")
        st.dataframe(repeat, use_container_width=True)

    st.subheader("Top Product Categories")
    top_cats = identifier.top_product_categories(df)
    if not top_cats.empty:
        st.dataframe(top_cats, use_container_width=True)
        fig_c = px.bar(
            top_cats.head(15),
            x="priority_score", y="product_category", orientation="h",
            color="class_i",
            color_continuous_scale="Reds",
            title="Top Product Categories by Priority Score",
        )
        fig_c.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_c, use_container_width=True)
