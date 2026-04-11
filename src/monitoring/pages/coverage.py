"""Coverage monitoring page — tracks data completeness across time and sources."""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.models.base import engine


def render():
    st.header("Data Coverage")

    with engine.connect() as conn:
        # Records per month by product type
        monthly_df = pd.read_sql(
            text("""
                SELECT
                    DATE_TRUNC('month', report_date)::date AS month,
                    product_type,
                    count(*) AS record_count
                FROM recalls
                WHERE report_date IS NOT NULL
                GROUP BY DATE_TRUNC('month', report_date), product_type
                ORDER BY month
            """),
            conn,
        )

        # Records by source over time
        source_df = pd.read_sql(
            text("""
                SELECT
                    DATE_TRUNC('month', report_date)::date AS month,
                    source,
                    count(*) AS record_count
                FROM recalls
                WHERE report_date IS NOT NULL
                GROUP BY DATE_TRUNC('month', report_date), source
                ORDER BY month
            """),
            conn,
        )

        # Latest record date per endpoint/type
        staleness_df = pd.read_sql(
            text("""
                SELECT
                    product_type,
                    source,
                    MAX(report_date) AS latest_date,
                    count(*) AS total_records
                FROM recalls
                GROUP BY product_type, source
                ORDER BY product_type, source
            """),
            conn,
        )

        # Overall summary
        summary = conn.execute(
            text("""
                SELECT
                    MIN(report_date) AS earliest,
                    MAX(report_date) AS latest,
                    count(*) AS total,
                    count(DISTINCT product_type) AS types
                FROM recalls
                WHERE report_date IS NOT NULL
            """)
        ).fetchone()

    # Summary KPIs
    if summary and summary[2]:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Records", f"{summary[2]:,}")
        col2.metric("Earliest Record", str(summary[0]))
        col3.metric("Latest Record", str(summary[1]))
        col4.metric("Product Types", summary[3])
    else:
        st.info("No records in database yet.")
        return

    # Heatmap: records per month x product_type
    if not monthly_df.empty:
        st.subheader("Monthly Record Count by Product Type")
        pivot = monthly_df.pivot_table(
            index="product_type", columns="month", values="record_count", fill_value=0
        )
        fig = px.imshow(
            pivot.values,
            x=[str(c) for c in pivot.columns],
            y=list(pivot.index),
            labels={"x": "Month", "y": "Product Type", "color": "Records"},
            title="Coverage Heatmap (records per month)",
            color_continuous_scale="YlOrRd",
            aspect="auto",
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

        # Gap detection
        st.subheader("Gap Detection")
        all_months = pd.date_range(
            monthly_df["month"].min(), monthly_df["month"].max(), freq="MS"
        )
        for ptype in monthly_df["product_type"].unique():
            type_months = set(monthly_df[monthly_df["product_type"] == ptype]["month"])
            missing = [m.date() for m in all_months if m.date() not in type_months]
            if missing:
                st.warning(
                    f"**{ptype}**: {len(missing)} months with zero records — "
                    f"{', '.join(str(m) for m in missing[:5])}"
                    f"{'...' if len(missing) > 5 else ''}"
                )
            else:
                st.success(f"**{ptype}**: No gaps detected")

    # Records by source
    if not source_df.empty:
        st.subheader("Records by Source Over Time")
        fig2 = px.area(
            source_df, x="month", y="record_count", color="source",
            title="Monthly Records by Source",
            labels={"month": "Month", "record_count": "Records", "source": "Source"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Staleness check
    if not staleness_df.empty:
        st.subheader("Data Staleness")
        st.dataframe(
            staleness_df.rename(columns={
                "product_type": "Product Type",
                "source": "Source",
                "latest_date": "Latest Record Date",
                "total_records": "Total Records",
            }),
            use_container_width=True,
        )
