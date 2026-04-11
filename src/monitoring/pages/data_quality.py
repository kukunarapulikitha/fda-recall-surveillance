"""Data Quality monitoring page."""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.models.base import engine


def render():
    st.header("Data Quality")

    with engine.connect() as conn:
        # Total records
        total = conn.execute(text("SELECT count(*) FROM recalls")).scalar()
        validated = conn.execute(
            text("SELECT count(*) FROM recalls WHERE is_validated = true")
        ).scalar()
        unvalidated = total - validated if total else 0

        # Null rates per key column
        null_rates_query = text("""
            SELECT
                count(*) AS total,
                count(*) FILTER (WHERE classification IS NULL) AS null_classification,
                count(*) FILTER (WHERE product_description IS NULL OR product_description = '') AS null_product_desc,
                count(*) FILTER (WHERE distribution_pattern IS NULL) AS null_distribution,
                count(*) FILTER (WHERE report_date IS NULL) AS null_report_date,
                count(*) FILTER (WHERE recall_initiation_date IS NULL) AS null_initiation_date,
                count(*) FILTER (WHERE brand_name IS NULL) AS null_brand_name,
                count(*) FILTER (WHERE generic_name IS NULL) AS null_generic_name,
                count(*) FILTER (WHERE code_info IS NULL OR code_info = '') AS null_code_info,
                count(*) FILTER (WHERE product_quantity IS NULL) AS null_product_qty
            FROM recalls
        """)
        null_row = conn.execute(null_rates_query).fetchone()

        # Validation failures over time
        failures_df = pd.read_sql(
            text("""
                SELECT DATE(started_at) AS date,
                       SUM(validation_failures) AS failures
                FROM ingestion_logs
                WHERE started_at IS NOT NULL
                GROUP BY DATE(started_at)
                ORDER BY date
            """),
            conn,
        )

        # Daily record counts for anomaly detection
        daily_counts = pd.read_sql(
            text("""
                SELECT report_date AS date, count(*) AS record_count
                FROM recalls
                WHERE report_date IS NOT NULL
                GROUP BY report_date
                ORDER BY report_date
            """),
            conn,
        )

    # KPI cards
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", f"{total:,}" if total else 0)
    col2.metric("Validated", f"{validated:,}" if validated else 0)
    col3.metric("Unvalidated", f"{unvalidated:,}" if unvalidated else 0)

    if not null_row or not null_row[0]:
        st.info("No records in database yet.")
        return

    # Null rates chart
    st.subheader("Null Rates by Column")
    total_count = null_row[0]
    columns = [
        "classification", "product_description", "distribution_pattern",
        "report_date", "recall_initiation_date", "brand_name",
        "generic_name", "code_info", "product_quantity",
    ]
    null_counts = list(null_row[1:])
    null_pcts = [round(c / total_count * 100, 1) if total_count > 0 else 0 for c in null_counts]

    null_df = pd.DataFrame({"column": columns, "null_pct": null_pcts})
    null_df = null_df.sort_values("null_pct", ascending=True)

    fig = px.bar(
        null_df, x="null_pct", y="column", orientation="h",
        title="Null Rate by Column (%)",
        labels={"null_pct": "Null %", "column": "Column"},
        color="null_pct",
        color_continuous_scale=["green", "yellow", "red"],
        range_color=[0, 100],
    )
    fig.add_vline(x=10, line_dash="dash", line_color="red", annotation_text="10% threshold")
    st.plotly_chart(fig, use_container_width=True)

    # Validation failures over time
    if not failures_df.empty:
        st.subheader("Validation Failures Over Time")
        fig2 = px.line(
            failures_df, x="date", y="failures",
            title="Daily Validation Failures",
            labels={"date": "Date", "failures": "Failure Count"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Anomaly detection
    if not daily_counts.empty and len(daily_counts) > 7:
        st.subheader("Record Count Anomalies")
        mean_count = daily_counts["record_count"].mean()
        std_count = daily_counts["record_count"].std()
        daily_counts["is_anomaly"] = (
            (daily_counts["record_count"] > mean_count + 2 * std_count)
            | (daily_counts["record_count"] < mean_count - 2 * std_count)
        )
        anomalies = daily_counts[daily_counts["is_anomaly"]]

        fig3 = px.scatter(
            daily_counts, x="date", y="record_count",
            color="is_anomaly",
            color_discrete_map={True: "red", False: "blue"},
            title=f"Daily Record Counts (mean={mean_count:.0f}, anomalies shown in red)",
            labels={"date": "Report Date", "record_count": "Records"},
        )
        fig3.add_hline(y=mean_count + 2 * std_count, line_dash="dash", line_color="red")
        fig3.add_hline(y=max(0, mean_count - 2 * std_count), line_dash="dash", line_color="red")
        st.plotly_chart(fig3, use_container_width=True)

        if not anomalies.empty:
            st.warning(f"Found {len(anomalies)} anomalous days")
            st.dataframe(anomalies[["date", "record_count"]], use_container_width=True)
