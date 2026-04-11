"""Pipeline Health monitoring page."""

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.models.base import engine


def render():
    st.header("Pipeline Health")

    with engine.connect() as conn:
        # Recent ingestion runs
        runs_df = pd.read_sql(
            text("""
                SELECT id, run_type, source, endpoint, date_range_start, date_range_end,
                       started_at, finished_at, records_fetched, records_inserted,
                       records_updated, records_skipped, validation_failures,
                       status, error_message, duration_seconds
                FROM ingestion_logs
                ORDER BY started_at DESC
                LIMIT 100
            """),
            conn,
        )

    if runs_df.empty:
        st.info("No pipeline runs recorded yet. Run the daily pipeline or backfill first.")
        return

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    total_runs = len(runs_df)
    success_runs = len(runs_df[runs_df["status"] == "success"])
    failed_runs = len(runs_df[runs_df["status"] == "failed"])
    avg_duration = runs_df["duration_seconds"].dropna().mean()

    col1.metric("Total Runs (last 100)", total_runs)
    col2.metric("Successful", success_runs)
    col3.metric("Failed", failed_runs)
    col4.metric("Avg Duration (s)", f"{avg_duration:.1f}" if pd.notna(avg_duration) else "N/A")

    # Success rate over time
    if "started_at" in runs_df.columns and not runs_df["started_at"].isna().all():
        runs_df["date"] = pd.to_datetime(runs_df["started_at"]).dt.date
        daily_stats = runs_df.groupby("date").agg(
            total=("status", "count"),
            successes=("status", lambda x: (x == "success").sum()),
        ).reset_index()
        daily_stats["success_rate"] = (
            daily_stats["successes"] / daily_stats["total"] * 100
        )
        fig = px.line(
            daily_stats, x="date", y="success_rate",
            title="Daily Success Rate (%)",
            labels={"date": "Date", "success_rate": "Success Rate (%)"},
        )
        fig.update_yaxes(range=[0, 105])
        st.plotly_chart(fig, use_container_width=True)

    # Duration trend
    if not runs_df["duration_seconds"].isna().all():
        fig2 = px.scatter(
            runs_df.dropna(subset=["duration_seconds"]),
            x="started_at", y="duration_seconds", color="endpoint",
            title="Run Duration Over Time",
            labels={"started_at": "Start Time", "duration_seconds": "Duration (s)"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Recent runs table
    st.subheader("Recent Runs")
    display_cols = [
        "run_type", "source", "endpoint", "started_at", "status",
        "records_fetched", "records_inserted", "records_updated",
        "validation_failures", "duration_seconds",
    ]
    st.dataframe(
        runs_df[display_cols].style.applymap(
            lambda v: "background-color: #d4edda" if v == "success"
            else "background-color: #f8d7da" if v == "failed" else "",
            subset=["status"],
        ),
        use_container_width=True,
        height=400,
    )

    # Error log viewer
    failed_df = runs_df[runs_df["status"] == "failed"]
    if not failed_df.empty:
        st.subheader("Error Log")
        endpoint_filter = st.selectbox(
            "Filter by endpoint",
            ["All"] + list(failed_df["endpoint"].unique()),
        )
        if endpoint_filter != "All":
            failed_df = failed_df[failed_df["endpoint"] == endpoint_filter]
        for _, row in failed_df.iterrows():
            with st.expander(f"{row['started_at']} — {row['endpoint']}"):
                st.code(row.get("error_message", "No error message"))
