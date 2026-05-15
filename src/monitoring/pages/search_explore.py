"""Search & Explore — user-friendly recall search with full-text filtering.

Designed for non-technical stakeholders: keyword search, point-and-click filters,
clear risk badges, and paginated recall cards.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.models.base import engine


# ── Classification badge colours ────────────────────────────────────────────
_CLASS_BADGE = {
    "Class I":   ("🔴", "#fdd", "#c0392b"),
    "Class II":  ("🟠", "#ffe8d6", "#e65100"),
    "Class III": ("🟡", "#fffde7", "#f9a825"),
}


def _badge(classification: str | None) -> str:
    icon, bg, fg = _CLASS_BADGE.get(classification or "", ("⚪", "#f0f0f0", "#666"))
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;border-radius:4px;'
        f'font-weight:bold;font-size:0.85em;">{icon} {classification or "Unclassified"}</span>'
    )


def _load(
    keyword: str,
    start: date,
    end: date,
    product_types: list[str],
    classifications: list[str],
    states: list[str],
    limit: int = 500,
) -> pd.DataFrame:
    filters = ["report_date BETWEEN :start AND :end"]
    params: dict = {"start": start, "end": end}

    if product_types:
        filters.append("product_type = ANY(:ptypes)")
        params["ptypes"] = product_types

    if classifications:
        filters.append("classification = ANY(:cls)")
        params["cls"] = classifications

    if states:
        filters.append("state = ANY(:states)")
        params["states"] = states

    if keyword.strip():
        # Simple case-insensitive full-text search across key text columns
        filters.append(
            "(LOWER(reason_for_recall) LIKE :kw OR LOWER(product_description) LIKE :kw "
            "OR LOWER(recalling_firm) LIKE :kw OR LOWER(brand_name) LIKE :kw "
            "OR LOWER(generic_name) LIKE :kw OR recall_number ILIKE :kw)"
        )
        params["kw"] = f"%{keyword.lower()}%"

    where = " AND ".join(filters)
    query = text(f"""
        SELECT recall_number, product_type, classification, status,
               recalling_firm, brand_name, generic_name,
               reason_for_recall, product_description, distribution_pattern,
               report_date, recall_initiation_date, state, country
        FROM   recalls
        WHERE  {where}
        ORDER  BY report_date DESC, recall_number
        LIMIT  :lim
    """)
    params["lim"] = limit

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def _recall_card(row: pd.Series, idx: int) -> None:
    """Render a single recall as an expander card."""
    firm = row.get("recalling_firm") or "Unknown firm"
    cls = row.get("classification")
    rn = row.get("recall_number") or ""
    ptype = row.get("product_type") or ""

    label = f"{rn}  |  {firm[:55]}{'…' if len(firm) > 55 else ''}"
    with st.expander(label, expanded=(idx == 0)):
        col1, col2, col3 = st.columns([2, 1, 1])
        col1.markdown(f"**Firm:** {firm}")
        col2.markdown(f"**Product type:** {ptype}")
        col3.markdown(_badge(cls), unsafe_allow_html=True)

        if row.get("brand_name") or row.get("generic_name"):
            st.markdown(
                f"**Product:** {row.get('brand_name') or ''} "
                f"{'/ ' + row.get('generic_name') if row.get('generic_name') else ''}"
            )

        st.markdown(f"**Reason for recall:**  \n{row.get('reason_for_recall') or '—'}")

        if row.get("product_description"):
            st.markdown(f"**Product description:**  \n{row.get('product_description')}")

        meta1, meta2, meta3, meta4 = st.columns(4)
        meta1.markdown(f"📅 **Reported:** {row.get('report_date') or '—'}")
        meta2.markdown(f"🏭 **Initiated:** {row.get('recall_initiation_date') or '—'}")
        meta3.markdown(f"📍 **State:** {row.get('state') or '—'}")
        meta4.markdown(f"🔖 **Status:** {row.get('status') or '—'}")

        if row.get("distribution_pattern"):
            st.caption(f"Distribution: {row['distribution_pattern']}")


def render() -> None:
    st.header("🔍 Search & Explore Recalls")
    st.markdown(
        "Search the full recall database with plain-English keywords or use the "
        "filters below. Click any result to expand the full record."
    )

    # ── Filter bar ───────────────────────────────────────────────────────────
    with st.container():
        kw_col, date_col1, date_col2 = st.columns([3, 1, 1])
        keyword = kw_col.text_input(
            "🔎 Keyword search",
            placeholder="e.g. listeria, insulin, pacemaker, contamination …",
            key="se_keyword",
        )
        default_start = date.today() - timedelta(days=365 * 5)
        start = date_col1.date_input("From", value=default_start, key="se_start")
        end = date_col2.date_input("To", value=date.today(), key="se_end")

    col_a, col_b, col_c = st.columns(3)
    product_types = col_a.multiselect(
        "Product type",
        ["Drugs", "Devices", "Food"],
        default=[],
        key="se_ptypes",
    )
    classifications = col_b.multiselect(
        "Classification",
        ["Class I", "Class II", "Class III"],
        default=[],
        key="se_cls",
    )
    # Build state list dynamically from DB (short list, cached)
    @st.cache_data(ttl=600)
    def _states() -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT DISTINCT state FROM recalls WHERE state IS NOT NULL ORDER BY state")
            ).fetchall()
        return [r[0] for r in rows if r[0]]

    try:
        state_options = _states()
    except Exception:
        state_options = []

    states = col_c.multiselect(
        "State / Region",
        state_options,
        default=[],
        key="se_states",
    )

    # ── Results ──────────────────────────────────────────────────────────────
    try:
        df = _load(keyword, start, end, product_types, classifications, states)
    except Exception as exc:
        st.error(f"Database error: {exc}")
        return

    if df.empty:
        st.info("No recalls match your search. Try broadening the date range or removing filters.")
        return

    # Summary bar
    total = len(df)
    cls1 = int((df["classification"] == "Class I").sum())
    firms = df["recalling_firm"].nunique()
    capped = total >= 500

    cols = st.columns(4)
    cols[0].metric("Results shown", f"{total:,}" + (" (capped)" if capped else ""))
    cols[1].metric("Class I", cls1)
    cols[2].metric("Unique firms", firms)
    cols[3].metric(
        "Date range",
        f"{df['report_date'].min()} → {df['report_date'].max()}",
    )

    if capped:
        st.warning(
            "Showing first 500 results. Add more filters or narrow the date range to see all records."
        )

    # Download
    csv_bytes = df.to_csv(index=False).encode()
    st.download_button(
        "⬇️ Download results (CSV)",
        data=csv_bytes,
        file_name=f"fda-recalls-{date.today()}.csv",
        mime="text/csv",
    )

    st.markdown("---")

    # Pagination
    page_size = 20
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="se_page")
    page_df = df.iloc[(page - 1) * page_size: page * page_size]

    st.markdown(f"**Page {page} of {total_pages}**")
    for i, (_, row) in enumerate(page_df.iterrows()):
        _recall_card(row, i)
