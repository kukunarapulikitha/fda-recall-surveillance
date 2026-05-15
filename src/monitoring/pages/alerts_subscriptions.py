"""Alerts & Subscriptions — create/manage subscriptions and view notification history."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from src.alerts.subscriptions import (
    create_subscription,
    deactivate_subscription,
    list_subscriptions,
)
from src.models.base import SessionLocal, engine


# ── Helpers ─────────────────────────────────────────────────────────────────

def _status_badge(status: str) -> str:
    colour = {"sent": "🟢", "failed": "🔴", "pending": "🟡"}.get(status, "⚪")
    return f"{colour} {status.capitalize()}"


def _load_notifications(limit: int = 200) -> pd.DataFrame:
    q = text("""
        SELECT n.id, n.recall_number, n.recall_classification, n.recall_firm,
               n.recall_product_type, n.channel, n.status, n.error_message,
               n.sent_at, n.created_at,
               s.name AS subscription_name, s.email
        FROM   alert_notifications n
        JOIN   alert_subscriptions s ON s.id = n.subscription_id
        ORDER  BY n.created_at DESC
        LIMIT  :lim
    """)
    try:
        with engine.connect() as conn:
            return pd.read_sql(q, conn, params={"lim": limit})
    except Exception:
        return pd.DataFrame()


# ── Tab: Manage subscriptions ────────────────────────────────────────────────

def _render_manage() -> None:
    st.subheader("Active Subscriptions")

    try:
        session = SessionLocal()
        subs = list_subscriptions(session, active_only=False)
    except Exception as e:
        st.error(f"Could not load subscriptions: {e}")
        return
    finally:
        session.close()

    if not subs:
        st.info("No subscriptions yet — create one below.")
    else:
        for sub in subs:
            status = "✅ Active" if sub.active else "⏸ Inactive"
            channels = []
            if sub.email_enabled and sub.email:
                channels.append(f"📧 {sub.email}")
            if sub.webhook_enabled and sub.webhook_url:
                channels.append(f"🔗 webhook")
            label = f"{status}  |  **{sub.name}**  |  {' · '.join(channels) or 'No channels'}"

            with st.expander(label):
                c1, c2, c3 = st.columns(3)
                c1.write(f"**Product types:** {sub.product_types or 'All'}")
                c2.write(f"**Classifications:** {sub.classifications or 'All'}")
                c3.write(f"**Min risk score:** {sub.min_risk_score}")

                if sub.keywords:
                    st.write(f"**Keywords:** {', '.join(sub.keywords)}")
                if sub.states:
                    st.write(f"**States:** {', '.join(sub.states)}")
                if sub.webhook_url:
                    st.code(sub.webhook_url, language=None)

                if sub.active:
                    if st.button("Deactivate", key=f"deact_{sub.id}"):
                        try:
                            s2 = SessionLocal()
                            deactivate_subscription(s2, sub.id)
                            s2.close()
                            st.success("Subscription deactivated.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")

    st.divider()
    st.subheader("➕ Create New Subscription")
    _render_create_form()


def _render_create_form() -> None:
    with st.form("create_sub_form", clear_on_submit=True):
        name = st.text_input("Subscription name *", placeholder="e.g. Class I Drug Alerts")
        email = st.text_input("Email address", placeholder="you@example.com")
        webhook_url = st.text_input(
            "Webhook URL (optional)", placeholder="https://hooks.example.com/…"
        )

        col1, col2 = st.columns(2)
        product_types = col1.multiselect(
            "Product types (leave empty for all)",
            ["Drugs", "Devices", "Food"],
        )
        classifications = col2.multiselect(
            "Classifications (leave empty for all)",
            ["Class I", "Class II", "Class III"],
        )

        col3, col4 = st.columns(2)
        keywords_raw = col3.text_input(
            "Keywords (comma-separated)", placeholder="insulin, contamination"
        )
        states_raw = col4.text_input(
            "States to watch (comma-separated)", placeholder="CA, TX, NY"
        )
        min_risk = st.slider("Minimum risk score (0 = no filter)", 0, 100, 0, 5)

        ch1, ch2 = st.columns(2)
        email_enabled = ch1.checkbox("Enable email notifications", value=True)
        webhook_enabled = ch2.checkbox("Enable webhook notifications", value=False)

        submitted = st.form_submit_button("Create Subscription")

    if submitted:
        if not name.strip():
            st.error("Subscription name is required.")
            return
        if email_enabled and not email.strip():
            st.error("Email address is required when email notifications are enabled.")
            return
        if webhook_enabled and not webhook_url.strip():
            st.error("Webhook URL is required when webhook notifications are enabled.")
            return

        keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
        states = [s.strip().upper() for s in states_raw.split(",") if s.strip()]

        try:
            session = SessionLocal()
            sub = create_subscription(
                session,
                name=name.strip(),
                email=email.strip() or None,
                webhook_url=webhook_url.strip() or None,
                product_types=product_types,
                classifications=classifications,
                keywords=keywords,
                states=states,
                min_risk_score=min_risk,
                email_enabled=email_enabled,
                webhook_enabled=webhook_enabled,
            )
            st.success(f"Subscription **{sub.name}** created (id={sub.id}).")
            st.rerun()
        except Exception as e:
            st.error(f"Failed to create subscription: {e}")
        finally:
            session.close()


# ── Tab: Notification history ────────────────────────────────────────────────

def _render_history() -> None:
    st.subheader("Notification History (last 200)")

    df = _load_notifications()
    if df.empty:
        st.info("No notifications dispatched yet.")
        return

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total sent", int((df["status"] == "sent").sum()))
    c2.metric("Failed", int((df["status"] == "failed").sum()))
    c3.metric("Unique recalls alerted", df["recall_number"].nunique())
    c4.metric("Unique subscriptions", df["subscription_name"].nunique())

    # Status over time chart
    if "created_at" in df.columns and not df["created_at"].isna().all():
        df["day"] = pd.to_datetime(df["created_at"]).dt.date
        daily = (
            df.groupby(["day", "status"]).size().reset_index(name="count")
        )
        if not daily.empty:
            fig = px.bar(
                daily, x="day", y="count", color="status",
                color_discrete_map={"sent": "#2e7d32", "failed": "#c0392b", "pending": "#f9a825"},
                title="Daily Notifications by Status",
                barmode="stack",
            )
            st.plotly_chart(fig, use_container_width=True)

    # Filterable table
    status_filter = st.selectbox(
        "Filter by status", ["All", "sent", "failed", "pending"], key="notif_status"
    )
    display = df if status_filter == "All" else df[df["status"] == status_filter]

    display = display.copy()
    display["status_icon"] = display["status"].apply(_status_badge)

    st.dataframe(
        display[[
            "created_at", "subscription_name", "recall_number",
            "recall_classification", "recall_firm", "channel", "status_icon",
        ]].rename(columns={
            "created_at": "When",
            "subscription_name": "Subscription",
            "recall_number": "Recall #",
            "recall_classification": "Class",
            "recall_firm": "Firm",
            "channel": "Channel",
            "status_icon": "Status",
        }),
        use_container_width=True,
        height=400,
    )

    # Error details
    failed = display[display["status"] == "failed"]
    if not failed.empty:
        st.subheader("Failed notification details")
        for _, row in failed.iterrows():
            with st.expander(f"{row['recall_number']} — {row['channel']} — {row.get('When', '')}"):
                st.code(row.get("error_message") or "No error message recorded")


# ── Tab: How it works ────────────────────────────────────────────────────────

def _render_how_it_works() -> None:
    st.markdown("""
## How the Alert System Works

### Subscription Filters
Create a subscription to receive notifications whenever a new recall matches
**all** of your selected criteria:

| Filter | Behaviour |
|--------|-----------|
| **Product types** | Match only Drugs / Devices / Food (empty = all) |
| **Classifications** | Match only Class I / II / III (empty = all) |
| **Keywords** | Case-insensitive search across firm name, product description, brand/generic name, and recall reason |
| **States** | Match recalls originating from the selected states |
| **Min risk score** | Match only recalls with a composite risk score ≥ threshold |

### Notification Channels

| Channel | How to enable | Notes |
|---------|---------------|-------|
| **Email** | Add an email address + tick "Enable email" | Requires `SMTP_HOST` in `.env` |
| **Webhook** | Add a URL + tick "Enable webhook" | Receives a JSON POST body (see below) |

### Webhook Payload Example

```json
{
  "event": "fda_recall_alert",
  "subscription": "Class I Drug Alerts",
  "recall": {
    "recall_number": "D-0179-2024",
    "product_type": "Drugs",
    "classification": "Class I",
    "recalling_firm": "Acme Pharma Inc.",
    "reason_for_recall": "Presence of foreign particulate matter",
    "report_date": "2024-03-15",
    "state": "CA",
    "status": "Ongoing"
  }
}
```

### Email Configuration (`.env`)

```
ALERTS_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USE_TLS=true
SMTP_USERNAME=you@gmail.com
SMTP_PASSWORD=your-app-password
ALERT_EMAIL_FROM=fda-alerts@yourdomain.com
```

> **Tip:** For Gmail, generate an App Password under Google Account → Security → 2-Step Verification → App passwords.
""")


# ── Main render ──────────────────────────────────────────────────────────────

def render() -> None:
    st.header("🔔 Alerts & Subscriptions")
    st.markdown(
        "Receive automatic notifications whenever new FDA recalls match your criteria. "
        "Supports **email** and **webhook** channels."
    )

    tab_manage, tab_history, tab_docs = st.tabs([
        "Manage Subscriptions", "Notification History", "How It Works",
    ])

    with tab_manage:
        _render_manage()
    with tab_history:
        _render_history()
    with tab_docs:
        _render_how_it_works()
