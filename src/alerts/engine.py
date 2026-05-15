"""Alert engine — matches new recalls against subscriptions and dispatches notifications.

Usage (called from the ingestion pipeline after upsert):

    from src.alerts.engine import AlertEngine
    engine = AlertEngine(session)
    engine.process_recalls(newly_inserted_records)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from src.alerts.channels.email_channel import send_email
from src.alerts.channels.webhook_channel import send_webhook
from src.alerts.subscriptions import list_subscriptions
from src.config import settings
from src.models.alert import AlertNotification  # noqa: F401 (re-exported for patching)

logger = logging.getLogger(__name__)


class AlertEngine:
    """Evaluate recalls against active subscriptions and send notifications."""

    def __init__(self, session: Session):
        self.session = session

    def process_recalls(self, recalls: list[dict]) -> int:
        """
        Match each recall against active subscriptions.

        Returns the total number of notifications dispatched.
        """
        if not recalls:
            return 0

        subscriptions = list_subscriptions(self.session, active_only=True)
        if not subscriptions:
            logger.debug("No active subscriptions — skipping alert evaluation")
            return 0

        dispatched = 0
        for recall in recalls:
            for sub in subscriptions:
                if not sub.matches(recall):
                    continue
                dispatched += self._dispatch(sub, recall)

        logger.info("Alert engine: evaluated %d recalls, dispatched %d notifications",
                    len(recalls), dispatched)
        return dispatched

    def _dispatch(self, sub, recall: dict) -> int:
        """Send all enabled channels for this (subscription, recall) pair."""
        sent = 0

        if sub.email_enabled and sub.email:
            sent += self._send_channel(sub, recall, "email")

        if sub.webhook_enabled and sub.webhook_url:
            sent += self._send_channel(sub, recall, "webhook")

        return sent

    def _send_channel(self, sub, recall: dict, channel: str) -> int:
        notif = AlertNotification(
            subscription_id=sub.id,
            recall_number=recall.get("recall_number", ""),
            recall_classification=recall.get("classification"),
            recall_firm=recall.get("recalling_firm"),
            recall_product_type=recall.get("product_type"),
            channel=channel,
            status="pending",
        )
        self.session.add(notif)
        self.session.flush()  # get notif.id

        try:
            if channel == "email":
                send_email(sub.email, sub.name, recall)
            elif channel == "webhook":
                send_webhook(sub.webhook_url, sub.name, recall)
            else:
                raise ValueError(f"Unknown channel: {channel}")

            notif.status = "sent"
            notif.sent_at = datetime.now(tz=timezone.utc)
            self.session.commit()
            return 1

        except Exception as exc:
            logger.warning(
                "Alert dispatch failed [sub=%d, recall=%s, channel=%s]: %s",
                sub.id, recall.get("recall_number"), channel, exc,
            )
            notif.status = "failed"
            notif.error_message = str(exc)[:500]
            self.session.commit()
            return 0


def evaluate_and_alert(session: Session, recalls: list[dict]) -> int:
    """Convenience wrapper used by the pipeline."""
    if not settings.ALERTS_ENABLED:
        return 0
    return AlertEngine(session).process_recalls(recalls)
