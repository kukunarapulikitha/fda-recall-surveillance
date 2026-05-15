"""Alert subscription and notification models."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from src.models.base import Base


class AlertSubscription(Base):
    """Represents a user's alert subscription profile."""

    __tablename__ = "alert_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Who gets notified
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    email: Mapped[str | None] = mapped_column(String(500))
    webhook_url: Mapped[str | None] = mapped_column(Text)

    # Filter criteria (empty list means "match all")
    product_types: Mapped[list] = mapped_column(JSONB, default=list)
    classifications: Mapped[list] = mapped_column(JSONB, default=list)
    keywords: Mapped[list] = mapped_column(JSONB, default=list)
    states: Mapped[list] = mapped_column(JSONB, default=list)
    min_risk_score: Mapped[int] = mapped_column(Integer, default=0)

    # Channel switches
    email_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    webhook_enabled: Mapped[bool] = mapped_column(Boolean, default=False)

    # Status
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationship
    notifications: Mapped[list[AlertNotification]] = relationship(
        back_populates="subscription", lazy="select"
    )

    def matches(self, recall: dict) -> bool:
        """Return True if this subscription's filters match the given recall dict."""
        # Product type filter
        if self.product_types and recall.get("product_type") not in self.product_types:
            return False

        # Classification filter
        if self.classifications and recall.get("classification") not in self.classifications:
            return False

        # State filter
        if self.states and recall.get("state") not in self.states:
            return False

        # Risk score filter
        risk_score = recall.get("risk_score", 0) or 0
        if risk_score < self.min_risk_score:
            return False

        # Keyword filter — match against product_description + reason_for_recall
        if self.keywords:
            text_blob = " ".join(
                str(recall.get(f, "") or "").lower()
                for f in ("product_description", "reason_for_recall", "brand_name",
                          "recalling_firm", "generic_name")
            )
            if not any(kw.lower() in text_blob for kw in self.keywords):
                return False

        return True


class AlertNotification(Base):
    """Record of a dispatched alert notification."""

    __tablename__ = "alert_notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subscription_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("alert_subscriptions.id", ondelete="CASCADE"), nullable=False
    )
    recall_number: Mapped[str] = mapped_column(String(50), nullable=False)
    recall_classification: Mapped[str | None] = mapped_column(String(20))
    recall_firm: Mapped[str | None] = mapped_column(String(500))
    recall_product_type: Mapped[str | None] = mapped_column(String(20))
    channel: Mapped[str] = mapped_column(String(20), nullable=False)  # email / webhook
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending / sent / failed
    error_message: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Relationship
    subscription: Mapped[AlertSubscription] = relationship(back_populates="notifications")
