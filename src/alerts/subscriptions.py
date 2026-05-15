"""CRUD helpers for alert subscriptions."""

from __future__ import annotations

from sqlalchemy.orm import Session

from src.models.alert import AlertSubscription


# ── Create ──────────────────────────────────────────────────────────────────

def create_subscription(
    session: Session,
    *,
    name: str,
    email: str | None = None,
    webhook_url: str | None = None,
    product_types: list[str] | None = None,
    classifications: list[str] | None = None,
    keywords: list[str] | None = None,
    states: list[str] | None = None,
    min_risk_score: int = 0,
    email_enabled: bool = True,
    webhook_enabled: bool = False,
) -> AlertSubscription:
    """Create and persist a new subscription. Returns the saved object."""
    sub = AlertSubscription(
        name=name,
        email=email,
        webhook_url=webhook_url,
        product_types=product_types or [],
        classifications=classifications or [],
        keywords=keywords or [],
        states=states or [],
        min_risk_score=min_risk_score,
        email_enabled=email_enabled,
        webhook_enabled=webhook_enabled,
        active=True,
    )
    session.add(sub)
    session.commit()
    session.refresh(sub)
    return sub


# ── Read ────────────────────────────────────────────────────────────────────

def list_subscriptions(session: Session, active_only: bool = True) -> list[AlertSubscription]:
    q = session.query(AlertSubscription)
    if active_only:
        q = q.filter(AlertSubscription.active.is_(True))
    return q.order_by(AlertSubscription.created_at.desc()).all()


def get_subscription(session: Session, sub_id: int) -> AlertSubscription | None:
    return session.get(AlertSubscription, sub_id)


# ── Update ──────────────────────────────────────────────────────────────────

def update_subscription(
    session: Session,
    sub_id: int,
    **kwargs,
) -> AlertSubscription | None:
    sub = session.get(AlertSubscription, sub_id)
    if sub is None:
        return None
    for key, val in kwargs.items():
        if hasattr(sub, key):
            setattr(sub, key, val)
    session.commit()
    session.refresh(sub)
    return sub


def deactivate_subscription(session: Session, sub_id: int) -> bool:
    sub = session.get(AlertSubscription, sub_id)
    if sub is None:
        return False
    sub.active = False
    session.commit()
    return True


# ── Delete ──────────────────────────────────────────────────────────────────

def delete_subscription(session: Session, sub_id: int) -> bool:
    sub = session.get(AlertSubscription, sub_id)
    if sub is None:
        return False
    session.delete(sub)
    session.commit()
    return True
