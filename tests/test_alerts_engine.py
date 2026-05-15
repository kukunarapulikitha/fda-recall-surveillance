"""Unit tests for the alert engine — subscription matching and dispatch logic."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.alerts.engine import AlertEngine
from src.models.alert import AlertSubscription


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _make_sub(**kwargs) -> AlertSubscription:
    defaults = dict(
        id=1,
        name="Test Sub",
        email="user@example.com",
        webhook_url=None,
        product_types=[],
        classifications=[],
        keywords=[],
        states=[],
        min_risk_score=0,
        email_enabled=True,
        webhook_enabled=False,
        active=True,
    )
    defaults.update(kwargs)
    sub = MagicMock(spec=AlertSubscription)
    for k, v in defaults.items():
        setattr(sub, k, v)
    # Delegate matches() to the real implementation
    sub.matches = lambda recall: AlertSubscription.matches(sub, recall)
    return sub


_RECALL = {
    "recall_number": "D-0001-2024",
    "product_type": "Drugs",
    "classification": "Class I",
    "recalling_firm": "Acme Pharma",
    "reason_for_recall": "Presence of glass particles (contamination)",
    "product_description": "Insulin 10mg tablets",
    "state": "CA",
    "risk_score": 80,
    "report_date": date(2024, 3, 1),
}


# ── AlertSubscription.matches() ───────────────────────────────────────────────

class TestSubscriptionMatches:
    def test_empty_filters_match_all(self):
        sub = _make_sub()
        assert sub.matches(_RECALL) is True

    def test_product_type_filter_match(self):
        sub = _make_sub(product_types=["Drugs"])
        assert sub.matches(_RECALL) is True

    def test_product_type_filter_no_match(self):
        sub = _make_sub(product_types=["Devices"])
        assert sub.matches(_RECALL) is False

    def test_classification_filter_match(self):
        sub = _make_sub(classifications=["Class I"])
        assert sub.matches(_RECALL) is True

    def test_classification_filter_no_match(self):
        sub = _make_sub(classifications=["Class III"])
        assert sub.matches(_RECALL) is False

    def test_state_filter_match(self):
        sub = _make_sub(states=["CA", "TX"])
        assert sub.matches(_RECALL) is True

    def test_state_filter_no_match(self):
        sub = _make_sub(states=["NY"])
        assert sub.matches(_RECALL) is False

    def test_min_risk_score_match(self):
        sub = _make_sub(min_risk_score=50)
        assert sub.matches(_RECALL) is True

    def test_min_risk_score_no_match(self):
        sub = _make_sub(min_risk_score=90)
        assert sub.matches(_RECALL) is False

    def test_keyword_match_in_reason(self):
        sub = _make_sub(keywords=["contamination"])
        assert sub.matches(_RECALL) is True

    def test_keyword_match_in_firm(self):
        sub = _make_sub(keywords=["acme"])
        assert sub.matches(_RECALL) is True

    def test_keyword_no_match(self):
        sub = _make_sub(keywords=["paclitaxel"])
        assert sub.matches(_RECALL) is False

    def test_keyword_case_insensitive(self):
        sub = _make_sub(keywords=["INSULIN"])
        assert sub.matches(_RECALL) is True

    def test_combined_filters_all_match(self):
        sub = _make_sub(
            product_types=["Drugs"],
            classifications=["Class I"],
            keywords=["glass"],
            states=["CA"],
            min_risk_score=60,
        )
        assert sub.matches(_RECALL) is True

    def test_combined_filters_one_miss(self):
        sub = _make_sub(
            product_types=["Drugs"],
            classifications=["Class I"],
            keywords=["salmonella"],  # doesn't match
        )
        assert sub.matches(_RECALL) is False

    def test_missing_risk_score_defaults_to_zero(self):
        recall = {**_RECALL, "risk_score": None}
        sub = _make_sub(min_risk_score=0)
        assert sub.matches(recall) is True


# ── AlertEngine.process_recalls() ────────────────────────────────────────────

class TestAlertEngine:
    def _make_engine(self, subs):
        session = MagicMock()
        engine = AlertEngine(session)
        with patch("src.alerts.engine.list_subscriptions", return_value=subs):
            yield engine

    def test_no_recalls_returns_zero(self):
        session = MagicMock()
        eng = AlertEngine(session)
        with patch("src.alerts.engine.list_subscriptions", return_value=[]):
            result = eng.process_recalls([])
        assert result == 0

    def test_no_active_subscriptions_returns_zero(self):
        session = MagicMock()
        eng = AlertEngine(session)
        with patch("src.alerts.engine.list_subscriptions", return_value=[]):
            result = eng.process_recalls([_RECALL])
        assert result == 0

    def test_email_dispatch_success(self):
        sub = _make_sub(email_enabled=True, webhook_enabled=False)
        session = MagicMock()
        # Make flush/add/commit no-ops; fake notification object
        notif = MagicMock()
        session.add = MagicMock()
        session.flush = MagicMock()
        session.commit = MagicMock()

        eng = AlertEngine(session)
        with (
            patch("src.alerts.engine.list_subscriptions", return_value=[sub]),
            patch("src.alerts.engine.send_email") as mock_send,
            patch("src.alerts.engine.AlertNotification") as MockNotif,
        ):
            MockNotif.return_value = notif
            result = eng.process_recalls([_RECALL])

        mock_send.assert_called_once_with(sub.email, sub.name, _RECALL)

    def test_email_dispatch_failure_records_error(self):
        sub = _make_sub(email_enabled=True, webhook_enabled=False)
        session = MagicMock()
        notif = MagicMock()
        session.add = MagicMock()
        session.flush = MagicMock()
        session.commit = MagicMock()

        eng = AlertEngine(session)
        with (
            patch("src.alerts.engine.list_subscriptions", return_value=[sub]),
            patch("src.alerts.engine.send_email", side_effect=RuntimeError("SMTP failure")),
            # Patch the class where the engine module imported it from
            patch("src.alerts.engine.AlertNotification") as MockNotif,
        ):
            MockNotif.return_value = notif
            result = eng.process_recalls([_RECALL])

        assert notif.status == "failed"
        assert "SMTP failure" in (notif.error_message or "")

    def test_webhook_dispatch_called(self):
        sub = _make_sub(
            email_enabled=False,
            webhook_enabled=True,
            webhook_url="https://hooks.example.com/abc",
        )
        session = MagicMock()
        notif = MagicMock()
        session.add = MagicMock()
        session.flush = MagicMock()
        session.commit = MagicMock()

        eng = AlertEngine(session)
        with (
            patch("src.alerts.engine.list_subscriptions", return_value=[sub]),
            patch("src.alerts.engine.send_webhook") as mock_wh,
            patch("src.alerts.engine.AlertNotification") as MockNotif,
        ):
            MockNotif.return_value = notif
            eng.process_recalls([_RECALL])

        mock_wh.assert_called_once_with(sub.webhook_url, sub.name, _RECALL)

    def test_no_dispatch_if_no_channels(self):
        sub = _make_sub(email_enabled=False, webhook_enabled=False)
        session = MagicMock()
        eng = AlertEngine(session)
        with (
            patch("src.alerts.engine.list_subscriptions", return_value=[sub]),
            patch("src.alerts.engine.send_email") as mock_email,
            patch("src.alerts.engine.send_webhook") as mock_wh,
        ):
            eng.process_recalls([_RECALL])

        mock_email.assert_not_called()
        mock_wh.assert_not_called()
