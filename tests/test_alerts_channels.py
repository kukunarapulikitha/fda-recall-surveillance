"""Unit tests for email and webhook notification channels."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.alerts.channels.webhook_channel import _recall_payload, send_webhook


_RECALL = {
    "recall_number": "D-0002-2024",
    "product_type": "Drugs",
    "classification": "Class I",
    "recalling_firm": "Beta Pharma LLC",
    "reason_for_recall": "Sub-potent product",
    "product_description": "Metformin 500mg",
    "distribution_pattern": "Nationwide",
    "report_date": date(2024, 4, 10),
    "recall_initiation_date": date(2024, 4, 5),
    "state": "TX",
    "status": "Ongoing",
}


# ── Webhook channel ───────────────────────────────────────────────────────────

class TestWebhookChannel:
    def test_payload_structure(self):
        payload = _recall_payload(_RECALL, "My Sub")
        assert payload["event"] == "fda_recall_alert"
        assert payload["subscription"] == "My Sub"
        assert payload["recall"]["recall_number"] == "D-0002-2024"
        assert payload["recall"]["classification"] == "Class I"
        assert payload["recall"]["report_date"] == "2024-04-10"  # ISO string

    def test_payload_none_dates_become_none(self):
        recall = {**_RECALL, "report_date": None}
        payload = _recall_payload(recall, "Sub")
        assert payload["recall"]["report_date"] is None

    def test_payload_date_objects_serialised(self):
        payload = _recall_payload(_RECALL, "Sub")
        # Both date fields should be ISO strings, not date objects
        assert isinstance(payload["recall"]["report_date"], str)
        assert isinstance(payload["recall"]["recall_initiation_date"], str)

    def test_send_webhook_success(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with patch("src.alerts.channels.webhook_channel.httpx.post", return_value=mock_resp) as mock_post:
            send_webhook("https://hooks.example.com/test", "My Sub", _RECALL)

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == "https://hooks.example.com/test"
        assert call_kwargs[1]["json"]["event"] == "fda_recall_alert"

    def test_send_webhook_raises_on_http_error(self):
        import httpx

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock()
        )

        with patch("src.alerts.channels.webhook_channel.httpx.post", return_value=mock_resp):
            with pytest.raises(httpx.HTTPStatusError):
                send_webhook("https://hooks.example.com/fail", "Sub", _RECALL)

    def test_send_webhook_raises_on_connection_error(self):
        import httpx

        with patch(
            "src.alerts.channels.webhook_channel.httpx.post",
            side_effect=httpx.ConnectError("connection refused"),
        ):
            with pytest.raises(httpx.ConnectError):
                send_webhook("https://dead.example.com/", "Sub", _RECALL)


# ── Email channel ─────────────────────────────────────────────────────────────

class TestEmailChannel:
    def test_email_raises_when_smtp_host_empty(self):
        from src.alerts.channels.email_channel import send_email

        with patch("src.alerts.channels.email_channel.settings") as mock_settings:
            mock_settings.SMTP_HOST = ""
            with pytest.raises(RuntimeError, match="SMTP_HOST not configured"):
                send_email("user@example.com", "Sub", _RECALL)

    def test_email_builds_message_with_classification(self):
        from src.alerts.channels.email_channel import _build_message

        with patch("src.alerts.channels.email_channel.settings") as mock_settings:
            mock_settings.ALERT_EMAIL_FROM = "alerts@fda.example.com"
            msg = _build_message("user@example.com", "Class I Alerts", _RECALL)

        assert "Class I" in msg["Subject"]
        assert "Beta Pharma" in msg["Subject"]

    def test_email_subject_truncates_long_firm_names(self):
        from src.alerts.channels.email_channel import _build_message

        long_recall = {**_RECALL, "recalling_firm": "A" * 200}
        with patch("src.alerts.channels.email_channel.settings") as mock_settings:
            mock_settings.ALERT_EMAIL_FROM = "alerts@fda.example.com"
            msg = _build_message("user@example.com", "Sub", long_recall)

        # Subject should not contain all 200 chars
        assert len(msg["Subject"]) < 300

    def test_send_email_calls_smtp(self):
        from src.alerts.channels.email_channel import send_email

        mock_server = MagicMock()
        with (
            patch("src.alerts.channels.email_channel.settings") as mock_settings,
            patch("src.alerts.channels.email_channel.smtplib.SMTP", return_value=mock_server),
        ):
            mock_settings.SMTP_HOST = "smtp.example.com"
            mock_settings.SMTP_PORT = 587
            mock_settings.SMTP_USE_TLS = True
            mock_settings.SMTP_USERNAME = "user"
            mock_settings.SMTP_PASSWORD = "pass"
            mock_settings.ALERT_EMAIL_FROM = "from@example.com"

            send_email("to@example.com", "Sub", _RECALL)

        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
