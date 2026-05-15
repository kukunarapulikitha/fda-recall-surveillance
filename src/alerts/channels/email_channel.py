"""Email notification channel via SMTP (stdlib smtplib — no extra dependencies)."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.config import settings

logger = logging.getLogger(__name__)


_EMAIL_TEMPLATE = """\
<html><body style="font-family:Arial,sans-serif;margin:20px;">
<h2 style="color:#c0392b;">⚠️ FDA Recall Alert</h2>
<table style="border-collapse:collapse;width:100%;">
  <tr><td style="padding:6px;font-weight:bold;width:180px;">Recall Number</td>
      <td style="padding:6px;">{recall_number}</td></tr>
  <tr style="background:#f8f8f8;">
      <td style="padding:6px;font-weight:bold;">Classification</td>
      <td style="padding:6px;">{classification}</td></tr>
  <tr><td style="padding:6px;font-weight:bold;">Product Type</td>
      <td style="padding:6px;">{product_type}</td></tr>
  <tr style="background:#f8f8f8;">
      <td style="padding:6px;font-weight:bold;">Recalling Firm</td>
      <td style="padding:6px;">{firm}</td></tr>
  <tr><td style="padding:6px;font-weight:bold;">Reason</td>
      <td style="padding:6px;">{reason}</td></tr>
  <tr style="background:#f8f8f8;">
      <td style="padding:6px;font-weight:bold;">Product</td>
      <td style="padding:6px;">{product}</td></tr>
  <tr><td style="padding:6px;font-weight:bold;">Report Date</td>
      <td style="padding:6px;">{report_date}</td></tr>
</table>
<p style="color:#666;font-size:12px;margin-top:20px;">
  This alert was triggered by your FDA Recall Surveillance subscription "{sub_name}".
</p>
</body></html>
"""


def _build_message(
    to_email: str,
    sub_name: str,
    recall: dict,
) -> MIMEMultipart:
    classification = recall.get("classification") or "Unknown"
    firm = recall.get("recalling_firm") or "Unknown"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"[FDA Alert] {classification} Recall — {firm[:50]}"
        f" ({recall.get('recall_number', '')})"
    )
    msg["From"] = settings.ALERT_EMAIL_FROM
    msg["To"] = to_email

    plain = (
        f"FDA Recall Alert\n\n"
        f"Recall Number : {recall.get('recall_number')}\n"
        f"Classification: {classification}\n"
        f"Product Type  : {recall.get('product_type')}\n"
        f"Firm          : {firm}\n"
        f"Reason        : {recall.get('reason_for_recall', '')[:300]}\n"
        f"Product       : {recall.get('product_description', '')[:200]}\n"
        f"Report Date   : {recall.get('report_date')}\n\n"
        f"Subscription  : {sub_name}\n"
    )
    html = _EMAIL_TEMPLATE.format(
        recall_number=recall.get("recall_number", ""),
        classification=classification,
        product_type=recall.get("product_type", ""),
        firm=firm,
        reason=(recall.get("reason_for_recall") or "")[:500],
        product=(recall.get("product_description") or "")[:300],
        report_date=str(recall.get("report_date") or ""),
        sub_name=sub_name,
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))
    return msg


def send_email(to_email: str, sub_name: str, recall: dict) -> None:
    """Send a recall alert email. Raises on failure."""
    if not settings.SMTP_HOST:
        raise RuntimeError("SMTP_HOST not configured — email channel is disabled")

    msg = _build_message(to_email, sub_name, recall)
    port = settings.SMTP_PORT
    host = settings.SMTP_HOST

    if settings.SMTP_USE_TLS:
        server = smtplib.SMTP(host, port, timeout=10)
        server.ehlo()
        server.starttls()
    else:
        server = smtplib.SMTP_SSL(host, port, timeout=10)  # type: ignore[assignment]

    try:
        if settings.SMTP_USERNAME and settings.SMTP_PASSWORD:
            server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        server.sendmail(settings.ALERT_EMAIL_FROM, [to_email], msg.as_string())
        logger.info("Email alert sent to %s for recall %s", to_email, recall.get("recall_number"))
    finally:
        server.quit()
