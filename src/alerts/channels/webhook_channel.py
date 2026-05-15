"""Webhook notification channel — HTTP POST via httpx."""

from __future__ import annotations

import logging
from datetime import date

import httpx

logger = logging.getLogger(__name__)

_TIMEOUT = 15  # seconds


def _recall_payload(recall: dict, sub_name: str) -> dict:
    """Build a JSON-serialisable payload for webhook delivery."""
    def _str(v) -> str | None:
        if v is None:
            return None
        if isinstance(v, date):
            return v.isoformat()
        return str(v)

    return {
        "event": "fda_recall_alert",
        "subscription": sub_name,
        "recall": {
            "recall_number": recall.get("recall_number"),
            "product_type": recall.get("product_type"),
            "classification": recall.get("classification"),
            "recalling_firm": recall.get("recalling_firm"),
            "reason_for_recall": recall.get("reason_for_recall"),
            "product_description": recall.get("product_description"),
            "distribution_pattern": recall.get("distribution_pattern"),
            "report_date": _str(recall.get("report_date")),
            "recall_initiation_date": _str(recall.get("recall_initiation_date")),
            "state": recall.get("state"),
            "status": recall.get("status"),
        },
    }


def send_webhook(url: str, sub_name: str, recall: dict) -> None:
    """POST a recall alert to the given webhook URL. Raises on failure."""
    payload = _recall_payload(recall, sub_name)
    response = httpx.post(
        url,
        json=payload,
        timeout=_TIMEOUT,
        headers={"Content-Type": "application/json", "User-Agent": "FDA-Recall-Alert/1.0"},
    )
    response.raise_for_status()
    logger.info(
        "Webhook alert sent to %s for recall %s (HTTP %s)",
        url[:60], recall.get("recall_number"), response.status_code,
    )
