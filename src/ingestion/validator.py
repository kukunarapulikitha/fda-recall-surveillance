"""Pydantic validation schemas for recall records."""

import logging
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

VALID_CLASSIFICATIONS = {"Class I", "Class II", "Class III", "Not Yet Classified", ""}
VALID_STATUSES = {"Ongoing", "Terminated", "Completed", "On-Going", "Pending", ""}


class RecallRecord(BaseModel):
    """Pydantic model for validating normalized recall records."""

    recall_number: str = Field(min_length=1)
    event_id: str | None = None
    source: str = "openfda_api"
    product_type: Literal["Drugs", "Devices", "Food"]
    classification: str | None = None
    status: str | None = None
    recalling_firm: str = Field(min_length=1)
    reason_for_recall: str = Field(min_length=1)
    product_description: str | None = None
    product_quantity: str | None = None
    code_info: str | None = None
    distribution_pattern: str | None = None
    voluntary_mandated: str | None = None
    initial_firm_notification: str | None = None
    recall_initiation_date: date | None = None
    report_date: date | None = None
    center_classification_date: date | None = None
    termination_date: date | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    postal_code: str | None = None
    brand_name: str | None = None
    generic_name: str | None = None
    manufacturer_name: str | None = None
    product_ndc: str | None = None
    substance_name: str | None = None
    route: str | None = None
    application_number: str | None = None
    raw_json: dict = Field(default_factory=dict)

    model_config = {"str_strip_whitespace": True}

    @field_validator("classification", mode="before")
    @classmethod
    def validate_classification(cls, v: str | None) -> str | None:
        if v is None or v == "":
            return None
        if v not in VALID_CLASSIFICATIONS:
            logger.warning("Unexpected classification: %s", v)
        return v

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v: str | None) -> str | None:
        if v is None or v == "":
            return None
        # Normalize "On-Going" to "Ongoing"
        if v == "On-Going":
            return "Ongoing"
        return v

    @field_validator(
        "recall_initiation_date", "report_date", "center_classification_date",
        "termination_date", mode="before",
    )
    @classmethod
    def validate_date_not_future(cls, v: date | None) -> date | None:
        if v is not None and isinstance(v, date) and v > date.today():
            logger.warning("Future date detected: %s — setting to None", v)
            return None
        return v


def validate_record(normalized: dict) -> tuple[dict, list[str]]:
    """Validate a normalized recall record.

    Returns:
        Tuple of (validated_dict, list_of_warnings).
        Raises pydantic.ValidationError for critical failures.
    """
    warnings: list[str] = []

    # Collect non-critical warnings before strict validation
    if not normalized.get("product_description"):
        warnings.append("Missing product_description")
    if not normalized.get("distribution_pattern"):
        warnings.append("Missing distribution_pattern")
    if not normalized.get("classification"):
        warnings.append("Missing classification")
    if not normalized.get("report_date") and not normalized.get("recall_initiation_date"):
        warnings.append("No date fields present")

    record = RecallRecord(**normalized)
    validated = record.model_dump()

    return validated, warnings


def validate_batch(
    records: list[dict],
) -> tuple[list[dict], list[dict], int]:
    """Validate a batch of normalized records.

    Returns:
        Tuple of (valid_records, failed_records_with_errors, warning_count).
    """
    valid = []
    failed = []
    total_warnings = 0

    for rec in records:
        try:
            validated, warnings = validate_record(rec)
            validated["is_validated"] = True
            validated["validation_errors"] = warnings if warnings else None
            valid.append(validated)
            total_warnings += len(warnings)
        except Exception as e:
            logger.warning(
                "Validation failed for %s: %s",
                rec.get("recall_number", "UNKNOWN"), e,
            )
            failed.append({
                "recall_number": rec.get("recall_number", "UNKNOWN"),
                "error": str(e),
                "raw": rec,
            })

    return valid, failed, total_warnings
