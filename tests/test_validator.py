"""Tests for the recall validator."""

from datetime import date

import pytest
from pydantic import ValidationError

from src.ingestion.validator import RecallRecord, validate_batch, validate_record


class TestRecallRecord:
    def test_valid_record(self):
        record = RecallRecord(
            recall_number="D-0100-2026",
            product_type="Drugs",
            classification="Class I",
            recalling_firm="Test Corp",
            reason_for_recall="Product contamination",
        )
        assert record.recall_number == "D-0100-2026"
        assert record.product_type == "Drugs"

    def test_missing_recall_number(self):
        with pytest.raises(ValidationError):
            RecallRecord(
                recall_number="",
                product_type="Drugs",
                recalling_firm="Test Corp",
                reason_for_recall="Something",
            )

    def test_invalid_product_type(self):
        with pytest.raises(ValidationError):
            RecallRecord(
                recall_number="D-001",
                product_type="Invalid",
                recalling_firm="Test Corp",
                reason_for_recall="Something",
            )

    def test_status_normalization(self):
        record = RecallRecord(
            recall_number="D-001",
            product_type="Drugs",
            recalling_firm="Test Corp",
            reason_for_recall="Something",
            status="On-Going",
        )
        assert record.status == "Ongoing"

    def test_none_classification(self):
        record = RecallRecord(
            recall_number="D-001",
            product_type="Drugs",
            recalling_firm="Test Corp",
            reason_for_recall="Something",
            classification=None,
        )
        assert record.classification is None


class TestValidateRecord:
    def test_valid_record_with_warnings(self):
        data = {
            "recall_number": "D-001",
            "product_type": "Drugs",
            "recalling_firm": "Test Corp",
            "reason_for_recall": "Test reason",
            "raw_json": {},
        }
        validated, warnings = validate_record(data)
        assert validated["recall_number"] == "D-001"
        assert "Missing product_description" in warnings
        assert "Missing distribution_pattern" in warnings
        assert "Missing classification" in warnings

    def test_invalid_record_raises(self):
        data = {
            "recall_number": "",
            "product_type": "Drugs",
            "recalling_firm": "Test Corp",
            "reason_for_recall": "Test reason",
            "raw_json": {},
        }
        with pytest.raises(Exception):
            validate_record(data)


class TestValidateBatch:
    def test_mixed_batch(self):
        records = [
            {
                "recall_number": "D-001",
                "product_type": "Drugs",
                "recalling_firm": "Corp A",
                "reason_for_recall": "Reason A",
                "raw_json": {},
            },
            {
                "recall_number": "",  # Invalid — empty
                "product_type": "Drugs",
                "recalling_firm": "Corp B",
                "reason_for_recall": "Reason B",
                "raw_json": {},
            },
            {
                "recall_number": "F-002",
                "product_type": "Food",
                "recalling_firm": "Corp C",
                "reason_for_recall": "Reason C",
                "classification": "Class I",
                "product_description": "Some product",
                "distribution_pattern": "Nationwide",
                "report_date": date(2026, 1, 1),
                "raw_json": {},
            },
        ]
        valid, failed, warning_count = validate_batch(records)
        assert len(valid) == 2
        assert len(failed) == 1
        assert failed[0]["recall_number"] == ""
        # First record should have warnings (missing fields), third should have fewer
        assert warning_count >= 3
