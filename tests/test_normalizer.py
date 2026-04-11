"""Tests for the recall normalizer."""

from datetime import date

from src.ingestion.normalizer import RecallNormalizer


class TestRecallNormalizer:
    def setup_method(self):
        self.normalizer = RecallNormalizer()

    def test_normalize_drug_enforcement(self, drug_record):
        result = self.normalizer.normalize_drug_enforcement(drug_record)

        assert result["recall_number"] == "D-0100-2026"
        assert result["product_type"] == "Drugs"
        assert result["classification"] == "Class II"
        assert result["status"] == "Ongoing"
        assert result["recalling_firm"] == "Test Pharmaceutical Inc"
        assert "dissolution" in result["reason_for_recall"].lower()
        assert result["report_date"] == date(2026, 1, 10)
        assert result["recall_initiation_date"] == date(2026, 1, 5)
        assert result["brand_name"] == "METFORMIN HCL ER"
        assert result["generic_name"] == "METFORMIN HYDROCHLORIDE"
        assert result["source"] == "openfda_api"
        assert result["raw_json"] is not None

    def test_normalize_device_recall(self, device_record):
        result = self.normalizer.normalize_device_recall(device_record)

        assert result["recall_number"] == "Z-0200-2026"
        assert result["product_type"] == "Devices"
        assert result["classification"] == "Class II"
        assert result["status"] == "Ongoing"
        assert result["recalling_firm"] == "MedDevice Corp"
        assert "glucose" in result["reason_for_recall"].lower()
        assert result["recall_initiation_date"] == date(2026, 2, 1)
        assert result["source"] == "openfda_api"

    def test_normalize_food_enforcement(self, food_record):
        result = self.normalizer.normalize_food_enforcement(food_record)

        assert result["recall_number"] == "F-0300-2026"
        assert result["product_type"] == "Food"
        assert result["classification"] == "Class I"
        assert result["status"] == "Ongoing"
        assert result["recalling_firm"] == "Healthy Snacks LLC"
        assert "tree nut" in result["reason_for_recall"].lower()

    def test_normalize_dispatch(self, drug_record):
        result = self.normalizer.normalize(drug_record, "Drugs", "openfda_api")
        assert result["product_type"] == "Drugs"

    def test_parse_fda_date_valid(self):
        assert RecallNormalizer._parse_fda_date("20260115") == date(2026, 1, 15)

    def test_parse_fda_date_none(self):
        assert RecallNormalizer._parse_fda_date(None) is None
        assert RecallNormalizer._parse_fda_date("") is None

    def test_parse_fda_date_iso(self):
        assert RecallNormalizer._parse_fda_date("2026-01-15") == date(2026, 1, 15)

    def test_flatten_openfda_empty(self):
        result = RecallNormalizer._flatten_openfda({})
        assert result["brand_name"] is None
        assert result["generic_name"] is None

    def test_flatten_openfda_with_lists(self):
        openfda = {
            "brand_name": ["Brand A", "Brand B"],
            "generic_name": ["Generic X"],
        }
        result = RecallNormalizer._flatten_openfda(openfda)
        assert result["brand_name"] == "Brand A, Brand B"
        assert result["generic_name"] == "Generic X"

    def test_clean_text(self):
        assert RecallNormalizer._clean_text("  hello   world  ") == "hello world"
        assert RecallNormalizer._clean_text(None) == ""
        assert RecallNormalizer._clean_text("") == ""
