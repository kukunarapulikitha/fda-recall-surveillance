"""Tests for the recall categorization module."""

import pandas as pd
import pytest

from src.analytics.categorize import (
    RecallCategorizer,
    categorize_device_type,
    categorize_food_type,
    categorize_recall_reason,
    categorize_therapeutic_area,
    parse_quantity,
)


class TestTherapeuticArea:
    def test_diabetes_from_generic(self):
        assert categorize_therapeutic_area(generic_name="METFORMIN HYDROCHLORIDE") == "Diabetes & Endocrine"

    def test_cardiovascular_from_substance(self):
        assert categorize_therapeutic_area(substance_name="Atorvastatin") == "Cardiovascular"

    def test_antibiotic_from_brand(self):
        assert categorize_therapeutic_area(brand_name="Amoxicillin 500mg") == "Anti-Infective"

    def test_pain_from_description(self):
        assert categorize_therapeutic_area(product_description="Ibuprofen 200mg tablets") == "Pain & Analgesic"

    def test_oncology_takes_precedence(self):
        # Both "cancer" and "pain" keywords — oncology rule comes first.
        assert categorize_therapeutic_area(reason_for_recall="Used in cancer pain management") == "Oncology"

    def test_other_when_no_match(self):
        assert categorize_therapeutic_area(generic_name="UNKNOWN COMPOUND XYZ") == "Other"

    def test_other_when_all_none(self):
        assert categorize_therapeutic_area() == "Other"


class TestDeviceType:
    def test_cardiac(self):
        assert categorize_device_type(product_description="Cardiac pacemaker lead") == "Cardiac Devices"

    def test_monitoring(self):
        assert categorize_device_type(product_description="Pulse ox monitor") == "Monitoring Equipment"

    def test_in_vitro(self):
        assert categorize_device_type(product_description="Glucose meter test strip") == "In Vitro Diagnostic"


class TestFoodType:
    def test_dairy(self):
        assert categorize_food_type(product_description="Yogurt 32oz") == "Dairy"

    def test_produce(self):
        assert categorize_food_type(product_description="Romaine lettuce heads") == "Produce"

    def test_meat(self):
        assert categorize_food_type(product_description="Ground beef 80/20") == "Meat & Poultry"


class TestRecallReason:
    def test_contamination(self):
        assert categorize_recall_reason("Salmonella contamination detected") == "Contamination"

    def test_mislabeling(self):
        assert categorize_recall_reason("Undeclared allergen: peanut") == "Mislabeling"

    def test_dissolution(self):
        assert categorize_recall_reason("Failed dissolution specifications") == "Out of Specification"

    def test_unknown_for_none(self):
        assert categorize_recall_reason(None) == "Unknown"


class TestParseQuantity:
    def test_with_commas(self):
        assert parse_quantity("50,000 bottles") == 50_000

    def test_no_unit(self):
        assert parse_quantity("1200") == 1200

    def test_none(self):
        assert parse_quantity(None) is None

    def test_empty(self):
        assert parse_quantity("") is None

    def test_no_numbers(self):
        assert parse_quantity("unknown quantity") is None


class TestRecallCategorizer:
    def setup_method(self):
        self.df = pd.DataFrame([
            {
                "recall_number": "D-001", "product_type": "Drugs",
                "classification": "Class I", "generic_name": "INSULIN GLARGINE",
                "brand_name": "LANTUS", "substance_name": "INSULIN",
                "product_description": "Insulin Glargine 100 units/mL",
                "reason_for_recall": "Potential contamination",
                "route": "SUBCUTANEOUS", "recalling_firm": "Pharma A",
            },
            {
                "recall_number": "Z-002", "product_type": "Devices",
                "classification": "Class II", "generic_name": None,
                "brand_name": "CardioMon 3000", "substance_name": None,
                "product_description": "Cardiac pacemaker lead, single-chamber",
                "reason_for_recall": "Malfunction of pacing",
                "route": None, "recalling_firm": "MedCo",
            },
            {
                "recall_number": "F-003", "product_type": "Food",
                "classification": "Class I", "generic_name": None,
                "brand_name": None, "substance_name": None,
                "product_description": "Romaine lettuce, bagged",
                "reason_for_recall": "E. coli contamination",
                "route": None, "recalling_firm": "Farms B",
            },
        ])

    def test_adds_expected_columns(self):
        out = RecallCategorizer().categorize(self.df)
        for col in ("therapeutic_area", "product_category", "reason_category", "severity_rank"):
            assert col in out.columns

    def test_therapeutic_area_for_drug_only(self):
        out = RecallCategorizer().categorize(self.df)
        assert out.loc[0, "therapeutic_area"] == "Diabetes & Endocrine"
        assert out.loc[1, "therapeutic_area"] == "N/A"  # Devices
        assert out.loc[2, "therapeutic_area"] == "N/A"  # Food

    def test_product_category_dispatch(self):
        out = RecallCategorizer().categorize(self.df)
        assert out.loc[0, "product_category"] == "Subcutaneous"
        assert out.loc[1, "product_category"] == "Cardiac Devices"
        assert out.loc[2, "product_category"] == "Produce"

    def test_severity_rank(self):
        out = RecallCategorizer().categorize(self.df)
        assert out.loc[0, "severity_rank"] == 3  # Class I
        assert out.loc[1, "severity_rank"] == 2  # Class II

    def test_reason_categories(self):
        out = RecallCategorizer().categorize(self.df)
        assert out.loc[0, "reason_category"] == "Contamination"
        assert out.loc[1, "reason_category"] == "Malfunction"
        assert out.loc[2, "reason_category"] == "Contamination"

    def test_empty_df(self):
        empty = pd.DataFrame(columns=self.df.columns)
        out = RecallCategorizer().categorize(empty)
        assert "therapeutic_area" in out.columns

    def test_summary_by(self):
        out = RecallCategorizer().categorize(self.df)
        summary = RecallCategorizer.summary_by(out, "product_type")
        assert set(summary["product_type"]) == {"Drugs", "Devices", "Food"}
        assert (summary["total_recalls"] == 1).all()
