"""Tests for the executive report generator."""

from datetime import date

import pandas as pd

from src.analytics.reports import ExecutiveReport


def _sample_df() -> pd.DataFrame:
    rows = []
    for i in range(5):
        rows.append({
            "recall_number": f"D-I-{i}",
            "product_type": "Drugs",
            "classification": "Class I",
            "status": "Ongoing",
            "recalling_firm": "BigPharma",
            "manufacturer_name": "BigPharma",
            "reason_for_recall": "Salmonella contamination found",
            "product_description": "Metformin HCl tablets",
            "product_quantity": "100000 bottles",
            "distribution_pattern": "Nationwide",
            "generic_name": "METFORMIN",
            "brand_name": "GLUCOPHAGE",
            "substance_name": "METFORMIN HYDROCHLORIDE",
            "route": "ORAL",
            "state": "IL",
            "report_date": pd.Timestamp(f"2024-{(i % 12) + 1:02d}-15"),
        })
    for i in range(3):
        rows.append({
            "recall_number": f"F-III-{i}",
            "product_type": "Food",
            "classification": "Class III",
            "status": "Terminated",
            "recalling_firm": "Small Farm",
            "manufacturer_name": None,
            "reason_for_recall": "Undeclared allergen",
            "product_description": "Frozen meal",
            "product_quantity": "500 units",
            "distribution_pattern": "Only CA",
            "generic_name": None, "brand_name": None, "substance_name": None,
            "route": None, "state": "CA",
            "report_date": pd.Timestamp(f"2024-{i + 1:02d}-10"),
        })
    return pd.DataFrame(rows)


class TestExecutiveReport:
    def test_build_non_empty(self):
        report = ExecutiveReport().build(_sample_df())
        assert report["summary"]["total_recalls"] == 8
        assert report["summary"]["class_i"] == 5
        assert "by_classification" in report["sections"]
        assert len(report["insights"]) >= 2

    def test_build_empty(self):
        report = ExecutiveReport().build(pd.DataFrame())
        assert report["summary"]["total_recalls"] == 0
        assert report["insights"] == ["No recall data available."]

    def test_render_markdown_contains_headings(self):
        md = ExecutiveReport().render_markdown(_sample_df())
        assert "# FDA Recall Surveillance — Executive Summary" in md
        assert "## Summary Metrics" in md
        assert "## Key Insights" in md
        assert "Class I" in md

    def test_render_markdown_empty(self):
        md = ExecutiveReport().render_markdown(pd.DataFrame())
        assert "No recall data available" in md

    def test_insights_include_top_manufacturer(self):
        md = ExecutiveReport().render_markdown(_sample_df())
        assert "BigPharma" in md

    def test_sections_have_expected_structure(self):
        report = ExecutiveReport().build(_sample_df())
        sections = report["sections"]
        assert isinstance(sections["by_classification"], pd.DataFrame)
        assert isinstance(sections["trend_stats"], dict)
        assert "slope" in sections["trend_stats"]
