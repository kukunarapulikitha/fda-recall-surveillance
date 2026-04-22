"""Tests for the high-risk manufacturer / product identifier."""

import pandas as pd

from src.analytics.high_risk import HighRiskIdentifier
from src.analytics.risk_scoring import RiskScorer


def _make_df() -> pd.DataFrame:
    # Firm A: 4 Class I + 1 Class II recalls (high-volume, high-severity)
    # Firm B: 3 Class III recalls (high-volume, low-severity)
    # Firm C: 1 Class I recall (low-volume — should be filtered out with min_recalls=3)
    rows = []
    for i in range(4):
        rows.append({
            "recall_number": f"A-I-{i}", "classification": "Class I",
            "recalling_firm": "Firm A", "manufacturer_name": "Firm A",
            "distribution_pattern": "Nationwide", "product_quantity": "10000 units",
            "status": "Ongoing", "product_type": "Drugs", "product_category": "Oral",
        })
    rows.append({
        "recall_number": "A-II-0", "classification": "Class II",
        "recalling_firm": "Firm A", "manufacturer_name": "Firm A",
        "distribution_pattern": "Nationwide", "product_quantity": "5000 units",
        "status": "Ongoing", "product_type": "Drugs", "product_category": "Oral",
    })
    for i in range(3):
        rows.append({
            "recall_number": f"B-III-{i}", "classification": "Class III",
            "recalling_firm": "Firm B", "manufacturer_name": "Firm B",
            "distribution_pattern": "Only CA", "product_quantity": "100 units",
            "status": "Terminated", "product_type": "Drugs", "product_category": "Oral",
        })
    rows.append({
        "recall_number": "C-I-0", "classification": "Class I",
        "recalling_firm": "Firm C", "manufacturer_name": "Firm C",
        "distribution_pattern": "Nationwide", "product_quantity": "1000 units",
        "status": "Ongoing", "product_type": "Drugs", "product_category": "Oral",
    })
    return pd.DataFrame(rows)


class TestHighRiskIdentifier:
    def test_top_manufacturers_ordering(self):
        df = RiskScorer().score(_make_df())
        ranked = HighRiskIdentifier(min_recalls=3).top_manufacturers(df)
        assert not ranked.empty
        # Firm A (severe + volume) should beat Firm B (low severity)
        assert ranked.iloc[0]["manufacturer"] == "Firm A"

    def test_min_recalls_filter(self):
        df = RiskScorer().score(_make_df())
        ranked = HighRiskIdentifier(min_recalls=3).top_manufacturers(df)
        # Firm C has only 1 recall, should be excluded.
        assert "Firm C" not in ranked["manufacturer"].tolist()

    def test_min_recalls_allows_firm_c_at_threshold_1(self):
        df = RiskScorer().score(_make_df())
        ranked = HighRiskIdentifier(min_recalls=1).top_manufacturers(df)
        assert "Firm C" in ranked["manufacturer"].tolist()

    def test_repeat_offenders(self):
        df = _make_df()
        offenders = HighRiskIdentifier().repeat_offenders(df, min_class_i=2)
        # Firm A has 4 Class I -> flagged; Firm C has 1 -> not flagged.
        assert list(offenders["manufacturer"]) == ["Firm A"]
        assert offenders.iloc[0]["class_i_recalls"] == 4

    def test_top_product_categories(self):
        df = RiskScorer().score(_make_df())
        cats = HighRiskIdentifier().top_product_categories(df)
        assert not cats.empty
        assert "Oral" in cats["product_category"].tolist()

    def test_empty_df(self):
        empty = pd.DataFrame(columns=[
            "recall_number", "classification", "recalling_firm", "manufacturer_name",
            "distribution_pattern", "product_quantity", "status", "product_type",
            "product_category",
        ])
        ranked = HighRiskIdentifier().top_manufacturers(empty)
        assert ranked.empty

    def test_works_without_risk_score_column(self):
        """Fallback path — rank by severity-weighted counts."""
        df = _make_df()  # no risk_score column
        ranked = HighRiskIdentifier(min_recalls=3).top_manufacturers(df)
        assert not ranked.empty
        assert ranked.iloc[0]["manufacturer"] == "Firm A"
