"""Tests for correlation analysis."""

import pandas as pd

from src.analytics.categorize import RecallCategorizer
from src.analytics.correlation import CorrelationAnalyzer
from src.analytics.risk_scoring import RiskScorer


def _sample_df(n_per_group: int = 5) -> pd.DataFrame:
    rows = []
    for state in ("CA", "NY", "TX"):
        for ptype in ("Drugs", "Devices", "Food"):
            for i in range(n_per_group):
                rows.append({
                    "recall_number": f"{state}-{ptype}-{i}",
                    "product_type": ptype,
                    "classification": "Class I" if i % 3 == 0 else "Class II",
                    "state": state,
                    "distribution_pattern": "Nationwide" if i % 2 == 0 else f"Only {state}",
                    "product_quantity": "1000 units",
                    "status": "Ongoing",
                    "reason_for_recall": "Contamination" if i % 2 == 0 else "Mislabeling",
                    "generic_name": None, "brand_name": None, "substance_name": None,
                    "product_description": None, "route": None,
                    "recalling_firm": f"Firm-{state}",
                })
    return pd.DataFrame(rows)


class TestCorrelationAnalyzer:
    def test_type_by_state(self):
        df = _sample_df()
        result = CorrelationAnalyzer().type_by_state(df)
        assert not result.empty
        assert set(result.index) == {"CA", "NY", "TX"}

    def test_type_by_state_empty(self):
        assert CorrelationAnalyzer().type_by_state(pd.DataFrame()).empty

    def test_severity_by_reach(self):
        df = _sample_df()
        result = CorrelationAnalyzer().severity_by_reach(df)
        assert not result.empty
        assert "Nationwide" in result.columns.tolist()

    def test_reason_by_product_type(self):
        df = RecallCategorizer().categorize(_sample_df())
        result = CorrelationAnalyzer().reason_by_product_type(df)
        assert not result.empty
        for col in result.columns:
            # normalized to percentages per column
            assert abs(result[col].sum() - 100.0) < 1.0

    def test_chi_square_reason_vs_severity(self):
        df = RecallCategorizer().categorize(_sample_df())
        result = CorrelationAnalyzer().chi_square_reason_vs_severity(df)
        assert "p_value" in result
        assert result["p_value"] is not None

    def test_chi_square_insufficient_data(self):
        tiny = _sample_df(n_per_group=0)
        tiny = RecallCategorizer().categorize(tiny)
        result = CorrelationAnalyzer().chi_square_reason_vs_severity(tiny)
        assert result["p_value"] is None

    def test_pearson_recalls_vs_reach(self):
        df = RiskScorer().score(_sample_df())
        result = CorrelationAnalyzer().pearson_recalls_vs_reach(df)
        assert result["r"] is not None
        assert -1 <= result["r"] <= 1
