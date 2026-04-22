"""Tests for the risk scoring engine."""

import pandas as pd

from src.analytics.risk_scoring import (
    RiskScorer,
    compute_risk_score,
    estimate_state_reach,
    risk_tier,
)


class TestEstimateStateReach:
    def test_nationwide(self):
        assert estimate_state_reach("Nationwide") == 50

    def test_worldwide(self):
        assert estimate_state_reach("Worldwide distribution") == 50

    def test_single_state_abbr(self):
        assert estimate_state_reach("Distributed to CA only") == 1

    def test_multiple_states(self):
        reach = estimate_state_reach("Distributed to CA, NY, TX, and FL")
        assert reach == 4

    def test_empty(self):
        assert estimate_state_reach(None) == 0
        assert estimate_state_reach("") == 0

    def test_state_name(self):
        assert estimate_state_reach("Distributed in california and oregon") == 2


class TestComputeRiskScore:
    def test_class_i_nationwide_high_quantity(self):
        # Maximum realistic score
        score = compute_risk_score(
            classification="Class I",
            distribution_pattern="Nationwide",
            product_quantity="1,000,000 bottles",
            status="Ongoing",
        )
        assert score >= 80

    def test_class_iii_single_state_terminated_is_low(self):
        score = compute_risk_score(
            classification="Class III",
            distribution_pattern="Distributed in CA only",
            product_quantity="100 units",
            status="Terminated",
        )
        assert score < 20

    def test_terminated_lower_than_ongoing(self):
        kwargs = dict(classification="Class I", distribution_pattern="Nationwide",
                      product_quantity="10000 units")
        ongoing = compute_risk_score(status="Ongoing", **kwargs)
        terminated = compute_risk_score(status="Terminated", **kwargs)
        assert ongoing > terminated

    def test_class_i_higher_than_class_iii(self):
        kwargs = dict(distribution_pattern="Nationwide",
                      product_quantity="10000 units", status="Ongoing")
        assert compute_risk_score(classification="Class I", **kwargs) > compute_risk_score(
            classification="Class III", **kwargs
        )

    def test_nationwide_higher_than_single_state(self):
        kwargs = dict(classification="Class II", product_quantity="1000 units", status="Ongoing")
        nationwide = compute_risk_score(distribution_pattern="Nationwide", **kwargs)
        single = compute_risk_score(distribution_pattern="Only CA", **kwargs)
        assert nationwide > single

    def test_unknown_classification_uses_default(self):
        score = compute_risk_score(
            classification=None,
            distribution_pattern="Nationwide",
            product_quantity="1000 units",
            status="Ongoing",
        )
        assert score > 0
        assert score <= 100

    def test_score_never_exceeds_100(self):
        score = compute_risk_score(
            classification="Class I",
            distribution_pattern="Nationwide",
            product_quantity="9999999999999 units",
            status="Ongoing",
        )
        assert score <= 100


class TestRiskTier:
    def test_tiers(self):
        assert risk_tier(80) == "Critical"
        assert risk_tier(70) == "Critical"
        assert risk_tier(60) == "High"
        assert risk_tier(50) == "High"
        assert risk_tier(40) == "Medium"
        assert risk_tier(29) == "Low"
        assert risk_tier(0) == "Low"


class TestRiskScorer:
    def test_adds_score_and_tier(self):
        df = pd.DataFrame([
            {
                "recall_number": "D-001", "classification": "Class I",
                "distribution_pattern": "Nationwide", "product_quantity": "1,000,000 bottles",
                "status": "Ongoing",
            },
            {
                "recall_number": "D-002", "classification": "Class III",
                "distribution_pattern": "Only CA", "product_quantity": "100 units",
                "status": "Terminated",
            },
        ])
        scored = RiskScorer().score(df)
        assert "risk_score" in scored.columns
        assert "risk_tier" in scored.columns
        assert scored.loc[0, "risk_score"] > scored.loc[1, "risk_score"]
        assert scored.loc[0, "risk_tier"] in ("Critical", "High")
        assert scored.loc[1, "risk_tier"] == "Low"

    def test_empty_df(self):
        scored = RiskScorer().score(pd.DataFrame(columns=[
            "classification", "distribution_pattern", "product_quantity", "status"
        ]))
        assert "risk_score" in scored.columns
        assert scored.empty

    def test_tier_distribution(self):
        df = pd.DataFrame([
            {"classification": "Class I", "distribution_pattern": "Nationwide",
             "product_quantity": "1000000", "status": "Ongoing"},
            {"classification": "Class III", "distribution_pattern": "CA",
             "product_quantity": "100", "status": "Terminated"},
        ])
        scored = RiskScorer().score(df)
        dist = RiskScorer.tier_distribution(scored)
        assert list(dist["risk_tier"]) == ["Critical", "High", "Medium", "Low"]
        assert dist["count"].sum() == 2
