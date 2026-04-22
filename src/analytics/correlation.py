"""Correlation analysis between recall types and geographic/demographic dimensions.

The FDA recall corpus does not ship with patient-demographic fields; what we do
have is firm geography (state/country) and distribution pattern. These act as a
proxy for the population exposed to each recall, so this module focuses on:

  * Recall type × geography (state, country)
  * Recall type × recall reason
  * Severity × distribution reach
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

from src.analytics.risk_scoring import estimate_state_reach


@dataclass
class CorrelationAnalyzer:
    """Cross-tabulations and statistical tests between recall dimensions."""

    def type_by_state(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recall counts by state × product_type (firm state = demographic proxy)."""
        if df.empty or "state" not in df.columns:
            return pd.DataFrame()
        filtered = df.dropna(subset=["state"])
        filtered = filtered[filtered["state"].astype(str).str.len() == 2]
        if filtered.empty:
            return pd.DataFrame()
        pivot = pd.crosstab(filtered["state"], filtered["product_type"])
        pivot["total"] = pivot.sum(axis=1)
        return pivot.sort_values("total", ascending=False)

    def severity_by_reach(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cross-tab of classification × distribution reach bucket."""
        if df.empty:
            return pd.DataFrame()
        reach = df["distribution_pattern"].apply(estimate_state_reach)
        bucket = pd.cut(
            reach,
            bins=[-1, 0, 1, 5, 15, 60],
            labels=["Unknown", "1 state", "2-5 states", "6-15 states", "Nationwide"],
        )
        return pd.crosstab(df["classification"].fillna("Unclassified"), bucket)

    def reason_by_product_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cross-tab of reason_category × product_type (normalized to %)."""
        if df.empty or "reason_category" not in df.columns:
            return pd.DataFrame()
        ct = pd.crosstab(df["reason_category"], df["product_type"], normalize="columns") * 100
        return ct.round(1)

    def chi_square_reason_vs_severity(self, df: pd.DataFrame) -> dict:
        """Chi-square independence test between recall reason and severity class."""
        if df.empty or "reason_category" not in df.columns:
            return {"statistic": None, "p_value": None, "interpretation": "insufficient data"}
        subset = df.dropna(subset=["classification", "reason_category"])
        if len(subset) < 10:
            return {"statistic": None, "p_value": None, "interpretation": "insufficient data"}
        ct = pd.crosstab(subset["reason_category"], subset["classification"])
        if ct.shape[0] < 2 or ct.shape[1] < 2:
            return {"statistic": None, "p_value": None, "interpretation": "insufficient variation"}
        chi2, p, dof, _ = stats.chi2_contingency(ct)
        return {
            "statistic": round(float(chi2), 3),
            "p_value": round(float(p), 4),
            "dof": int(dof),
            "n": int(ct.values.sum()),
            "interpretation": (
                "statistically significant association (p<0.05)"
                if p < 0.05 else "no significant association (p≥0.05)"
            ),
        }

    def pearson_recalls_vs_reach(self, df: pd.DataFrame) -> dict:
        """Correlation between estimated state reach and risk score."""
        if df.empty or "risk_score" not in df.columns:
            return {"r": None, "p_value": None}
        reach = df["distribution_pattern"].apply(estimate_state_reach).to_numpy(dtype=float)
        score = df["risk_score"].to_numpy(dtype=float)
        mask = ~np.isnan(reach) & ~np.isnan(score)
        if mask.sum() < 3:
            return {"r": None, "p_value": None}
        r, p = stats.pearsonr(reach[mask], score[mask])
        return {"r": round(float(r), 3), "p_value": round(float(p), 4), "n": int(mask.sum())}
