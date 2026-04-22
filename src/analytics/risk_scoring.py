"""Risk scoring engine: assigns a 0–100 risk score to every recall.

The score combines:
  * Classification severity (Class I / II / III)
  * Reach of distribution (nationwide vs single-state vs limited)
  * Affected quantity (log-scaled)
  * Current status (Ongoing recalls are higher risk than Terminated ones)

The formula is deliberately simple and transparent so downstream users can
inspect and adjust the weights.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

import pandas as pd

from src.analytics.categorize import parse_quantity

# Weights — sum should produce scores roughly in the 0-100 range.
SEVERITY_WEIGHT = {"Class I": 40, "Class II": 20, "Class III": 8}
DEFAULT_SEVERITY_WEIGHT = 15  # unknown / not-yet-classified

DISTRIBUTION_WEIGHT_NATIONWIDE = 25
DISTRIBUTION_WEIGHT_MULTI_STATE = 15
DISTRIBUTION_WEIGHT_SINGLE_STATE = 7
DISTRIBUTION_WEIGHT_UNKNOWN = 10

QUANTITY_MAX_POINTS = 20  # log-scaled

STATUS_MULTIPLIER = {
    "Ongoing": 1.0,
    "On-Going": 1.0,
    "Pending": 0.95,
    "Completed": 0.7,
    "Terminated": 0.4,
}
DEFAULT_STATUS_MULTIPLIER = 0.9

# Matches typical US state abbreviation tokens in distribution_pattern strings.
_STATE_TOKEN_RE = re.compile(r"\b[A-Z]{2}\b")
_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
}


def estimate_state_reach(distribution_pattern: str | None) -> int:
    """Best-effort count of U.S. states mentioned in a distribution pattern string.

    Returns 50 for 'nationwide', otherwise counts distinct state tokens and
    state names. Returns 0 if nothing identifiable is found.
    """
    if not distribution_pattern:
        return 0
    text = distribution_pattern.lower()
    if "nationwide" in text or "nation wide" in text or "nation-wide" in text:
        return 50
    if "international" in text or "worldwide" in text:
        return 50

    found: set[str] = set()
    for match in _STATE_TOKEN_RE.findall(distribution_pattern):
        # Filter out common false positives like "OK" (state) vs "OK" (unit).
        found.add(match.upper())
    for name in _STATE_NAMES:
        if name in text:
            found.add(name)
    return len(found)


def _distribution_points(distribution_pattern: str | None) -> int:
    reach = estimate_state_reach(distribution_pattern)
    if reach >= 10:
        return DISTRIBUTION_WEIGHT_NATIONWIDE
    if reach >= 2:
        return DISTRIBUTION_WEIGHT_MULTI_STATE
    if reach == 1:
        return DISTRIBUTION_WEIGHT_SINGLE_STATE
    return DISTRIBUTION_WEIGHT_UNKNOWN


def _quantity_points(product_quantity: str | None) -> float:
    qty = parse_quantity(product_quantity)
    if qty is None or qty <= 0:
        return 0.0
    # log10(1) = 0 -> 0 pts, log10(1_000_000) = 6 -> QUANTITY_MAX_POINTS (capped)
    score = math.log10(qty) / 6.0 * QUANTITY_MAX_POINTS
    return float(min(QUANTITY_MAX_POINTS, max(0.0, score)))


def compute_risk_score(
    classification: str | None,
    distribution_pattern: str | None,
    product_quantity: str | None,
    status: str | None,
) -> float:
    """Compute a 0–100 risk score from recall attributes."""
    severity_pts = SEVERITY_WEIGHT.get(classification or "", DEFAULT_SEVERITY_WEIGHT)
    distribution_pts = _distribution_points(distribution_pattern)
    quantity_pts = _quantity_points(product_quantity)

    raw_score = severity_pts + distribution_pts + quantity_pts
    multiplier = STATUS_MULTIPLIER.get(status or "", DEFAULT_STATUS_MULTIPLIER)
    final = raw_score * multiplier
    return round(min(100.0, max(0.0, final)), 2)


def risk_tier(score: float) -> str:
    """Bucket a numeric score into a qualitative tier."""
    if score >= 70:
        return "Critical"
    if score >= 50:
        return "High"
    if score >= 30:
        return "Medium"
    return "Low"


@dataclass
class RiskScorer:
    """Applies risk scoring to a recall DataFrame."""

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a copy of df with risk_score and risk_tier columns added."""
        if df.empty:
            return df.assign(risk_score=pd.Series(dtype="float"),
                             risk_tier=pd.Series(dtype="object"))
        out = df.copy()
        out["risk_score"] = out.apply(
            lambda row: compute_risk_score(
                classification=row.get("classification"),
                distribution_pattern=row.get("distribution_pattern"),
                product_quantity=row.get("product_quantity"),
                status=row.get("status"),
            ),
            axis=1,
        )
        out["risk_tier"] = out["risk_score"].apply(risk_tier)
        return out

    @staticmethod
    def tier_distribution(scored_df: pd.DataFrame) -> pd.DataFrame:
        """Count recalls in each risk tier."""
        if scored_df.empty or "risk_tier" not in scored_df.columns:
            return pd.DataFrame(columns=["risk_tier", "count"])
        tiers = ["Critical", "High", "Medium", "Low"]
        counts = scored_df["risk_tier"].value_counts().reindex(tiers, fill_value=0)
        return counts.rename_axis("risk_tier").reset_index(name="count")
