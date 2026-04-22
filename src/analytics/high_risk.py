"""Identify high-risk manufacturers and product categories."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class HighRiskIdentifier:
    """Rank manufacturers / product categories by recall volume and severity."""

    # Minimum recalls required before a manufacturer can be flagged. Prevents
    # a firm with one Class I recall from appearing as "highest risk".
    min_recalls: int = 3
    top_n: int = 20

    def top_manufacturers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rank firms by aggregate risk (weighted by classification severity).

        Expects df to contain `risk_score` (from RiskScorer). Falls back to a
        severity-weighted count if that column is missing.
        """
        firm_col = self._pick_firm_column(df)
        if df.empty or firm_col is None:
            return self._empty_manufacturer_frame()

        grouped = df.groupby(firm_col, dropna=True).agg(
            total_recalls=("recall_number", "count"),
            class_i=("classification", lambda s: (s == "Class I").sum()),
            class_ii=("classification", lambda s: (s == "Class II").sum()),
            class_iii=("classification", lambda s: (s == "Class III").sum()),
        ).reset_index().rename(columns={firm_col: "manufacturer"})

        if "risk_score" in df.columns:
            risk_stats = df.groupby(firm_col)["risk_score"].agg(
                avg_risk_score="mean", max_risk_score="max"
            ).reset_index().rename(columns={firm_col: "manufacturer"})
            grouped = grouped.merge(risk_stats, on="manufacturer", how="left")
        else:
            grouped["avg_risk_score"] = (
                grouped["class_i"] * 40 + grouped["class_ii"] * 20 + grouped["class_iii"] * 8
            ) / grouped["total_recalls"].replace(0, pd.NA)
            grouped["max_risk_score"] = grouped["avg_risk_score"]

        grouped["avg_risk_score"] = grouped["avg_risk_score"].round(2)
        grouped["max_risk_score"] = grouped["max_risk_score"].round(2)

        # Composite priority: volume × avg severity (capped to prevent single-recall outliers)
        grouped["priority_score"] = (
            grouped["total_recalls"].clip(upper=100) * grouped["avg_risk_score"].fillna(0) / 10
        ).round(2)

        qualified = grouped[grouped["total_recalls"] >= self.min_recalls]
        return qualified.sort_values(
            ["priority_score", "total_recalls"], ascending=[False, False]
        ).head(self.top_n).reset_index(drop=True)

    def top_product_categories(
        self,
        df: pd.DataFrame,
        category_column: str = "product_category",
    ) -> pd.DataFrame:
        """Rank product categories by recall volume and severity."""
        cols = [category_column, "total_recalls", "class_i", "avg_risk_score", "priority_score"]
        if df.empty or category_column not in df.columns:
            return pd.DataFrame(columns=cols)

        grouped = df.groupby(category_column, dropna=False).agg(
            total_recalls=("recall_number", "count"),
            class_i=("classification", lambda s: (s == "Class I").sum()),
        ).reset_index()

        if "risk_score" in df.columns:
            risk_stats = df.groupby(category_column, dropna=False)["risk_score"].mean().reset_index()
            risk_stats = risk_stats.rename(columns={"risk_score": "avg_risk_score"})
            grouped = grouped.merge(risk_stats, on=category_column, how="left")
        else:
            grouped["avg_risk_score"] = (
                grouped["class_i"] / grouped["total_recalls"].replace(0, pd.NA) * 40
            )

        grouped["avg_risk_score"] = grouped["avg_risk_score"].round(2)
        grouped["priority_score"] = (
            grouped["total_recalls"] * grouped["avg_risk_score"].fillna(0) / 10
        ).round(2)
        return grouped.sort_values(
            ["priority_score", "total_recalls"], ascending=[False, False]
        ).head(self.top_n).reset_index(drop=True)

    def repeat_offenders(self, df: pd.DataFrame, min_class_i: int = 2) -> pd.DataFrame:
        """Firms with repeated Class I recalls — the most concerning pattern."""
        firm_col = self._pick_firm_column(df)
        if df.empty or firm_col is None:
            return pd.DataFrame(columns=["manufacturer", "class_i_recalls", "total_recalls"])
        class_i = df[df["classification"] == "Class I"]
        counts = class_i.groupby(firm_col).size().reset_index(name="class_i_recalls")
        counts = counts[counts["class_i_recalls"] >= min_class_i]
        totals = df.groupby(firm_col).size().reset_index(name="total_recalls")
        merged = counts.merge(totals, on=firm_col).rename(columns={firm_col: "manufacturer"})
        return merged.sort_values("class_i_recalls", ascending=False).reset_index(drop=True)

    @staticmethod
    def _pick_firm_column(df: pd.DataFrame) -> str | None:
        for col in ("recalling_firm", "manufacturer_name"):
            if col in df.columns and df[col].notna().any():
                return col
        return None

    @staticmethod
    def _empty_manufacturer_frame() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            "manufacturer", "total_recalls", "class_i", "class_ii", "class_iii",
            "avg_risk_score", "max_risk_score", "priority_score",
        ])
