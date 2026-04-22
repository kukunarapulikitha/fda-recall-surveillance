"""Temporal pattern analysis: recall frequency by month/quarter and seasonal trends."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

DATE_COLUMN = "report_date"

SEASON_BY_MONTH = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Fall", 10: "Fall", 11: "Fall",
}


@dataclass
class TemporalAnalyzer:
    """Aggregate recall counts by month, quarter, year, and season."""

    date_column: str = DATE_COLUMN

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows with no date and ensure the date column is datetime."""
        if df.empty or self.date_column not in df.columns:
            return df.iloc[0:0].copy()
        out = df.dropna(subset=[self.date_column]).copy()
        out[self.date_column] = pd.to_datetime(out[self.date_column], errors="coerce")
        return out.dropna(subset=[self.date_column])

    def monthly_counts(self, df: pd.DataFrame, by: str | None = "product_type") -> pd.DataFrame:
        """Recall counts per calendar month, optionally broken down by a column."""
        prep = self._prepare(df)
        if prep.empty:
            return pd.DataFrame(columns=["month", by, "count"] if by else ["month", "count"])
        prep["month"] = prep[self.date_column].dt.to_period("M").dt.to_timestamp()
        group_cols = ["month"] + ([by] if by else [])
        return (
            prep.groupby(group_cols)
            .size()
            .reset_index(name="count")
            .sort_values("month")
        )

    def quarterly_counts(self, df: pd.DataFrame, by: str | None = "product_type") -> pd.DataFrame:
        """Recall counts per calendar quarter."""
        prep = self._prepare(df)
        if prep.empty:
            return pd.DataFrame(columns=["quarter", by, "count"] if by else ["quarter", "count"])
        prep["quarter"] = prep[self.date_column].dt.to_period("Q").astype(str)
        group_cols = ["quarter"] + ([by] if by else [])
        return prep.groupby(group_cols).size().reset_index(name="count").sort_values("quarter")

    def yearly_counts(self, df: pd.DataFrame, by: str | None = "product_type") -> pd.DataFrame:
        """Recall counts per calendar year."""
        prep = self._prepare(df)
        if prep.empty:
            return pd.DataFrame(columns=["year", by, "count"] if by else ["year", "count"])
        prep["year"] = prep[self.date_column].dt.year
        group_cols = ["year"] + ([by] if by else [])
        return prep.groupby(group_cols).size().reset_index(name="count").sort_values("year")

    def seasonal_counts(self, df: pd.DataFrame, by: str | None = "product_type") -> pd.DataFrame:
        """Recall counts by meteorological season, aggregated across years."""
        prep = self._prepare(df)
        if prep.empty:
            return pd.DataFrame(columns=["season", by, "count"] if by else ["season", "count"])
        prep["season"] = prep[self.date_column].dt.month.map(SEASON_BY_MONTH)
        group_cols = ["season"] + ([by] if by else [])
        counts = prep.groupby(group_cols).size().reset_index(name="count")
        season_order = ["Winter", "Spring", "Summer", "Fall"]
        counts["season"] = pd.Categorical(counts["season"], categories=season_order, ordered=True)
        return counts.sort_values(["season"] + ([by] if by else []))

    def month_of_year_profile(self, df: pd.DataFrame) -> pd.DataFrame:
        """Average and std-dev of monthly recall counts across years (seasonality profile)."""
        monthly = self.monthly_counts(df, by=None)
        if monthly.empty:
            return pd.DataFrame(columns=["month_num", "month_name", "mean", "std"])
        monthly["month_num"] = monthly["month"].dt.month
        profile = monthly.groupby("month_num")["count"].agg(["mean", "std"]).reset_index()
        profile["std"] = profile["std"].fillna(0)
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        profile["month_name"] = profile["month_num"].apply(lambda m: month_names[m - 1])
        return profile.sort_values("month_num").reset_index(drop=True)

    def trend(self, df: pd.DataFrame) -> dict[str, float]:
        """Linear regression slope for monthly recall counts — positive = trending up."""
        monthly = self.monthly_counts(df, by=None)
        if len(monthly) < 3:
            return {"slope": 0.0, "mean": 0.0, "n_months": len(monthly)}
        x = np.arange(len(monthly))
        y = monthly["count"].to_numpy(dtype=float)
        slope, intercept = np.polyfit(x, y, 1)
        return {
            "slope": float(slope),
            "intercept": float(intercept),
            "mean": float(y.mean()),
            "n_months": len(monthly),
        }

    def spike_detection(self, df: pd.DataFrame, z_threshold: float = 2.0) -> pd.DataFrame:
        """Flag months whose recall count deviates more than z_threshold stdevs from the mean."""
        monthly = self.monthly_counts(df, by=None)
        if len(monthly) < 3:
            monthly["z_score"] = 0.0
            monthly["is_spike"] = False
            return monthly
        mean = monthly["count"].mean()
        std = monthly["count"].std(ddof=0)
        if std == 0 or pd.isna(std):
            monthly["z_score"] = 0.0
        else:
            monthly["z_score"] = ((monthly["count"] - mean) / std).fillna(0.0).round(2)
        monthly["is_spike"] = (monthly["z_score"].abs().fillna(0) >= z_threshold).to_numpy(dtype=bool)
        return monthly
