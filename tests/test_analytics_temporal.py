"""Tests for temporal pattern analysis."""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.analytics.temporal import TemporalAnalyzer


def _make_df(dates: list[str], product_types: list[str] | None = None) -> pd.DataFrame:
    n = len(dates)
    product_types = product_types or (["Drugs"] * n)
    return pd.DataFrame({
        "recall_number": [f"R-{i}" for i in range(n)],
        "report_date": pd.to_datetime(dates),
        "product_type": product_types,
    })


class TestTemporalAnalyzer:
    def test_monthly_counts_groups_correctly(self):
        df = _make_df([
            "2024-01-05", "2024-01-20", "2024-02-10", "2024-02-15", "2024-03-01",
        ])
        result = TemporalAnalyzer().monthly_counts(df, by=None)
        assert len(result) == 3
        assert result["count"].tolist() == [2, 2, 1]

    def test_monthly_counts_by_product_type(self):
        df = _make_df(
            ["2024-01-05", "2024-01-10", "2024-02-01"],
            ["Drugs", "Devices", "Drugs"],
        )
        result = TemporalAnalyzer().monthly_counts(df, by="product_type")
        assert len(result) == 3

    def test_quarterly_counts(self):
        df = _make_df(["2024-01-15", "2024-02-15", "2024-04-15", "2024-07-15"])
        result = TemporalAnalyzer().quarterly_counts(df, by=None)
        assert len(result) == 3  # Q1, Q2, Q3

    def test_yearly_counts(self):
        df = _make_df(["2023-06-01", "2024-01-01", "2024-12-31"])
        result = TemporalAnalyzer().yearly_counts(df, by=None)
        assert result["count"].tolist() == [1, 2]

    def test_seasonal_counts_ordering(self):
        df = _make_df([
            "2024-01-15", "2024-04-15", "2024-07-15", "2024-10-15", "2024-12-15",
        ])
        result = TemporalAnalyzer().seasonal_counts(df, by=None)
        seasons = [str(s) for s in result["season"]]
        assert seasons == ["Winter", "Spring", "Summer", "Fall"]
        winter_count = int(result[result["season"] == "Winter"]["count"].iloc[0])
        assert winter_count == 2  # January + December

    def test_month_of_year_profile(self):
        df = _make_df([
            "2023-01-15", "2024-01-10", "2023-07-15",
        ])
        profile = TemporalAnalyzer().month_of_year_profile(df)
        assert "month_name" in profile.columns
        assert set(profile["month_name"]) >= {"Jan", "Jul"}

    def test_trend_slope_positive(self):
        # Ramp-up: 1 recall in month 1, 3 in month 2, 5 in month 3.
        df = _make_df(
            ["2024-01-15"]
            + ["2024-02-10", "2024-02-20", "2024-02-25"]
            + ["2024-03-01", "2024-03-05", "2024-03-10", "2024-03-15", "2024-03-20"]
        )
        trend = TemporalAnalyzer().trend(df)
        assert trend["slope"] > 0
        assert trend["n_months"] == 3

    def test_trend_insufficient_data(self):
        df = _make_df(["2024-01-15", "2024-02-15"])
        trend = TemporalAnalyzer().trend(df)
        assert trend["slope"] == 0.0

    def test_spike_detection(self):
        # 11 normal months (1 recall each) + 1 month with 20 recalls = spike.
        dates = [f"2024-{m:02d}-15" for m in range(1, 12)]
        dates += ["2024-12-01"] * 20
        df = _make_df(dates)
        spikes = TemporalAnalyzer().spike_detection(df)
        assert spikes["is_spike"].any()

    def test_empty_df(self):
        empty = pd.DataFrame({"recall_number": [], "report_date": pd.to_datetime([]),
                              "product_type": []})
        assert TemporalAnalyzer().monthly_counts(empty).empty
        assert TemporalAnalyzer().seasonal_counts(empty).empty

    def test_handles_null_dates(self):
        df = pd.DataFrame({
            "recall_number": ["R-1", "R-2", "R-3"],
            "report_date": [pd.NaT, pd.Timestamp("2024-01-15"), pd.Timestamp("2024-01-20")],
            "product_type": ["Drugs"] * 3,
        })
        result = TemporalAnalyzer().monthly_counts(df, by=None)
        assert len(result) == 1
        assert result["count"].iloc[0] == 2
