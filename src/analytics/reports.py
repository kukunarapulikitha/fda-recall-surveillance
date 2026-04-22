"""Executive summary report generator.

Produces a self-contained markdown report from a scored + categorized recall
DataFrame. Useful for CLI output (scripts/generate_report.py) and for dropping
into the Streamlit dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from src.analytics.categorize import RecallCategorizer
from src.analytics.correlation import CorrelationAnalyzer
from src.analytics.high_risk import HighRiskIdentifier
from src.analytics.risk_scoring import RiskScorer
from src.analytics.temporal import TemporalAnalyzer


@dataclass
class ExecutiveReport:
    """Assemble a single markdown executive summary from a recalls DataFrame."""

    top_n_manufacturers: int = 10
    top_n_categories: int = 10

    def build(self, df: pd.DataFrame) -> dict:
        """Return a dict of pre-computed sections for rendering or JSON export."""
        if df.empty:
            return {
                "generated_at": date.today().isoformat(),
                "summary": {"total_recalls": 0},
                "sections": {},
                "insights": ["No recall data available."],
            }

        categorized = RecallCategorizer().categorize(df)
        scored = RiskScorer().score(categorized)

        high_risk = HighRiskIdentifier(top_n=self.top_n_manufacturers)
        temporal = TemporalAnalyzer()
        corr = CorrelationAnalyzer()

        summary = self._summary_metrics(scored)
        sections = {
            "by_product_type": RecallCategorizer.summary_by(scored, "product_type"),
            "by_classification": RecallCategorizer.summary_by(scored, "classification"),
            "by_therapeutic_area": RecallCategorizer.summary_by(
                scored[scored["product_type"] == "Drugs"], "therapeutic_area"
            ),
            "by_product_category": RecallCategorizer.summary_by(scored, "product_category"),
            "by_reason_category": RecallCategorizer.summary_by(scored, "reason_category"),
            "top_manufacturers": high_risk.top_manufacturers(scored),
            "repeat_offenders": high_risk.repeat_offenders(scored),
            "top_product_categories": high_risk.top_product_categories(scored),
            "monthly_trend": temporal.monthly_counts(scored, by="product_type"),
            "seasonal_profile": temporal.seasonal_counts(scored),
            "trend_stats": temporal.trend(scored),
            "spikes": temporal.spike_detection(scored),
            "risk_tier_distribution": RiskScorer.tier_distribution(scored),
            "state_distribution": corr.type_by_state(scored),
            "severity_vs_reach": corr.severity_by_reach(scored),
            "reason_vs_severity_test": corr.chi_square_reason_vs_severity(scored),
            "reach_vs_risk_correlation": corr.pearson_recalls_vs_reach(scored),
        }

        insights = self._generate_insights(scored, sections)

        return {
            "generated_at": date.today().isoformat(),
            "summary": summary,
            "sections": sections,
            "insights": insights,
        }

    def render_markdown(self, df: pd.DataFrame) -> str:
        """Render the executive summary as a markdown string."""
        report = self.build(df)
        s = report["summary"]
        sections = report["sections"]
        lines: list[str] = []

        lines.append(f"# FDA Recall Surveillance — Executive Summary")
        lines.append(f"_Generated on {report['generated_at']}_\n")

        if s.get("total_recalls", 0) == 0:
            lines.append("No recall data available.")
            return "\n".join(lines)

        lines.append("## Summary Metrics\n")
        lines.append(f"- **Total recalls analyzed:** {s['total_recalls']:,}")
        lines.append(f"- **Date range:** {s['earliest']} to {s['latest']}")
        lines.append(f"- **Unique manufacturers:** {s['unique_firms']:,}")
        lines.append(f"- **Class I recalls:** {s['class_i']} ({s['class_i_pct']}%)")
        lines.append(f"- **Average risk score:** {s['avg_risk_score']:.1f} / 100")
        lines.append(f"- **Critical/High risk recalls:** {s['critical_count'] + s['high_count']}\n")

        lines.append("## Key Insights\n")
        for insight in report["insights"]:
            lines.append(f"- {insight}")
        lines.append("")

        lines.append("## Recalls by Classification\n")
        lines.append(self._df_to_md(sections["by_classification"]))

        lines.append("\n## Recalls by Product Type\n")
        lines.append(self._df_to_md(sections["by_product_type"]))

        if not sections["by_therapeutic_area"].empty:
            lines.append("\n## Top Therapeutic Areas (Drugs)\n")
            lines.append(self._df_to_md(sections["by_therapeutic_area"].head(10)))

        lines.append("\n## Top Recall Reasons\n")
        lines.append(self._df_to_md(sections["by_reason_category"].head(10)))

        lines.append(f"\n## Top Manufacturers by Priority Score\n")
        lines.append(self._df_to_md(sections["top_manufacturers"].head(self.top_n_manufacturers)))

        if not sections["repeat_offenders"].empty:
            lines.append(f"\n## Repeat Class I Offenders\n")
            lines.append(self._df_to_md(sections["repeat_offenders"]))

        lines.append(f"\n## Risk Tier Distribution\n")
        lines.append(self._df_to_md(sections["risk_tier_distribution"]))

        lines.append(f"\n## Seasonal Pattern\n")
        lines.append(self._df_to_md(sections["seasonal_profile"]))

        trend = sections["trend_stats"]
        if trend["n_months"] >= 3:
            direction = "increasing" if trend["slope"] > 0 else "decreasing"
            lines.append(f"\n## Temporal Trend\n")
            lines.append(
                f"- Over {trend['n_months']} months, recalls are **{direction}** at "
                f"{abs(trend['slope']):.2f} recalls/month "
                f"(mean = {trend['mean']:.1f}/month)."
            )

        chi = sections["reason_vs_severity_test"]
        if chi.get("p_value") is not None:
            lines.append(f"\n## Statistical Findings\n")
            lines.append(
                f"- **Reason vs Severity**: χ²={chi['statistic']}, p={chi['p_value']} "
                f"({chi['interpretation']})"
            )
            corr_result = sections["reach_vs_risk_correlation"]
            if corr_result.get("r") is not None:
                lines.append(
                    f"- **Distribution reach vs Risk score**: r={corr_result['r']} "
                    f"(n={corr_result['n']})"
                )

        return "\n".join(lines)

    @staticmethod
    def _summary_metrics(scored: pd.DataFrame) -> dict:
        date_col = "report_date"
        if date_col in scored.columns:
            dates = pd.to_datetime(scored[date_col], errors="coerce").dropna()
        else:
            dates = pd.Series(dtype="datetime64[ns]")
        total = len(scored)
        class_i = int((scored["classification"] == "Class I").sum())
        tiers = scored.get("risk_tier")
        critical = int((tiers == "Critical").sum()) if tiers is not None else 0
        high = int((tiers == "High").sum()) if tiers is not None else 0
        return {
            "total_recalls": total,
            "earliest": str(dates.min().date()) if not dates.empty else "n/a",
            "latest": str(dates.max().date()) if not dates.empty else "n/a",
            "unique_firms": int(scored["recalling_firm"].nunique()),
            "class_i": class_i,
            "class_i_pct": round(class_i / total * 100, 1) if total else 0,
            "avg_risk_score": float(scored["risk_score"].mean()) if "risk_score" in scored.columns else 0.0,
            "critical_count": critical,
            "high_count": high,
        }

    @staticmethod
    def _generate_insights(scored: pd.DataFrame, sections: dict) -> list[str]:
        """Generate short plain-English insights from the computed sections."""
        insights: list[str] = []

        # Dominant classification
        by_class = sections["by_classification"]
        if not by_class.empty:
            top_class = by_class.iloc[0]
            insights.append(
                f"Most common classification is **{top_class['classification'] or 'Unclassified'}** "
                f"with {top_class['total_recalls']:,} recalls "
                f"({top_class['total_recalls'] / len(scored) * 100:.0f}% of total)."
            )

        # Dominant product type
        by_type = sections["by_product_type"]
        if not by_type.empty:
            top_type = by_type.iloc[0]
            insights.append(
                f"**{top_type['product_type']}** accounts for the largest share with "
                f"{top_type['total_recalls']:,} recalls ({top_type['class_i_pct']}% Class I)."
            )

        # Top manufacturer
        top_mfrs = sections["top_manufacturers"]
        if not top_mfrs.empty:
            top = top_mfrs.iloc[0]
            insights.append(
                f"Highest-priority manufacturer: **{top['manufacturer']}** with "
                f"{int(top['total_recalls'])} recalls "
                f"(priority score {top['priority_score']:.1f})."
            )

        # Repeat offenders warning
        repeat = sections["repeat_offenders"]
        if not repeat.empty:
            insights.append(
                f"{len(repeat)} firms have **multiple Class I recalls** — "
                f"these merit closer regulatory attention."
            )

        # Spike detection
        spikes = sections["spikes"]
        if not spikes.empty and "is_spike" in spikes.columns and spikes["is_spike"].any():
            n_spikes = int(spikes["is_spike"].sum())
            insights.append(
                f"Detected **{n_spikes} month(s)** with anomalously high or low recall counts."
            )

        # Trend
        trend = sections["trend_stats"]
        if trend["n_months"] >= 6:
            if trend["slope"] > 0.5:
                insights.append(f"Recall volume is trending **upward** (+{trend['slope']:.1f}/month).")
            elif trend["slope"] < -0.5:
                insights.append(f"Recall volume is trending **downward** ({trend['slope']:.1f}/month).")

        # Top therapeutic area
        ta = sections["by_therapeutic_area"]
        if not ta.empty and ta.iloc[0]["therapeutic_area"] != "Other":
            top_ta = ta.iloc[0]
            insights.append(
                f"Most-recalled drug therapeutic area: **{top_ta['therapeutic_area']}** "
                f"({int(top_ta['total_recalls'])} recalls)."
            )

        # Risk tier distribution
        tiers = sections["risk_tier_distribution"]
        if not tiers.empty:
            critical_row = tiers[tiers["risk_tier"] == "Critical"]
            if not critical_row.empty and int(critical_row.iloc[0]["count"]) > 0:
                insights.append(
                    f"**{int(critical_row.iloc[0]['count'])} recalls** scored in the Critical tier "
                    f"(risk score ≥ 70)."
                )

        return insights

    @staticmethod
    def _df_to_md(df: pd.DataFrame) -> str:
        """Convert a DataFrame to a markdown table, handling the empty case."""
        if df is None or (hasattr(df, "empty") and df.empty):
            return "_No data._"
        try:
            return df.to_markdown(index=False)
        except ImportError:
            # `tabulate` not installed — fall back to a simple pipe-table.
            cols = list(df.columns)
            lines = [
                "| " + " | ".join(str(c) for c in cols) + " |",
                "| " + " | ".join("---" for _ in cols) + " |",
            ]
            for _, row in df.iterrows():
                lines.append("| " + " | ".join(str(row[c]) for c in cols) + " |")
            return "\n".join(lines)
