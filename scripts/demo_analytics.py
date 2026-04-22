#!/usr/bin/env python
"""End-to-end analytics demo using a realistic synthetic recall dataset.

No database required. Exercises every analytics module and prints the outputs.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import pandas as pd

from src.analytics.categorize import RecallCategorizer
from src.analytics.correlation import CorrelationAnalyzer
from src.analytics.high_risk import HighRiskIdentifier
from src.analytics.reports import ExecutiveReport
from src.analytics.risk_scoring import RiskScorer
from src.analytics.temporal import TemporalAnalyzer

random.seed(42)

# Realistic pools of drug/device/food names paired with the category they should map to.
DRUGS = [
    ("METFORMIN HYDROCHLORIDE", "GLUCOPHAGE", "ORAL"),
    ("ATORVASTATIN CALCIUM", "LIPITOR", "ORAL"),
    ("LISINOPRIL", "PRINIVIL", "ORAL"),
    ("AMOXICILLIN", "AMOXIL", "ORAL"),
    ("IBUPROFEN", "ADVIL", "ORAL"),
    ("SERTRALINE HCL", "ZOLOFT", "ORAL"),
    ("ALBUTEROL SULFATE", "PROVENTIL", "INHALATION"),
    ("OMEPRAZOLE", "PRILOSEC", "ORAL"),
    ("INSULIN GLARGINE", "LANTUS", "SUBCUTANEOUS"),
    ("LEVOTHYROXINE SODIUM", "SYNTHROID", "ORAL"),
    ("WARFARIN SODIUM", "COUMADIN", "ORAL"),
    ("HYDROCODONE", "VICODIN", "ORAL"),
    ("AZITHROMYCIN", "ZITHROMAX", "ORAL"),
    ("METOPROLOL TARTRATE", "LOPRESSOR", "ORAL"),
    ("GABAPENTIN", "NEURONTIN", "ORAL"),
    ("METHOTREXATE SODIUM", "TREXALL", "ORAL"),
    ("HYDROCORTISONE", "CORTEF", "TOPICAL"),
]

DEVICES = [
    ("Cardiac pacemaker lead, single-chamber", "CardioMon 3000"),
    ("Implantable cardioverter defibrillator", "DefibX 500"),
    ("Infusion pump for IV administration", "InfusePro"),
    ("Surgical scalpel, disposable", "SurgiCut"),
    ("Patient monitor with pulse oximetry", "PulseOx 200"),
    ("Knee implant prosthesis", "OrthoKnee"),
    ("Glucose meter test strip", "GlucoStrip"),
    ("CPAP continuous positive airway pressure", "BreatheEZ"),
    ("MRI imaging coil", "ImagePro MRI"),
    ("Intraocular lens implant", "ClearView IOL"),
]

FOODS = [
    ("Romaine lettuce, bagged 8oz", "Contamination"),
    ("Ground beef 80/20, 1lb packs", "Contamination"),
    ("Milk, whole gallon", "Contamination"),
    ("Frozen chicken nuggets, 2lb bag", "Contamination"),
    ("Almond snack packs", "Mislabeling"),
    ("Cheese slices, american", "Contamination"),
    ("Infant formula, powdered", "Contamination"),
    ("Yogurt, strawberry 32oz", "Contamination"),
    ("Frozen pizza, pepperoni", "Mislabeling"),
    ("Peanut butter, creamy 16oz", "Contamination"),
]

FIRMS_BY_TYPE = {
    "Drugs": ["MegaPharma Inc", "Acme Pharmaceuticals", "NovaMed Labs", "Prime Rx Co",
             "GenericoHealth", "BioScience Ltd", "UniCorp Drugs"],
    "Devices": ["MedDevice Corp", "CardioTech Systems", "OrthoPro Inc", "DiagnoseMed"],
    "Food": ["SunFarm Foods", "Valley Dairy Co", "FreshHarvest LLC", "GoodEats Brands",
             "Heritage Foods"],
}

DISTRIBUTIONS = [
    "Nationwide",
    "CA, NY, TX, FL",
    "MA, CT, RI",
    "Only CA",
    "Only TX",
    "Nationwide and Canada",
    "IL, IN, OH, KY",
]

REASONS_BY_TYPE = {
    "Drugs": [
        "Failed Dissolution Specifications: Out of specification dissolution",
        "Presence of foreign particulate matter",
        "Subpotent: Out of specification assay results",
        "Mislabeling: incorrect strength on label",
        "Potential contamination with bacteria",
        "Stability failure: impurities above threshold",
    ],
    "Devices": [
        "Device malfunction may cause injury",
        "Software defect causing incorrect readings",
        "Risk of non-sterility",
        "Manufacturing defect: faulty seal",
        "Packaging issue may compromise sterility",
    ],
    "Food": [
        "Salmonella contamination detected",
        "Listeria monocytogenes contamination",
        "Undeclared allergen: peanut",
        "Undeclared allergen: milk",
        "E. coli O157:H7 contamination",
        "Foreign material: metal fragments",
    ],
}

STATES = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "MA", "GA", "NC"]


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def build_dataset(n: int = 300) -> pd.DataFrame:
    """Generate a realistic synthetic recall dataset."""
    rows = []
    type_weights = {"Drugs": 0.55, "Devices": 0.20, "Food": 0.25}
    types = list(type_weights)
    weights = list(type_weights.values())

    for i in range(n):
        product_type = random.choices(types, weights=weights)[0]
        firm = random.choice(FIRMS_BY_TYPE[product_type])
        state = random.choice(STATES)
        classification = random.choices(
            ["Class I", "Class II", "Class III"],
            weights=[0.15, 0.65, 0.20],
        )[0]
        status = random.choices(["Ongoing", "Terminated", "Completed"],
                                weights=[0.6, 0.3, 0.1])[0]
        distribution = random.choice(DISTRIBUTIONS)
        quantity = f"{random.choice([500, 5000, 50000, 500000, 2_000_000])} units"

        # Introduce a "repeat offender" signal for MegaPharma
        if firm == "MegaPharma Inc" and random.random() < 0.4:
            classification = "Class I"

        if product_type == "Drugs":
            generic, brand, route = random.choice(DRUGS)
            substance = generic
            description = f"{brand} {random.choice([100, 250, 500])}mg tablets"
        elif product_type == "Devices":
            description, brand = random.choice(DEVICES)
            generic, substance, route = None, None, None
        else:
            description, _ = random.choice(FOODS)
            generic, brand, substance, route = None, None, None, None

        # Seasonality: food recalls spike in summer, drugs in Q1
        month_weights = [1] * 12
        if product_type == "Food":
            for m in (5, 6, 7, 8):
                month_weights[m - 1] = 3
        elif product_type == "Drugs":
            for m in (1, 2, 3):
                month_weights[m - 1] = 2
        year = random.choices([2022, 2023, 2024], weights=[0.25, 0.35, 0.40])[0]
        month = random.choices(range(1, 13), weights=month_weights)[0]
        day = random.randint(1, 28)
        report_date = date(year, month, day)

        rows.append({
            "recall_number": f"{product_type[0]}-{i:04d}-{year}",
            "source": "openfda_api",
            "product_type": product_type,
            "classification": classification,
            "status": status,
            "recalling_firm": firm,
            "manufacturer_name": firm,
            "reason_for_recall": random.choice(REASONS_BY_TYPE[product_type]),
            "product_description": description,
            "product_quantity": quantity,
            "distribution_pattern": distribution,
            "voluntary_mandated": "Voluntary: Firm Initiated",
            "recall_initiation_date": report_date - timedelta(days=random.randint(1, 10)),
            "report_date": report_date,
            "termination_date": report_date + timedelta(days=random.randint(30, 180)) if status == "Terminated" else None,
            "city": "Springfield", "state": state, "country": "United States",
            "brand_name": brand, "generic_name": generic, "substance_name": substance,
            "route": route,
        })
    return pd.DataFrame(rows)


def banner(title: str) -> None:
    bar = "=" * 78
    print(f"\n{bar}\n  {title}\n{bar}")


def main() -> None:
    print("Generating synthetic dataset (300 recalls, 2022-2024)...")
    df = build_dataset(300)
    print(f"  -> {len(df)} rows, "
          f"date range {df['report_date'].min()} to {df['report_date'].max()}")
    print(f"  -> product types: {df['product_type'].value_counts().to_dict()}")
    print(f"  -> classifications: {df['classification'].value_counts().to_dict()}")

    # 1. Categorization
    banner("1. CATEGORIZATION")
    categorized = RecallCategorizer().categorize(df)
    print("\nTherapeutic area breakdown (Drugs only):")
    drugs = categorized[categorized["product_type"] == "Drugs"]
    print(RecallCategorizer.summary_by(drugs, "therapeutic_area").head(10).to_string(index=False))

    print("\nProduct category breakdown:")
    print(RecallCategorizer.summary_by(categorized, "product_category").head(10).to_string(index=False))

    print("\nRecall reason breakdown:")
    print(RecallCategorizer.summary_by(categorized, "reason_category").to_string(index=False))

    # 2. Risk Scoring
    banner("2. RISK SCORING")
    scored = RiskScorer().score(categorized)
    print(f"\nRisk score stats: mean={scored['risk_score'].mean():.1f}, "
          f"max={scored['risk_score'].max():.1f}, "
          f"min={scored['risk_score'].min():.1f}")
    print("\nRisk tier distribution:")
    print(RiskScorer.tier_distribution(scored).to_string(index=False))

    print("\nTop 5 highest-scored recalls:")
    top_risky = scored.nlargest(5, "risk_score")[
        ["recall_number", "product_type", "classification",
         "recalling_firm", "risk_score", "risk_tier"]
    ]
    print(top_risky.to_string(index=False))

    # 3. Temporal Analysis
    banner("3. TEMPORAL PATTERN ANALYSIS")
    temporal = TemporalAnalyzer()
    print("\nMonthly counts (last 6 months):")
    monthly = temporal.monthly_counts(scored, by=None).tail(6)
    print(monthly.to_string(index=False))

    print("\nQuarterly counts:")
    quarterly = temporal.quarterly_counts(scored, by=None)
    print(quarterly.to_string(index=False))

    print("\nSeasonal pattern (all years combined):")
    seasonal = temporal.seasonal_counts(scored, by="product_type")
    print(seasonal.to_string(index=False))

    trend = temporal.trend(scored)
    print(f"\nTrend: slope={trend['slope']:+.3f} recalls/month "
          f"(mean={trend['mean']:.1f}, n={trend['n_months']} months)")

    spikes = temporal.spike_detection(scored)
    if spikes["is_spike"].any():
        print(f"\nAnomalous months (|z| >= 2):")
        print(spikes[spikes["is_spike"]][["month", "count", "z_score"]].to_string(index=False))
    else:
        print("\nNo anomalous months detected.")

    # 4. High-Risk Identification
    banner("4. HIGH-RISK MANUFACTURERS & PRODUCT CATEGORIES")
    identifier = HighRiskIdentifier(min_recalls=3, top_n=10)

    print("\nTop manufacturers by priority score:")
    top_mfrs = identifier.top_manufacturers(scored)
    print(top_mfrs[["manufacturer", "total_recalls", "class_i",
                    "avg_risk_score", "priority_score"]].to_string(index=False))

    print("\nRepeat Class I offenders (firms with >= 2 Class I recalls):")
    offenders = identifier.repeat_offenders(scored)
    print(offenders.to_string(index=False) if not offenders.empty else "  (none)")

    print("\nTop product categories:")
    top_cats = identifier.top_product_categories(scored).head(8)
    print(top_cats[["product_category", "total_recalls", "class_i",
                    "avg_risk_score", "priority_score"]].to_string(index=False))

    # 5. Correlation Analysis
    banner("5. CORRELATION ANALYSIS")
    corr = CorrelationAnalyzer()

    print("\nRecall count by state x product type:")
    print(corr.type_by_state(scored).head(10).to_string())

    print("\nClassification x distribution reach:")
    print(corr.severity_by_reach(scored).to_string())

    print("\nReason category x product type (column %):")
    print(corr.reason_by_product_type(scored).to_string())

    chi = corr.chi_square_reason_vs_severity(scored)
    print(f"\nChi-square test (reason vs severity):")
    print(f"  chi-squared = {chi['statistic']}, p = {chi['p_value']}")
    print(f"  dof = {chi['dof']}, n = {chi['n']}")
    print(f"  interpretation: {chi['interpretation']}")

    pearson = corr.pearson_recalls_vs_reach(scored)
    print(f"\nPearson correlation (distribution reach vs risk score):")
    print(f"  r = {pearson['r']}, p = {pearson['p_value']}, n = {pearson['n']}")

    # 6. Executive Summary
    banner("6. EXECUTIVE SUMMARY REPORT")
    report_md = ExecutiveReport().render_markdown(df)
    out_path = "demo_report.md"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report_md)
    print(f"\nFull markdown report written to {out_path} ({len(report_md):,} chars)")
    print("\n--- First 60 lines of the report ---\n")
    for line in report_md.splitlines()[:60]:
        try:
            print(line)
        except UnicodeEncodeError:
            print(line.encode("ascii", "replace").decode("ascii"))

    banner("DONE")


if __name__ == "__main__":
    main()
