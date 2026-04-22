"""Categorize recalls by therapeutic area, drug class, device type, and severity.

Since FDA recall records do not carry a canonical therapeutic-area label, we
derive one from keywords in the brand/generic/substance names and the recall
reason. The taxonomy is intentionally coarse — these buckets are meant for
trend analysis, not clinical decision-making.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

# Ordered rules — first match wins. More specific rules come first.
THERAPEUTIC_AREA_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Oncology", (
        "cancer", "oncolog", "tumor", "chemother", "leukemia", "lymphoma",
        "cytotoxic", "methotrex", "paclitax", "cisplatin",
    )),
    ("Cardiovascular", (
        "hypertens", "cardia", "blood pressure", "statin", "lipitor",
        "atorvastat", "simvastat", "warfarin", "heparin", "anticoag",
        "metoprolol", "amlodipin", "losartan", "valsartan", "lisinopril",
        "clopidogrel", "rivaroxaban",
    )),
    ("Diabetes & Endocrine", (
        "insulin", "diabet", "metformin", "glipizide", "glyburide",
        "glucose", "levothyrox", "thyroid", "liraglutid", "semaglutid",
    )),
    ("Anti-Infective", (
        "antibio", "antimicrob", "antibact", "antifung", "antiviral",
        "penicillin", "amoxicill", "azithro", "cephalosporin", "cipro",
        "doxycyclin", "vancomycin", "metronidazol", "fluconazole",
    )),
    ("Pain & Analgesic", (
        "analges", "ibuprofen", "acetaminophen", "aspirin", "naproxen",
        "opioid", "oxycodone", "hydrocodone", "morphine", "fentanyl",
        "tramadol", "codeine",
    )),
    ("Psychiatric & Neurological", (
        "antidepress", "antipsych", "anxi", "sertralin", "fluoxet",
        "prozac", "lorazepam", "alprazol", "diazepam", "gabapentin",
        "levetirac", "seizure", "epilep", "parkinson",
    )),
    ("Respiratory", (
        "asthma", "inhaler", "bronch", "albuterol", "montelukast",
        "fluticason", "copd", "respir",
    )),
    ("Gastrointestinal", (
        "gastro", "ulcer", "heartburn", "omeprazol", "pantopraz",
        "ranitidine", "antacid", "laxat",
    )),
    ("Dermatological", (
        "derma", "topical", "cream", "ointment", "lotion", "hydrocortison",
        "skin",
    )),
    ("Ophthalmic", (
        "ophthalm", "eye drop", "ocular", "timolol",
    )),
    ("Hormonal & Contraceptive", (
        "contracept", "estrogen", "progester", "testosteron", "hormon",
    )),
    ("Vaccine & Biologic", (
        "vaccine", "immunoglob", "biolog", "monoclonal",
    )),
    ("Nutritional & Supplement", (
        "vitamin", "supplement", "mineral", "probiot", "protein",
    )),
]

DEVICE_TYPE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Cardiac Devices", (
        "pacemaker", "defibrillat", "stent", "cardiac", "heart", "vascular",
    )),
    ("Diagnostic Imaging", (
        "mri", "ct scan", "x-ray", "ultrasound", "imaging", "radiog", "pet scan",
    )),
    ("Surgical Instruments", (
        "surg", "scalpel", "forceps", "clamp", "trocar", "suture",
    )),
    ("Infusion & Delivery", (
        "infusion", "pump", "catheter", "syringe", "needle", "iv set",
    )),
    ("Monitoring Equipment", (
        "monitor", "pulse ox", "ecg", "ekg", "blood pressure",
    )),
    ("Orthopedic", (
        "orthoped", "implant", "prosthe", "knee", "hip", "bone",
    )),
    ("In Vitro Diagnostic", (
        "test kit", "assay", "glucose meter", "reagent", "in vitro",
        "diagnostic", "strip",
    )),
    ("Respiratory Devices", (
        "ventilat", "cpap", "bipap", "oxygen concentrator",
    )),
    ("Dental", (
        "dental", "tooth", "oral",
    )),
    ("Ophthalmic Devices", (
        "intraocular", "contact lens", "eye",
    )),
]

FOOD_TYPE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Dairy", ("milk", "cheese", "yogurt", "dairy", "butter", "cream")),
    ("Meat & Poultry", ("beef", "pork", "chicken", "turkey", "meat", "poultry", "sausage", "bacon")),
    ("Seafood", ("fish", "salmon", "tuna", "shrimp", "seafood", "oyster", "crab")),
    ("Produce", ("lettuce", "spinach", "tomato", "onion", "fruit", "vegetable", "melon", "berries")),
    ("Bakery", ("bread", "bakery", "pastry", "cookie", "cake", "muffin")),
    ("Beverages", ("juice", "soda", "beverage", "drink", "water", "coffee", "tea")),
    ("Snacks & Confectionery", ("snack", "chip", "candy", "chocolate", "pretzel", "nut")),
    ("Infant Food", ("infant", "baby food", "formula")),
    ("Prepared Meals", ("frozen meal", "entree", "prepared", "soup", "pizza", "pasta")),
    ("Condiments & Sauces", ("sauce", "dressing", "condiment", "spice", "seasoning")),
]

CLASS_SEVERITY = {"Class I": 3, "Class II": 2, "Class III": 1}


def _first_keyword_match(text_blob: str, rules: list[tuple[str, tuple[str, ...]]]) -> str:
    """Return the first category whose keyword list matches the blob, else 'Other'."""
    if not text_blob:
        return "Other"
    haystack = text_blob.lower()
    for label, keywords in rules:
        for kw in keywords:
            if kw in haystack:
                return label
    return "Other"


def categorize_therapeutic_area(
    generic_name: str | None = None,
    brand_name: str | None = None,
    substance_name: str | None = None,
    product_description: str | None = None,
    reason_for_recall: str | None = None,
) -> str:
    """Best-guess therapeutic area from drug name and recall details."""
    blob = " ".join(
        part for part in (generic_name, brand_name, substance_name, product_description, reason_for_recall)
        if isinstance(part, str) and part
    )
    return _first_keyword_match(blob, THERAPEUTIC_AREA_RULES)


def categorize_device_type(
    product_description: str | None = None,
    brand_name: str | None = None,
    reason_for_recall: str | None = None,
) -> str:
    """Best-guess device category from the product description."""
    blob = " ".join(
        part for part in (product_description, brand_name, reason_for_recall)
        if isinstance(part, str) and part
    )
    return _first_keyword_match(blob, DEVICE_TYPE_RULES)


def categorize_food_type(
    product_description: str | None = None,
    reason_for_recall: str | None = None,
) -> str:
    """Best-guess food category from the product description."""
    blob = " ".join(
        part for part in (product_description, reason_for_recall)
        if isinstance(part, str) and part
    )
    return _first_keyword_match(blob, FOOD_TYPE_RULES)


# Reason-for-recall buckets (shared across product types).
RECALL_REASON_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Contamination", ("contamin", "foreign material", "particul", "bacteria", "salmonella",
                        "listeria", "e. coli", "mold", "fungal")),
    ("Mislabeling", ("label", "mislabel", "undeclar", "allergen", "misbrand")),
    ("Manufacturing Defect", ("manufactur", "defect", "quality", "gmp", "cgmp")),
    ("Out of Specification", ("dissolution", "out of spec", "potency", "assay", "stability",
                              "impurit", "subpotent", "superpotent")),
    ("Packaging Issue", ("packag", "seal", "container", "leak")),
    ("Adverse Event", ("adverse", "injury", "death", "serious")),
    ("Malfunction", ("malfunct", "fail", "not functioning", "software")),
    ("Sterility", ("steril", "non-sterile", "contaminated", "pyrogen")),
    ("Dosing Error", ("dose", "dosage", "overdose", "underdose")),
]


def categorize_recall_reason(reason_for_recall: str | None) -> str:
    """Bucket a free-text reason-for-recall into a coarse category."""
    if not isinstance(reason_for_recall, str) or not reason_for_recall:
        return "Unknown"
    return _first_keyword_match(reason_for_recall, RECALL_REASON_RULES)


@dataclass
class RecallCategorizer:
    """Adds derived category columns to a recall DataFrame."""

    def categorize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a new DataFrame with therapeutic_area, product_category, and reason_category columns."""
        if df.empty:
            return df.assign(therapeutic_area=pd.Series(dtype="object"),
                             product_category=pd.Series(dtype="object"),
                             reason_category=pd.Series(dtype="object"))

        out = df.copy()
        out["therapeutic_area"] = out.apply(self._therapeutic_area, axis=1)
        out["product_category"] = out.apply(self._product_category, axis=1)
        out["reason_category"] = out["reason_for_recall"].apply(categorize_recall_reason)
        out["severity_rank"] = out["classification"].map(CLASS_SEVERITY).fillna(0).astype(int)
        return out

    @staticmethod
    def _therapeutic_area(row: pd.Series) -> str:
        if row.get("product_type") != "Drugs":
            return "N/A"
        return categorize_therapeutic_area(
            generic_name=row.get("generic_name"),
            brand_name=row.get("brand_name"),
            substance_name=row.get("substance_name"),
            product_description=row.get("product_description"),
            reason_for_recall=row.get("reason_for_recall"),
        )

    @staticmethod
    def _product_category(row: pd.Series) -> str:
        ptype = row.get("product_type")
        if ptype == "Drugs":
            # For drugs, use the route as a coarse product category
            route = row.get("route")
            if route:
                return str(route).title()
            return "Unspecified Route"
        if ptype == "Devices":
            return categorize_device_type(
                product_description=row.get("product_description"),
                brand_name=row.get("brand_name"),
                reason_for_recall=row.get("reason_for_recall"),
            )
        if ptype == "Food":
            return categorize_food_type(
                product_description=row.get("product_description"),
                reason_for_recall=row.get("reason_for_recall"),
            )
        return "Other"

    @staticmethod
    def summary_by(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Count recalls and class-I recalls broken down by a category column."""
        if df.empty or column not in df.columns:
            return pd.DataFrame(columns=[column, "total_recalls", "class_i_recalls", "class_i_pct"])
        grouped = df.groupby(column, dropna=False).agg(
            total_recalls=("recall_number", "count"),
            class_i_recalls=("classification", lambda s: (s == "Class I").sum()),
        ).reset_index()
        grouped["class_i_pct"] = (
            grouped["class_i_recalls"] / grouped["total_recalls"].replace(0, pd.NA) * 100
        ).round(1)
        return grouped.sort_values("total_recalls", ascending=False).reset_index(drop=True)


# Regex used by other modules to parse "50,000 bottles" style quantities.
_QUANTITY_NUMBER_RE = re.compile(r"[\d,]+")


def parse_quantity(value: str | None) -> int | None:
    """Extract the first integer from a product_quantity string like '50,000 bottles'."""
    if not value:
        return None
    match = _QUANTITY_NUMBER_RE.search(value)
    if not match:
        return None
    try:
        return int(match.group(0).replace(",", ""))
    except ValueError:
        return None
