"""Shared data-loading helpers used by the analytics modules."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.models.base import engine as default_engine

RECALL_COLUMNS = [
    "id",
    "recall_number",
    "source",
    "product_type",
    "classification",
    "status",
    "recalling_firm",
    "reason_for_recall",
    "product_description",
    "product_quantity",
    "distribution_pattern",
    "voluntary_mandated",
    "recall_initiation_date",
    "report_date",
    "termination_date",
    "city",
    "state",
    "country",
    "brand_name",
    "generic_name",
    "manufacturer_name",
    "substance_name",
    "route",
]


def load_recalls(
    start_date: date | None = None,
    end_date: date | None = None,
    product_type: str | None = None,
    eng: Engine | None = None,
) -> pd.DataFrame:
    """Load recall records into a DataFrame, filtered by date range and/or product type."""
    eng = eng or default_engine
    clauses = []
    params: dict = {}

    if start_date is not None:
        clauses.append("COALESCE(report_date, recall_initiation_date) >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        clauses.append("COALESCE(report_date, recall_initiation_date) <= :end_date")
        params["end_date"] = end_date
    if product_type is not None:
        clauses.append("product_type = :product_type")
        params["product_type"] = product_type

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    cols = ", ".join(RECALL_COLUMNS)
    sql = text(f"SELECT {cols} FROM recalls {where} ORDER BY report_date DESC")

    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    for col in ("report_date", "recall_initiation_date", "termination_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def load_firms(eng: Engine | None = None) -> pd.DataFrame:
    """Load aggregated firm statistics."""
    eng = eng or default_engine
    with eng.connect() as conn:
        return pd.read_sql(
            text(
                "SELECT name, total_recalls, class_i_count, class_ii_count, class_iii_count, "
                "first_recall_date, latest_recall_date FROM firms"
            ),
            conn,
        )
