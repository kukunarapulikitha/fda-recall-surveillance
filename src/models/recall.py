"""Recall database model."""

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.models.base import Base


class Recall(Base):
    __tablename__ = "recalls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    recall_number: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    event_id: Mapped[str | None] = mapped_column(String(50))
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    product_type: Mapped[str] = mapped_column(String(20), nullable=False)
    classification: Mapped[str | None] = mapped_column(String(20))
    status: Mapped[str | None] = mapped_column(String(30))
    recalling_firm: Mapped[str] = mapped_column(String(500), nullable=False)
    reason_for_recall: Mapped[str] = mapped_column(Text, nullable=False)
    product_description: Mapped[str | None] = mapped_column(Text)
    product_quantity: Mapped[str | None] = mapped_column(String(500))
    code_info: Mapped[str | None] = mapped_column(Text)
    distribution_pattern: Mapped[str | None] = mapped_column(Text)
    voluntary_mandated: Mapped[str | None] = mapped_column(String(100))
    initial_firm_notification: Mapped[str | None] = mapped_column(String(100))
    recall_initiation_date: Mapped[date | None] = mapped_column(Date)
    report_date: Mapped[date | None] = mapped_column(Date)
    center_classification_date: Mapped[date | None] = mapped_column(Date)
    termination_date: Mapped[date | None] = mapped_column(Date)
    city: Mapped[str | None] = mapped_column(String(200))
    state: Mapped[str | None] = mapped_column(String(10))
    country: Mapped[str | None] = mapped_column(String(100))
    postal_code: Mapped[str | None] = mapped_column(String(20))

    # OpenFDA flattened fields
    brand_name: Mapped[str | None] = mapped_column(Text)
    generic_name: Mapped[str | None] = mapped_column(Text)
    manufacturer_name: Mapped[str | None] = mapped_column(Text)
    product_ndc: Mapped[str | None] = mapped_column(Text)
    substance_name: Mapped[str | None] = mapped_column(Text)
    route: Mapped[str | None] = mapped_column(Text)
    application_number: Mapped[str | None] = mapped_column(String(50))

    # Metadata
    raw_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    is_validated: Mapped[bool] = mapped_column(Boolean, default=True)
    validation_errors: Mapped[dict | None] = mapped_column(JSONB)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_recalls_product_type", "product_type"),
        Index("idx_recalls_classification", "classification"),
        Index("idx_recalls_status", "status"),
        Index("idx_recalls_report_date", "report_date"),
        Index("idx_recalls_recalling_firm", "recalling_firm"),
        Index("idx_recalls_recall_initiation", "recall_initiation_date"),
        Index("idx_recalls_source", "source"),
    )

    def __repr__(self) -> str:
        return f"<Recall {self.recall_number} [{self.classification}]>"
