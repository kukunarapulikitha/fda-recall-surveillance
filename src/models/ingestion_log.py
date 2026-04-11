"""Ingestion log model for pipeline observability."""

from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import Date, DateTime, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.models.base import Base


class IngestionLog(Base):
    __tablename__ = "ingestion_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), default=lambda: str(uuid4()), nullable=False
    )
    run_type: Mapped[str] = mapped_column(String(20), nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(100), nullable=False)
    date_range_start: Mapped[date | None] = mapped_column(Date)
    date_range_end: Mapped[date | None] = mapped_column(Date)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    records_fetched: Mapped[int] = mapped_column(Integer, default=0)
    records_inserted: Mapped[int] = mapped_column(Integer, default=0)
    records_updated: Mapped[int] = mapped_column(Integer, default=0)
    records_skipped: Mapped[int] = mapped_column(Integer, default=0)
    validation_failures: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(20), default="running")
    error_message: Mapped[str | None] = mapped_column(Text)
    duration_seconds: Mapped[float | None] = mapped_column(Numeric(10, 2))

    def __repr__(self) -> str:
        return f"<IngestionLog {self.run_type}/{self.endpoint} [{self.status}]>"
