"""Firm aggregate model."""

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from src.models.base import Base


class Firm(Base):
    __tablename__ = "firms"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    city: Mapped[str | None] = mapped_column(String(200))
    state: Mapped[str | None] = mapped_column(String(10))
    country: Mapped[str | None] = mapped_column(String(100))
    total_recalls: Mapped[int] = mapped_column(Integer, default=0)
    class_i_count: Mapped[int] = mapped_column(Integer, default=0)
    class_ii_count: Mapped[int] = mapped_column(Integer, default=0)
    class_iii_count: Mapped[int] = mapped_column(Integer, default=0)
    first_recall_date: Mapped[date | None] = mapped_column(Date)
    latest_recall_date: Mapped[date | None] = mapped_column(Date)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<Firm {self.name} (recalls={self.total_recalls})>"
