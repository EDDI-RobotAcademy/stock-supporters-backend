from datetime import datetime

from sqlalchemy import String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class CompanyDataCoverageOrm(Base):
    __tablename__ = "company_data_coverage"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corp_code: Mapped[str] = mapped_column(
        String(8), ForeignKey("companies.corp_code"), nullable=False, unique=True
    )
    has_b001: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_d002_d005: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_d001: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_e001: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_c001: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_a001: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_a002: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_a003: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_event_documents: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_collected_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_on_demand_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
