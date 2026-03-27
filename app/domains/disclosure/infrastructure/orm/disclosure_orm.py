from datetime import date, datetime

from sqlalchemy import String, Date, Boolean, DateTime, CheckConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class DisclosureOrm(Base):
    __tablename__ = "disclosures"
    __table_args__ = (
        CheckConstraint(
            "source_mode IN ('scheduled', 'ondemand')",
            name="chk_disclosures_source_mode",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rcept_no: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    corp_code: Mapped[str] = mapped_column(
        String(8), ForeignKey("companies.corp_code"), nullable=False
    )
    report_nm: Mapped[str] = mapped_column(String(500), nullable=False)
    rcept_dt: Mapped[date] = mapped_column(Date, nullable=False)
    pblntf_ty: Mapped[str | None] = mapped_column(String(10), nullable=True)
    pblntf_detail_ty: Mapped[str | None] = mapped_column(String(10), nullable=True)
    rm: Mapped[str | None] = mapped_column(String(100), nullable=True)
    disclosure_group: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="scheduled")
    is_core: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
