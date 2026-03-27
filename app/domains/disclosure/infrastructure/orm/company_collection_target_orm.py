from datetime import datetime

from sqlalchemy import String, Boolean, DateTime, CheckConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class CompanyCollectionTargetOrm(Base):
    __tablename__ = "company_collection_targets"
    __table_args__ = (
        CheckConstraint(
            "target_type IN ('top300', 'ondemand', 'promoted')",
            name="chk_company_collection_targets_target_type",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corp_code: Mapped[str] = mapped_column(
        String(8), ForeignKey("companies.corp_code"), nullable=False, unique=True
    )
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    added_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    added_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
