from datetime import datetime

from sqlalchemy import String, Integer, Boolean, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class CompanyOrm(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    corp_code: Mapped[str] = mapped_column(String(8), nullable=False, unique=True)
    corp_name: Mapped[str] = mapped_column(String(255), nullable=False)
    stock_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    market_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    market_cap_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_top300: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_collect_target: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_requested_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
