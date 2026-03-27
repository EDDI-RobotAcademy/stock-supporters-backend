from datetime import datetime

from sqlalchemy import String, Integer, Text, DateTime, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class CollectionJobOrm(Base):
    __tablename__ = "collection_jobs"
    __table_args__ = (
        CheckConstraint(
            "job_type IN ('hourly', 'daily', 'seasonal', 'cleanup', 'ondemand')",
            name="chk_collection_jobs_type",
        ),
        CheckConstraint(
            "status IN ('running', 'success', 'failed', 'partial')",
            name="chk_collection_jobs_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(100), nullable=False)
    job_type: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    collected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    saved_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
