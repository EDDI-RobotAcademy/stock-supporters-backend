from datetime import datetime

from sqlalchemy import String, Text, DateTime, BigInteger, CheckConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class CollectionJobItemOrm(Base):
    __tablename__ = "collection_job_items"
    __table_args__ = (
        CheckConstraint(
            "status IN ('collected', 'saved', 'skipped', 'failed')",
            name="chk_collection_job_items_status",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("collection_jobs.id", ondelete="CASCADE"), nullable=False
    )
    rcept_no: Mapped[str | None] = mapped_column(String(20), nullable=True)
    corp_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
