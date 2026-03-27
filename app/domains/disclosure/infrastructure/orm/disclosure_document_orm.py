from datetime import datetime

from sqlalchemy import String, Text, Boolean, DateTime, CheckConstraint, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base


class DisclosureDocumentOrm(Base):
    __tablename__ = "disclosure_documents"
    __table_args__ = (
        UniqueConstraint("rcept_no", "document_type", name="uq_disclosure_documents_rcept_no_document_type"),
        CheckConstraint(
            "document_type IN ('core_document', 'report_document', 'event_document')",
            name="chk_disclosure_documents_type",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rcept_no: Mapped[str] = mapped_column(
        String(20), ForeignKey("disclosures.rcept_no", ondelete="CASCADE"), nullable=False
    )
    document_type: Mapped[str] = mapped_column(String(30), nullable=False)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    parsed_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    summary_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    stored_in_rag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
