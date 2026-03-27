from datetime import datetime

from sqlalchemy import String, Integer, Text, DateTime, BigInteger, CheckConstraint, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.database import Base

try:
    from pgvector.sqlalchemy import Vector

    embedding_type = Vector(1536)
except ImportError:
    from sqlalchemy.dialects.postgresql import ARRAY
    from sqlalchemy import Float

    embedding_type = ARRAY(Float)


class RagDocumentChunkOrm(Base):
    __tablename__ = "rag_document_chunks"
    __table_args__ = (
        UniqueConstraint("rcept_no", "chunk_hash", name="uq_rag_chunks_rcept_chunk"),
        CheckConstraint(
            "document_type IN ('core_document', 'report_document', 'event_document')",
            name="chk_rag_document_chunks_doc_type",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rcept_no: Mapped[str] = mapped_column(
        String(20), ForeignKey("disclosures.rcept_no", ondelete="CASCADE"), nullable=False
    )
    corp_code: Mapped[str] = mapped_column(
        String(8), ForeignKey("companies.corp_code"), nullable=False
    )
    disclosure_document_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("disclosure_documents.id", ondelete="CASCADE"), nullable=True
    )
    report_nm: Mapped[str] = mapped_column(String(500), nullable=False)
    document_type: Mapped[str] = mapped_column(String(30), nullable=False)
    section_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    chunk_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    embedding = mapped_column(embedding_type, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
