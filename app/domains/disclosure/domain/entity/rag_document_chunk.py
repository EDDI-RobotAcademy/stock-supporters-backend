from datetime import datetime
from typing import Optional


class RagDocumentChunk:
    def __init__(
        self,
        rcept_no: str,
        corp_code: str,
        report_nm: str,
        document_type: str,
        chunk_index: int,
        chunk_text: str,
        chunk_hash: str,
        embedding: Optional[list[float]] = None,
        section_title: Optional[str] = None,
        disclosure_document_id: Optional[int] = None,
        chunk_id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.chunk_id = chunk_id
        self.rcept_no = rcept_no
        self.corp_code = corp_code
        self.disclosure_document_id = disclosure_document_id
        self.report_nm = report_nm
        self.document_type = document_type
        self.section_title = section_title
        self.chunk_index = chunk_index
        self.chunk_text = chunk_text
        self.chunk_hash = chunk_hash
        self.embedding = embedding
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
