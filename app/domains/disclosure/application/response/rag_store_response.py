from typing import Optional

from pydantic import BaseModel


class RagStoreResponse(BaseModel):
    total_documents: int
    processed_documents: int
    total_chunks_stored: int
    skipped_documents: int
    message: str


class RagChunkSearchResult(BaseModel):
    chunk_id: Optional[int] = None
    rcept_no: str
    corp_code: str
    report_nm: str
    document_type: str
    section_title: Optional[str] = None
    chunk_index: int
    chunk_text: str


class RagSearchResponse(BaseModel):
    query: str
    result_count: int
    results: list[RagChunkSearchResult]
