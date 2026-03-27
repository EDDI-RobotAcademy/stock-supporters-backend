from pydantic import BaseModel


class DataStatsResponse(BaseModel):
    disclosure_count: int
    document_count: int
    rag_chunk_count: int
    company_count: int
    job_count: int
