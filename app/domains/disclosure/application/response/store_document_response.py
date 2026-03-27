from pydantic import BaseModel


class StoreDocumentResponse(BaseModel):
    rcept_no: str
    document_type: str
    stored: bool
    parsed: bool
    message: str


class BatchStoreDocumentResponse(BaseModel):
    total_target: int
    success_count: int
    fail_count: int
    message: str
