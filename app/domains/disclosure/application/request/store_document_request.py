from pydantic import BaseModel, field_validator


class StoreDocumentRequest(BaseModel):
    rcept_no: str
    document_type: str

    @field_validator("document_type")
    @classmethod
    def validate_document_type(cls, v: str) -> str:
        allowed = {"core_document", "report_document", "event_document"}
        if v not in allowed:
            raise ValueError(
                f"document_type은 {allowed} 중 하나여야 합니다. 입력값: {v}"
            )
        return v


class BatchStoreDocumentRequest(BaseModel):
    document_type: str = "core_document"
    limit: int = 50

    @field_validator("document_type")
    @classmethod
    def validate_document_type(cls, v: str) -> str:
        allowed = {"core_document", "report_document", "event_document"}
        if v not in allowed:
            raise ValueError(
                f"document_type은 {allowed} 중 하나여야 합니다. 입력값: {v}"
            )
        return v

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v < 1 or v > 500:
            raise ValueError("limit은 1 이상 500 이하여야 합니다.")
        return v
