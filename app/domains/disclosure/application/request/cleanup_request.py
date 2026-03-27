from pydantic import BaseModel, Field


class CleanupRequest(BaseModel):
    disclosure_retention_days: int = Field(default=365, ge=1, description="공시 데이터 보관 기간 (일)")
    job_retention_days: int = Field(default=90, ge=1, description="수집 작업 로그 보관 기간 (일)")
