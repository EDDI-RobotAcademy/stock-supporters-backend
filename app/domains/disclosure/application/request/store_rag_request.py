from typing import Optional

from pydantic import BaseModel, field_validator


class StoreRagRequest(BaseModel):
    limit: int = 50

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v < 1 or v > 500:
            raise ValueError("limit은 1 이상 500 이하여야 합니다.")
        return v


class SearchRagRequest(BaseModel):
    query: str
    corp_code: Optional[str] = None
    limit: int = 10

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v < 1 or v > 100:
            raise ValueError("limit은 1 이상 100 이하여야 합니다.")
        return v
