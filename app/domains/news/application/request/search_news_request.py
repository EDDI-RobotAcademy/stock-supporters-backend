from pydantic import BaseModel, Field


class SearchNewsRequest(BaseModel):
    keyword: str = Field(..., min_length=1, description="검색 키워드")
    page: int = Field(1, ge=1, description="페이지 번호")
    page_size: int = Field(10, ge=1, le=100, description="페이지 크기")
