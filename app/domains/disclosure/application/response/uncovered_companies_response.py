from typing import Optional

from pydantic import BaseModel


class UncoveredCompanyItem(BaseModel):
    corp_code: str
    corp_name: str
    stock_code: Optional[str]
    market_cap_rank: Optional[int]
    is_top300: bool


class UncoveredCompaniesResponse(BaseModel):
    companies: list[UncoveredCompanyItem]
    total_count: int
