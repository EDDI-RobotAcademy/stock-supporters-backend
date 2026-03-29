from datetime import datetime
from typing import Optional


class Company:
    def __init__(
        self,
        corp_code: str,
        corp_name: str,
        stock_code: Optional[str] = None,
        market_type: Optional[str] = None,
        market_cap_rank: Optional[int] = None,
        is_top300: bool = False,
        is_collect_target: bool = False,
        is_active: bool = True,
        last_requested_at: Optional[datetime] = None,
        company_id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.company_id = company_id
        self.corp_code = corp_code
        self.corp_name = corp_name
        self.stock_code = stock_code
        self.market_type = market_type
        self.market_cap_rank = market_cap_rank
        self.is_top300 = is_top300
        self.is_collect_target = is_collect_target
        self.is_active = is_active
        self.last_requested_at = last_requested_at
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
