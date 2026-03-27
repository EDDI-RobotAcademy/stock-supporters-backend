from datetime import datetime
from typing import Optional


class CompanyCollectionTarget:
    def __init__(
        self,
        corp_code: str,
        target_type: str,
        is_enabled: bool = True,
        added_reason: Optional[str] = None,
        target_id: Optional[int] = None,
        added_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.target_id = target_id
        self.corp_code = corp_code
        self.target_type = target_type
        self.is_enabled = is_enabled
        self.added_reason = added_reason
        self.added_at = added_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
