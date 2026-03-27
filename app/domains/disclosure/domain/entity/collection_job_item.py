from datetime import datetime
from typing import Optional


class CollectionJobItem:
    def __init__(
        self,
        job_id: int,
        status: str,
        rcept_no: Optional[str] = None,
        corp_code: Optional[str] = None,
        message: Optional[str] = None,
        item_id: Optional[int] = None,
        created_at: Optional[datetime] = None,
    ):
        self.item_id = item_id
        self.job_id = job_id
        self.rcept_no = rcept_no
        self.corp_code = corp_code
        self.status = status
        self.message = message
        self.created_at = created_at or datetime.now()
