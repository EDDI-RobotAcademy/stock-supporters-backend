from datetime import datetime
from typing import Optional


class CollectionJob:
    def __init__(
        self,
        job_name: str,
        job_type: str,
        started_at: datetime,
        status: str,
        collected_count: int = 0,
        saved_count: int = 0,
        finished_at: Optional[datetime] = None,
        message: Optional[str] = None,
        job_id: Optional[int] = None,
        created_at: Optional[datetime] = None,
    ):
        self.job_id = job_id
        self.job_name = job_name
        self.job_type = job_type
        self.started_at = started_at
        self.finished_at = finished_at
        self.status = status
        self.collected_count = collected_count
        self.saved_count = saved_count
        self.message = message
        self.created_at = created_at or datetime.now()
