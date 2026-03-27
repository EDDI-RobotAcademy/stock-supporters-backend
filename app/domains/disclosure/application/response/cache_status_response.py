from typing import Optional

from pydantic import BaseModel


class CacheStatusResponse(BaseModel):
    corp_code: str
    analysis_type: str
    is_cached: bool
    ttl_remaining: Optional[int] = None
