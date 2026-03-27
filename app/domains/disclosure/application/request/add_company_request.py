from typing import Optional

from pydantic import BaseModel


class AddCompanyRequest(BaseModel):
    corp_code: str
    added_reason: Optional[str] = None
