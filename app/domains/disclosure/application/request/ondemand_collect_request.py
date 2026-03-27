from typing import Optional

from pydantic import BaseModel


class OndemandCollectRequest(BaseModel):
    corp_code: str
    bgn_de: str
    end_de: str
    pblntf_ty: Optional[str] = None
