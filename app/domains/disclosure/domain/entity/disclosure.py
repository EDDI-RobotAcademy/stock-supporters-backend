from datetime import date, datetime
from typing import Optional


class Disclosure:
    def __init__(
        self,
        rcept_no: str,
        corp_code: str,
        report_nm: str,
        rcept_dt: date,
        pblntf_ty: Optional[str] = None,
        pblntf_detail_ty: Optional[str] = None,
        rm: Optional[str] = None,
        disclosure_group: Optional[str] = None,
        source_mode: str = "scheduled",
        is_core: bool = False,
        disclosure_id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.disclosure_id = disclosure_id
        self.rcept_no = rcept_no
        self.corp_code = corp_code
        self.report_nm = report_nm
        self.rcept_dt = rcept_dt
        self.pblntf_ty = pblntf_ty
        self.pblntf_detail_ty = pblntf_detail_ty
        self.rm = rm
        self.disclosure_group = disclosure_group
        self.source_mode = source_mode
        self.is_core = is_core
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
