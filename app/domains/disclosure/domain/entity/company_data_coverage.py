from datetime import datetime
from typing import Optional


class CompanyDataCoverage:
    def __init__(
        self,
        corp_code: str,
        has_b001: bool = False,
        has_d002_d005: bool = False,
        has_d001: bool = False,
        has_e001: bool = False,
        has_c001: bool = False,
        has_a001: bool = False,
        has_a002: bool = False,
        has_a003: bool = False,
        has_event_documents: bool = False,
        last_collected_at: Optional[datetime] = None,
        last_on_demand_at: Optional[datetime] = None,
        coverage_id: Optional[int] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.coverage_id = coverage_id
        self.corp_code = corp_code
        self.has_b001 = has_b001
        self.has_d002_d005 = has_d002_d005
        self.has_d001 = has_d001
        self.has_e001 = has_e001
        self.has_c001 = has_c001
        self.has_a001 = has_a001
        self.has_a002 = has_a002
        self.has_a003 = has_a003
        self.has_event_documents = has_event_documents
        self.last_collected_at = last_collected_at
        self.last_on_demand_at = last_on_demand_at
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
