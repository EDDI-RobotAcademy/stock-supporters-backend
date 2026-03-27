from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class DartDisclosureInfo:
    corp_code: str
    corp_name: str
    stock_code: str
    report_nm: str
    rcept_no: str
    flr_nm: str
    rcept_dt: str
    rm: str
    pblntf_ty: str
    pblntf_detail_ty: str


@dataclass
class DartDisclosureListResult:
    items: list[DartDisclosureInfo]
    total_count: int
    total_page: int
    current_page: int


class DartDisclosureApiPort(ABC):

    @abstractmethod
    async def fetch_disclosure_list(
        self,
        bgn_de: str,
        end_de: str,
        corp_code: Optional[str] = None,
        pblntf_ty: Optional[str] = None,
        page_no: int = 1,
        page_count: int = 100,
    ) -> DartDisclosureListResult:
        pass

    @abstractmethod
    async def fetch_all_pages(
        self,
        bgn_de: str,
        end_de: str,
        corp_code: Optional[str] = None,
        pblntf_ty: Optional[str] = None,
    ) -> list[DartDisclosureInfo]:
        pass
