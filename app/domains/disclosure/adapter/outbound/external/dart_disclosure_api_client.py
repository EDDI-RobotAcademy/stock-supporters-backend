import logging
from typing import Optional

import httpx

from app.domains.disclosure.application.port.dart_disclosure_api_port import (
    DartDisclosureApiPort,
    DartDisclosureInfo,
    DartDisclosureListResult,
)
from app.infrastructure.config.settings import get_settings

logger = logging.getLogger(__name__)

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"


class DartDisclosureApiClient(DartDisclosureApiPort):

    async def fetch_disclosure_list(
        self,
        bgn_de: str,
        end_de: str,
        corp_code: Optional[str] = None,
        pblntf_ty: Optional[str] = None,
        page_no: int = 1,
        page_count: int = 100,
    ) -> DartDisclosureListResult:
        settings = get_settings()

        params = {
            "crtfc_key": settings.open_dart_api_key,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": page_count,
        }

        if corp_code:
            params["corp_code"] = corp_code
        if pblntf_ty:
            params["pblntf_ty"] = pblntf_ty

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(DART_LIST_URL, params=params)
            response.raise_for_status()

        data = response.json()

        if data.get("status") == "013":
            # 013: 조회된 데이터가 없습니다
            return DartDisclosureListResult(
                items=[], total_count=0, total_page=0, current_page=page_no
            )

        if data.get("status") != "000":
            logger.error("DART API 오류: %s - %s", data.get("status"), data.get("message"))
            raise RuntimeError(f"DART API 오류: {data.get('message')}")

        items = [
            DartDisclosureInfo(
                corp_code=item.get("corp_code", ""),
                corp_name=item.get("corp_name", ""),
                stock_code=item.get("stock_code", ""),
                report_nm=item.get("report_nm", ""),
                rcept_no=item.get("rcept_no", ""),
                flr_nm=item.get("flr_nm", ""),
                rcept_dt=item.get("rcept_dt", ""),
                rm=item.get("rm", ""),
                pblntf_ty=item.get("pblntf_ty", ""),
                pblntf_detail_ty=item.get("pblntf_detail_ty", ""),
            )
            for item in data.get("list", [])
        ]

        return DartDisclosureListResult(
            items=items,
            total_count=int(data.get("total_count", 0)),
            total_page=int(data.get("total_page", 0)),
            current_page=page_no,
        )

    async def fetch_all_pages(
        self,
        bgn_de: str,
        end_de: str,
        corp_code: Optional[str] = None,
        pblntf_ty: Optional[str] = None,
    ) -> list[DartDisclosureInfo]:
        all_items: list[DartDisclosureInfo] = []
        page_no = 1

        while True:
            result = await self.fetch_disclosure_list(
                bgn_de=bgn_de,
                end_de=end_de,
                corp_code=corp_code,
                pblntf_ty=pblntf_ty,
                page_no=page_no,
                page_count=100,
            )

            all_items.extend(result.items)

            if page_no >= result.total_page or not result.items:
                break

            page_no += 1

        logger.info(
            "DART 공시 전체 조회 완료: %d건 (기간: %s ~ %s)",
            len(all_items),
            bgn_de,
            end_de,
        )
        return all_items
