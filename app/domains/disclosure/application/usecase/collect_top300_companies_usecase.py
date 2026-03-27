import logging

from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.port.dart_corp_code_port import DartCorpCodePort
from app.domains.disclosure.application.port.krx_market_cap_port import KrxMarketCapPort
from app.domains.disclosure.application.response.company_response import CollectCompaniesResponse
from app.domains.disclosure.domain.entity.company import Company

logger = logging.getLogger(__name__)


class CollectTop300CompaniesUseCase:
    def __init__(
        self,
        company_repository: CompanyRepositoryPort,
        dart_corp_code_port: DartCorpCodePort,
        krx_market_cap_port: KrxMarketCapPort,
    ):
        self._company_repository = company_repository
        self._dart_corp_code_port = dart_corp_code_port
        self._krx_market_cap_port = krx_market_cap_port

    async def execute(self) -> CollectCompaniesResponse:
        # 1. DART에서 전체 기업 코드 수집
        corp_infos = await self._dart_corp_code_port.fetch_all_corp_codes()
        listed_corps = [c for c in corp_infos if c.stock_code]
        logger.info("전체 %d개 중 상장 기업 %d개 필터링", len(corp_infos), len(listed_corps))

        # stock_code → corp_code 매핑 테이블 생성
        stock_to_corp = {info.stock_code: info.corp_code for info in listed_corps}

        companies = [
            Company(
                corp_code=info.corp_code,
                corp_name=info.corp_name,
                stock_code=info.stock_code,
            )
            for info in listed_corps
        ]

        saved_count = await self._company_repository.save_bulk(companies)

        # 2. KRX 시가총액 상위 300개 조회
        market_cap_top300 = await self._krx_market_cap_port.fetch_top_by_market_cap(300)

        # 3. KRX stock_code → DART corp_code 변환하여 Top300 설정
        top300_corp_codes = []
        for info in market_cap_top300:
            corp_code = stock_to_corp.get(info.stock_code)
            if corp_code:
                top300_corp_codes.append(corp_code)

        updated_count = await self._company_repository.update_top300_flags(top300_corp_codes)

        logger.info(
            "기업 저장: %d건, KRX 시가총액 Top300 중 매칭: %d건",
            saved_count,
            updated_count,
        )

        return CollectCompaniesResponse(
            total_fetched=len(listed_corps),
            new_saved=saved_count,
            top300_updated=updated_count,
            message=f"상장 기업 {saved_count}건 저장, 시가총액 Top300 {updated_count}건 갱신 완료",
        )
