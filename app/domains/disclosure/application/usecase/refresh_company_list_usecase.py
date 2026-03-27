import logging

from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.port.dart_corp_code_port import DartCorpCodePort
from app.domains.disclosure.application.port.krx_market_cap_port import KrxMarketCapPort
from app.domains.disclosure.application.response.company_response import CollectCompaniesResponse
from app.domains.disclosure.domain.entity.company import Company

logger = logging.getLogger(__name__)


class RefreshCompanyListUseCase:
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
        # 1. DART에서 최신 기업 목록 수집
        corp_infos = await self._dart_corp_code_port.fetch_all_corp_codes()
        listed_corps = [c for c in corp_infos if c.stock_code]

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

        # 2. KRX 시가총액 기준으로 Top300 재선정
        market_cap_top300 = await self._krx_market_cap_port.fetch_top_by_market_cap(300)

        top300_corp_codes = []
        for info in market_cap_top300:
            corp_code = stock_to_corp.get(info.stock_code)
            if corp_code:
                top300_corp_codes.append(corp_code)

        updated_count = await self._company_repository.update_top300_flags(top300_corp_codes)

        logger.info("기업 리스트 갱신 완료: %d건 upsert, Top300 %d건", saved_count, updated_count)

        return CollectCompaniesResponse(
            total_fetched=len(listed_corps),
            new_saved=saved_count,
            top300_updated=updated_count,
            message=f"기업 리스트 갱신 완료: {saved_count}건 upsert, 시가총액 Top300 {updated_count}건",
        )
