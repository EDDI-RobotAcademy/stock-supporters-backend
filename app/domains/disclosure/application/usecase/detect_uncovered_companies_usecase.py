import logging

from app.domains.disclosure.application.port.company_data_coverage_repository_port import (
    CompanyDataCoverageRepositoryPort,
)
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.response.uncovered_companies_response import (
    UncoveredCompaniesResponse,
    UncoveredCompanyItem,
)

logger = logging.getLogger(__name__)


class DetectUncoveredCompaniesUseCase:
    def __init__(
        self,
        company_repository: CompanyRepositoryPort,
        coverage_repository: CompanyDataCoverageRepositoryPort,
    ):
        self._company_repo = company_repository
        self._coverage_repo = coverage_repository

    async def execute(self) -> UncoveredCompaniesResponse:
        uncovered_corp_codes = await self._coverage_repo.find_uncovered_companies()

        items: list[UncoveredCompanyItem] = []
        for corp_code in uncovered_corp_codes:
            company = await self._company_repo.find_by_corp_code(corp_code)
            if company is None:
                continue
            items.append(
                UncoveredCompanyItem(
                    corp_code=company.corp_code,
                    corp_name=company.corp_name,
                    stock_code=company.stock_code,
                    market_cap_rank=company.market_cap_rank,
                    is_top300=company.is_top300,
                )
            )

        logger.info(f"미수집 기업 {len(items)}건 탐지 완료")

        return UncoveredCompaniesResponse(
            companies=items,
            total_count=len(items),
        )
