import logging

from app.common.exception.app_exception import AppException
from app.domains.disclosure.application.port.company_collection_target_repository_port import (
    CompanyCollectionTargetRepositoryPort,
)
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.request.add_company_request import AddCompanyRequest
from app.domains.disclosure.application.response.company_response import AddCompanyResponse
from app.domains.disclosure.domain.entity.company_collection_target import CompanyCollectionTarget

logger = logging.getLogger(__name__)


class AddCompanyManuallyUseCase:
    def __init__(
        self,
        company_repository: CompanyRepositoryPort,
        collection_target_repository: CompanyCollectionTargetRepositoryPort,
    ):
        self._company_repository = company_repository
        self._collection_target_repository = collection_target_repository

    async def execute(self, request: AddCompanyRequest) -> AddCompanyResponse:
        company = await self._company_repository.find_by_corp_code(request.corp_code)
        if company is None:
            raise AppException(
                status_code=404,
                message=f"기업 코드 '{request.corp_code}'에 해당하는 기업이 존재하지 않습니다.",
            )

        existing_target = await self._collection_target_repository.find_by_corp_code(
            request.corp_code
        )
        if existing_target is not None and existing_target.is_enabled:
            raise AppException(
                status_code=409,
                message=f"기업 '{company.corp_name}'은 이미 수집 대상입니다.",
            )

        target = CompanyCollectionTarget(
            corp_code=request.corp_code,
            target_type="ondemand",
            is_enabled=True,
            added_reason=request.added_reason,
        )

        await self._collection_target_repository.save(target)

        logger.info(
            "수집 대상 수동 추가: %s (%s), 사유: %s",
            company.corp_name,
            request.corp_code,
            request.added_reason,
        )

        return AddCompanyResponse(
            corp_code=company.corp_code,
            corp_name=company.corp_name,
            target_type="ondemand",
            message=f"기업 '{company.corp_name}'을 수집 대상에 추가했습니다.",
        )
