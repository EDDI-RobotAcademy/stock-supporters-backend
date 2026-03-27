import logging
from datetime import datetime

from app.common.exception.app_exception import AppException
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.port.dart_disclosure_api_port import DartDisclosureApiPort
from app.domains.disclosure.application.port.disclosure_repository_port import (
    DisclosureRepositoryPort,
)
from app.domains.disclosure.application.request.collect_disclosures_request import (
    CollectCompanyDisclosuresRequest,
)
from app.domains.disclosure.application.response.disclosure_collection_response import (
    DisclosureCollectionResponse,
)
from app.domains.disclosure.domain.entity.disclosure import Disclosure
from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

logger = logging.getLogger(__name__)


class CollectCompanyDisclosuresUseCase:
    def __init__(
        self,
        dart_disclosure_api: DartDisclosureApiPort,
        disclosure_repository: DisclosureRepositoryPort,
        company_repository: CompanyRepositoryPort,
    ):
        self._dart_api = dart_disclosure_api
        self._disclosure_repo = disclosure_repository
        self._company_repo = company_repository

    async def execute(
        self, request: CollectCompanyDisclosuresRequest
    ) -> DisclosureCollectionResponse:
        company = await self._company_repo.find_by_corp_code(request.corp_code)
        if company is None:
            raise AppException(
                status_code=404,
                message=f"기업 코드 '{request.corp_code}'에 해당하는 기업이 존재하지 않습니다.",
            )

        all_items = await self._dart_api.fetch_all_pages(
            bgn_de=request.bgn_de,
            end_de=request.end_de,
            corp_code=request.corp_code,
            pblntf_ty=request.pblntf_ty,
        )

        classifier = DisclosureClassifier()
        disclosures: list[Disclosure] = []
        for item in all_items:
            disclosures.append(
                Disclosure(
                    rcept_no=item.rcept_no,
                    corp_code=item.corp_code,
                    report_nm=item.report_nm,
                    rcept_dt=datetime.strptime(item.rcept_dt, "%Y%m%d").date(),
                    pblntf_ty=item.pblntf_ty,
                    pblntf_detail_ty=item.pblntf_detail_ty,
                    rm=item.rm,
                    disclosure_group=classifier.classify_group(item.report_nm),
                    source_mode="ondemand",
                    is_core=classifier.is_core_disclosure(item.report_nm),
                )
            )

        saved_count = await self._disclosure_repo.upsert_bulk(disclosures)
        duplicate_skipped = len(disclosures) - saved_count

        message = (
            f"기업 '{company.corp_name}' 공시 {len(all_items)}건 조회, {saved_count}건 저장"
        )
        logger.info(message)

        return DisclosureCollectionResponse(
            total_fetched=len(all_items),
            filtered_count=len(all_items),
            saved_count=saved_count,
            duplicate_skipped=duplicate_skipped,
            message=message,
        )
