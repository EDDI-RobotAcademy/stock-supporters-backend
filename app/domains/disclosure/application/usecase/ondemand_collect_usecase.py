import logging
from datetime import datetime

from app.common.exception.app_exception import AppException
from app.domains.disclosure.application.port.company_data_coverage_repository_port import (
    CompanyDataCoverageRepositoryPort,
)
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.port.dart_disclosure_api_port import DartDisclosureApiPort
from app.domains.disclosure.application.port.disclosure_repository_port import (
    DisclosureRepositoryPort,
)
from app.domains.disclosure.application.request.ondemand_collect_request import (
    OndemandCollectRequest,
)
from app.domains.disclosure.application.response.ondemand_collect_response import (
    OndemandCollectResponse,
)
from app.domains.disclosure.domain.entity.company_data_coverage import CompanyDataCoverage
from app.domains.disclosure.domain.entity.disclosure import Disclosure
from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

logger = logging.getLogger(__name__)


class OndemandCollectUseCase:
    def __init__(
        self,
        dart_disclosure_api: DartDisclosureApiPort,
        disclosure_repository: DisclosureRepositoryPort,
        company_repository: CompanyRepositoryPort,
        coverage_repository: CompanyDataCoverageRepositoryPort,
    ):
        self._dart_api = dart_disclosure_api
        self._disclosure_repo = disclosure_repository
        self._company_repo = company_repository
        self._coverage_repo = coverage_repository

    async def execute(self, request: OndemandCollectRequest) -> OndemandCollectResponse:
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

        coverage_updated = await self._update_coverage(
            request.corp_code, disclosures
        )

        message = (
            f"기업 '{company.corp_name}' 온디맨드 수집: "
            f"{len(all_items)}건 조회, {saved_count}건 저장, {duplicate_skipped}건 중복"
        )
        logger.info(message)

        return OndemandCollectResponse(
            corp_code=request.corp_code,
            corp_name=company.corp_name,
            total_fetched=len(all_items),
            saved_count=saved_count,
            duplicate_skipped=duplicate_skipped,
            coverage_updated=coverage_updated,
            message=message,
        )

    async def _update_coverage(
        self, corp_code: str, disclosures: list[Disclosure]
    ) -> bool:
        if not disclosures:
            return False

        pblntf_types = {d.pblntf_ty for d in disclosures if d.pblntf_ty}
        has_event = any(d.disclosure_group == "event" for d in disclosures)

        existing = await self._coverage_repo.find_by_corp_code(corp_code)
        now = datetime.now()

        if existing:
            coverage = CompanyDataCoverage(
                coverage_id=existing.coverage_id,
                corp_code=corp_code,
                has_b001=existing.has_b001 or ("B" in pblntf_types),
                has_d002_d005=existing.has_d002_d005 or bool(pblntf_types & {"D002", "D003", "D004", "D005"}),
                has_d001=existing.has_d001 or ("D001" in pblntf_types),
                has_e001=existing.has_e001 or ("E" in pblntf_types),
                has_c001=existing.has_c001 or ("C" in pblntf_types),
                has_a001=existing.has_a001 or ("A001" in pblntf_types),
                has_a002=existing.has_a002 or ("A002" in pblntf_types),
                has_a003=existing.has_a003 or ("A003" in pblntf_types),
                has_event_documents=existing.has_event_documents or has_event,
                last_collected_at=existing.last_collected_at,
                last_on_demand_at=now,
                created_at=existing.created_at,
                updated_at=now,
            )
        else:
            coverage = CompanyDataCoverage(
                corp_code=corp_code,
                has_b001="B" in pblntf_types,
                has_d002_d005=bool(pblntf_types & {"D002", "D003", "D004", "D005"}),
                has_d001="D001" in pblntf_types,
                has_e001="E" in pblntf_types,
                has_c001="C" in pblntf_types,
                has_a001="A001" in pblntf_types,
                has_a002="A002" in pblntf_types,
                has_a003="A003" in pblntf_types,
                has_event_documents=has_event,
                last_on_demand_at=now,
            )

        await self._coverage_repo.upsert(coverage)
        return True
