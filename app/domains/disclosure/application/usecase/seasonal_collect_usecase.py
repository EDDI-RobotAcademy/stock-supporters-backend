import logging
from datetime import datetime

from app.domains.disclosure.application.port.collection_job_repository_port import (
    CollectionJobRepositoryPort,
)
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.port.dart_disclosure_api_port import DartDisclosureApiPort
from app.domains.disclosure.application.port.disclosure_repository_port import (
    DisclosureRepositoryPort,
)
from app.domains.disclosure.application.response.disclosure_collection_response import (
    DisclosureCollectionResponse,
)
from app.domains.disclosure.domain.entity.collection_job import CollectionJob
from app.domains.disclosure.domain.entity.disclosure import Disclosure
from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

logger = logging.getLogger(__name__)

# DART pblntf_ty 코드
REPORT_TYPE_MAP = {
    "A001": "사업보고서",
    "A002": "반기보고서",
    "A003": "분기보고서",
}


class SeasonalCollectUseCase:
    def __init__(
        self,
        dart_disclosure_api: DartDisclosureApiPort,
        disclosure_repository: DisclosureRepositoryPort,
        company_repository: CompanyRepositoryPort,
        collection_job_repository: CollectionJobRepositoryPort,
    ):
        self._dart_api = dart_disclosure_api
        self._disclosure_repo = disclosure_repository
        self._company_repo = company_repository
        self._job_repo = collection_job_repository

    async def execute(
        self,
        pblntf_ty: str,
        bgn_de: str,
        end_de: str,
    ) -> DisclosureCollectionResponse:
        report_name = REPORT_TYPE_MAP.get(pblntf_ty, pblntf_ty)

        job = await self._job_repo.save_job(
            CollectionJob(
                job_name=f"seasonal_collect_{pblntf_ty}",
                job_type="seasonal",
                started_at=datetime.now(),
                status="running",
            )
        )

        try:
            # 1. DART에서 해당 유형 공시 전체 조회
            all_items = await self._dart_api.fetch_all_pages(
                bgn_de=bgn_de,
                end_de=end_de,
                pblntf_ty=pblntf_ty,
            )

            # 2. 수집 대상 기업 필터링 (Top300 + 최근 30일 내 요청된 기업)
            collect_targets = await self._company_repo.find_collect_targets(recent_days=30)
            target_codes = {c.corp_code for c in collect_targets}
            filtered = [item for item in all_items if item.corp_code in target_codes]

            # 3. 분류 및 저장
            disclosures: list[Disclosure] = []
            for item in filtered:
                disclosures.append(
                    Disclosure(
                        rcept_no=item.rcept_no,
                        corp_code=item.corp_code,
                        report_nm=item.report_nm,
                        rcept_dt=datetime.strptime(item.rcept_dt, "%Y%m%d").date(),
                        pblntf_ty=item.pblntf_ty,
                        pblntf_detail_ty=item.pblntf_detail_ty,
                        rm=item.rm,
                        disclosure_group=DisclosureClassifier.classify_group(item.report_nm),
                        source_mode="scheduled",
                        is_core=DisclosureClassifier.is_core_disclosure(item.report_nm),
                    )
                )

            saved_count = await self._disclosure_repo.upsert_bulk(disclosures)
            duplicate_skipped = len(disclosures) - saved_count

            # 4. 작업 기록
            job.finished_at = datetime.now()
            job.status = "success"
            job.collected_count = len(all_items)
            job.saved_count = saved_count
            job.message = (
                f"{report_name} 시즌 수집 ({bgn_de}~{end_de}): "
                f"전체 {len(all_items)}건, Top300 {len(filtered)}건, 저장 {saved_count}건"
            )
            await self._job_repo.update_job(job)

            logger.info(job.message)

            return DisclosureCollectionResponse(
                total_fetched=len(all_items),
                filtered_count=len(filtered),
                saved_count=saved_count,
                duplicate_skipped=duplicate_skipped,
                message=job.message,
            )

        except Exception as e:
            job.finished_at = datetime.now()
            job.status = "failed"
            job.message = str(e)
            await self._job_repo.update_job(job)
            raise
