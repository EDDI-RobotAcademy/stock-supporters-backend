import asyncio
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
from app.domains.disclosure.application.request.collect_disclosures_request import (
    CollectDisclosuresRequest,
)
from app.domains.disclosure.application.response.disclosure_collection_response import (
    DisclosureCollectionResponse,
)
from app.domains.disclosure.domain.entity.collection_job import CollectionJob
from app.domains.disclosure.domain.entity.disclosure import Disclosure
from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

logger = logging.getLogger(__name__)

# 수집 대상 공시 유형 전체
ALL_TARGET_PBLNTF_TYPES = [
    "A",   # A001~A003 - 사업/반기/분기보고서
    "B",   # B001 - 주요사항보고서
    "C",   # C001 - 임원·주요주주 특정증권
    "D",   # D001~D005 - 합병/분할/각종 공시
    "E",   # E001 - 지분 공시
]


class CollectAllDisclosuresUseCase:
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

    async def execute(self, request: CollectDisclosuresRequest) -> DisclosureCollectionResponse:
        job = await self._job_repo.save_job(
            CollectionJob(
                job_name="collect_all_disclosures",
                job_type="daily",
                started_at=datetime.now(),
                status="running",
            )
        )

        try:
            # 1. 수집 대상 기업 코드 준비 (Top300 + 최근 30일 내 요청된 기업)
            collect_targets = await self._company_repo.find_collect_targets(recent_days=30)
            target_codes = {c.corp_code for c in collect_targets}

            # 2. 수집 대상 유형별로 DART 병렬 조회
            target_types = [request.pblntf_ty] if request.pblntf_ty else ALL_TARGET_PBLNTF_TYPES

            fetch_tasks = [
                self._dart_api.fetch_all_pages(
                    bgn_de=request.bgn_de,
                    end_de=request.end_de,
                    pblntf_ty=pblntf_ty,
                )
                for pblntf_ty in target_types
            ]
            results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            all_items = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error("DART 조회 실패 (유형=%s): %s", target_types[i], result)
                    continue
                all_items.extend(result)

            # 3. 수집 대상 기업 필터링
            filtered = [item for item in all_items if item.corp_code in target_codes]

            # 4. 분류 및 변환
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

            # 5. upsert
            saved_count = await self._disclosure_repo.upsert_bulk(disclosures)
            duplicate_skipped = len(disclosures) - saved_count

            # 6. 작업 기록
            job.finished_at = datetime.now()
            job.status = "success"
            job.collected_count = len(all_items)
            job.saved_count = saved_count
            job.message = (
                f"대상유형 {target_types}: "
                f"조회 {len(all_items)}건, 수집대상 {len(filtered)}건, 저장 {saved_count}건"
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
