import logging
from datetime import datetime, timedelta

from app.infrastructure.database.database import AsyncSessionLocal
from app.infrastructure.cache.redis_client import redis_client

logger = logging.getLogger(__name__)

BOOTSTRAP_TOP_N = 10
BOOTSTRAP_DISCLOSURE_DAYS = 90


async def job_bootstrap():
    """시스템 최초 기동 시: 시총 상위 10개 기업의 기본 데이터를 적재한다.

    이미 companies 테이블에 데이터가 있으면 건너뛴다.
    """
    from app.domains.disclosure.adapter.outbound.external.dart_corp_code_client import (
        DartCorpCodeClient,
    )
    from app.domains.disclosure.adapter.outbound.external.dart_disclosure_api_client import (
        DartDisclosureApiClient,
    )
    from app.domains.disclosure.adapter.outbound.external.krx_market_cap_client import (
        KrxMarketCapClient,
    )
    from app.domains.disclosure.adapter.outbound.persistence.company_repository_impl import (
        CompanyRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_repository_impl import (
        DisclosureRepositoryImpl,
    )
    from app.domains.disclosure.domain.entity.company import Company
    from app.domains.disclosure.domain.entity.disclosure import Disclosure
    from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

    import time as _time

    total_start = _time.monotonic()
    logger.info("=" * 60)
    logger.info("[부트스트랩] 초기 데이터 적재 시작 (시총 상위 %d개, 최근 %d일)", BOOTSTRAP_TOP_N, BOOTSTRAP_DISCLOSURE_DAYS)
    logger.info("=" * 60)

    async with AsyncSessionLocal() as db:
        company_repo = CompanyRepositoryImpl(db)

        # ── Step 1/4: 기존 데이터 확인 ──
        step_start = _time.monotonic()
        logger.info("[부트스트랩][1/4] 기존 데이터 확인 중...")
        existing = await company_repo.find_all_active()
        if existing:
            logger.info("[부트스트랩][1/4] 기업 데이터 %d건 존재 — 부트스트랩 스킵", len(existing))
            return
        logger.info("[부트스트랩][1/4] 기존 데이터 없음 — 초기 적재 진행 (%.1f초)", _time.monotonic() - step_start)

        # ── Step 2/4: DART 전체 기업 목록 수집 ──
        step_start = _time.monotonic()
        logger.info("[부트스트랩][2/4] DART 전체 상장기업 목록 수집 중... (ZIP 다운로드 → XML 파싱)")
        dart_client = DartCorpCodeClient()
        corp_infos = await dart_client.fetch_all_corp_codes()
        listed_corps = [c for c in corp_infos if c.stock_code]
        logger.info("[부트스트랩][2/4] DART 수집 완료: 전체 %d건, 상장 %d건 (%.1f초)",
                     len(corp_infos), len(listed_corps), _time.monotonic() - step_start)

        stock_to_corp = {info.stock_code: info.corp_code for info in listed_corps}

        companies = [
            Company(
                corp_code=info.corp_code,
                corp_name=info.corp_name,
                stock_code=info.stock_code,
            )
            for info in listed_corps
        ]

        step_start = _time.monotonic()
        logger.info("[부트스트랩][2/4] DB 저장 중... (%d건 upsert)", len(companies))
        saved_count = await company_repo.save_bulk(companies)
        logger.info("[부트스트랩][2/4] DB 저장 완료: %d건 (%.1f초)", saved_count, _time.monotonic() - step_start)

        # ── Step 3/4: KRX 시총 상위 N개 선정 ──
        step_start = _time.monotonic()
        logger.info("[부트스트랩][3/4] KRX 시총 상위 %d개 조회 중... (pykrx)", BOOTSTRAP_TOP_N)
        krx_client = KrxMarketCapClient()
        market_cap_top = await krx_client.fetch_top_by_market_cap(BOOTSTRAP_TOP_N)

        top_corp_codes = []
        top_names = []
        for info in market_cap_top:
            corp_code = stock_to_corp.get(info.stock_code)
            if corp_code:
                top_corp_codes.append(corp_code)
                top_names.append(f"{info.corp_name}({info.stock_code})")
        logger.info("[부트스트랩][3/4] 시총 상위 기업: %s (%.1f초)", ", ".join(top_names), _time.monotonic() - step_start)

        step_start = _time.monotonic()
        updated = await company_repo.update_top300_flags(top_corp_codes)
        logger.info("[부트스트랩][3/4] 수집 대상 플래그 설정 완료: %d건 (%.1f초)", updated, _time.monotonic() - step_start)

        # ── Step 4/4: 상위 기업 공시 수집 ──
        end_date = datetime.now().strftime("%Y%m%d")
        bgn_date = (datetime.now() - timedelta(days=BOOTSTRAP_DISCLOSURE_DAYS)).strftime("%Y%m%d")

        step_start = _time.monotonic()
        logger.info("[부트스트랩][4/4] DART 공시 수집 중... (기간: %s ~ %s, 유형: A/B/C/D/E 병렬)", bgn_date, end_date)
        dart_api = DartDisclosureApiClient()
        disclosure_repo = DisclosureRepositoryImpl(db)

        target_types = ["A", "B", "C", "D", "E"]
        type_labels = {"A": "정기보고서", "B": "주요사항", "C": "특정증권", "D": "합병/분할", "E": "지분공시"}

        import asyncio
        fetch_tasks = [
            dart_api.fetch_all_pages(bgn_de=bgn_date, end_de=end_date, pblntf_ty=pblntf_ty)
            for pblntf_ty in target_types
        ]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        all_items = []
        for i, result in enumerate(results):
            t = target_types[i]
            if isinstance(result, Exception):
                logger.error("[부트스트랩][4/4]   유형 %s(%s) — 실패: %s", t, type_labels[t], result)
                continue
            all_items.extend(result)
            logger.info("[부트스트랩][4/4]   유형 %s(%s) — %d건", t, type_labels[t], len(result))

        logger.info("[부트스트랩][4/4] DART 조회 완료: 전체 %d건 (%.1f초)", len(all_items), _time.monotonic() - step_start)

        top_codes_set = set(top_corp_codes)
        filtered = [item for item in all_items if item.corp_code in top_codes_set]
        logger.info("[부트스트랩][4/4] 상위 %d개 기업 필터링: %d건 → %d건", BOOTSTRAP_TOP_N, len(all_items), len(filtered))

        disclosures = [
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
            for item in filtered
        ]

        step_start = _time.monotonic()
        logger.info("[부트스트랩][4/4] 공시 DB 저장 중... (%d건 upsert)", len(disclosures))
        disc_saved = await disclosure_repo.upsert_bulk(disclosures)
        logger.info("[부트스트랩][4/4] 공시 저장 완료: %d건 (중복 제외 %d건) (%.1f초)",
                     disc_saved, len(disclosures) - disc_saved, _time.monotonic() - step_start)

        total_elapsed = _time.monotonic() - total_start
        logger.info("=" * 60)
        logger.info("[부트스트랩] 초기 적재 완료 — 총 %.1f초", total_elapsed)
        logger.info("[부트스트랩]   기업: %d건 저장", saved_count)
        logger.info("[부트스트랩]   시총 상위: %s", ", ".join(top_names))
        logger.info("[부트스트랩]   공시: %d건 저장 (전체 %d건 중 대상 %d건)", disc_saved, len(all_items), len(filtered))
        logger.info("=" * 60)


def _seasonal_date_range(months_back: int = 3) -> tuple[str, str]:
    """시즌 수집용 날짜 범위를 계산한다. 기본 최근 3개월."""
    end = datetime.now()
    bgn = end - timedelta(days=months_back * 30)
    return bgn.strftime("%Y%m%d"), end.strftime("%Y%m%d")


async def job_incremental_collect():
    """매시간: 마지막 수집 시점 이후 공시를 증분 수집한다."""
    from app.domains.disclosure.adapter.outbound.external.dart_disclosure_api_client import (
        DartDisclosureApiClient,
    )
    from app.domains.disclosure.adapter.outbound.persistence.collection_job_repository_impl import (
        CollectionJobRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.company_repository_impl import (
        CompanyRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_repository_impl import (
        DisclosureRepositoryImpl,
    )
    from app.domains.disclosure.application.usecase.incremental_collect_usecase import (
        IncrementalCollectUseCase,
    )

    logger.info("[스케줄러] 증분 공시 수집 시작")
    try:
        async with AsyncSessionLocal() as db:
            usecase = IncrementalCollectUseCase(
                dart_disclosure_api=DartDisclosureApiClient(),
                disclosure_repository=DisclosureRepositoryImpl(db),
                company_repository=CompanyRepositoryImpl(db),
                collection_job_repository=CollectionJobRepositoryImpl(db),
            )
            result = await usecase.execute()
            logger.info("[스케줄러] 증분 수집 완료: %s", result.message)
    except Exception as e:
        logger.error("[스케줄러] 증분 수집 실패: %s", str(e))


async def job_refresh_company_list():
    """매일 02:00: DART + KRX에서 기업 리스트를 갱신한다."""
    from app.domains.disclosure.adapter.outbound.external.dart_corp_code_client import (
        DartCorpCodeClient,
    )
    from app.domains.disclosure.adapter.outbound.external.krx_market_cap_client import (
        KrxMarketCapClient,
    )
    from app.domains.disclosure.adapter.outbound.persistence.company_repository_impl import (
        CompanyRepositoryImpl,
    )
    from app.domains.disclosure.application.usecase.refresh_company_list_usecase import (
        RefreshCompanyListUseCase,
    )

    logger.info("[스케줄러] 기업 리스트 갱신 시작")
    try:
        async with AsyncSessionLocal() as db:
            usecase = RefreshCompanyListUseCase(
                company_repository=CompanyRepositoryImpl(db),
                dart_corp_code_port=DartCorpCodeClient(),
                krx_market_cap_port=KrxMarketCapClient(),
            )
            result = await usecase.execute()
            logger.info("[스케줄러] 기업 리스트 갱신 완료: %s", result.message)
    except Exception as e:
        logger.error("[스케줄러] 기업 리스트 갱신 실패: %s", str(e))


async def job_batch_store_documents():
    """매일 02:30: 원문이 없는 공시의 문서를 배치 저장한다."""
    from app.domains.disclosure.adapter.outbound.external.dart_document_api_client import (
        DartDocumentApiClient,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_document_repository_impl import (
        DisclosureDocumentRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_repository_impl import (
        DisclosureRepositoryImpl,
    )
    from app.domains.disclosure.application.usecase.batch_store_documents_usecase import (
        BatchStoreDocumentsUseCase,
    )

    logger.info("[스케줄러] 공시 원문 배치 저장 시작")
    try:
        async with AsyncSessionLocal() as db:
            usecase = BatchStoreDocumentsUseCase(
                dart_document_api=DartDocumentApiClient(),
                disclosure_document_repository=DisclosureDocumentRepositoryImpl(db),
                disclosure_repository=DisclosureRepositoryImpl(db),
            )
            result = await usecase.execute()
            logger.info("[스케줄러] 원문 배치 저장 완료: %s", result.message)
    except Exception as e:
        logger.error("[스케줄러] 원문 배치 저장 실패: %s", str(e))


async def job_store_rag_chunks():
    """매일 02:45: RAG 미적재 문서를 청크 분할 후 벡터 적재한다."""
    from app.domains.disclosure.adapter.outbound.external.openai_embedding_client import (
        OpenAIEmbeddingClient,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_document_repository_impl import (
        DisclosureDocumentRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_repository_impl import (
        DisclosureRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.rag_chunk_repository_impl import (
        RagChunkRepositoryImpl,
    )
    from app.domains.disclosure.application.usecase.store_rag_chunks_usecase import (
        StoreRagChunksUseCase,
    )

    logger.info("[스케줄러] RAG 벡터 적재 시작")
    try:
        async with AsyncSessionLocal() as db:
            usecase = StoreRagChunksUseCase(
                disclosure_document_repository=DisclosureDocumentRepositoryImpl(db),
                disclosure_repository=DisclosureRepositoryImpl(db),
                rag_chunk_repository=RagChunkRepositoryImpl(db),
                embedding_port=OpenAIEmbeddingClient(),
            )
            result = await usecase.execute()
            logger.info("[스케줄러] RAG 적재 완료: %s", result.message)
    except Exception as e:
        logger.error("[스케줄러] RAG 적재 실패: %s", str(e))


async def job_cleanup_expired_data():
    """매일 03:00: 보관 기한이 만료된 데이터를 삭제한다."""
    from app.domains.disclosure.adapter.outbound.persistence.collection_job_repository_impl import (
        CollectionJobRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.data_cleanup_repository_impl import (
        DataCleanupRepositoryImpl,
    )
    from app.domains.disclosure.application.request.cleanup_request import CleanupRequest
    from app.domains.disclosure.application.usecase.cleanup_expired_data_usecase import (
        CleanupExpiredDataUseCase,
    )

    logger.info("[스케줄러] 만료 데이터 정리 시작")
    try:
        async with AsyncSessionLocal() as db:
            usecase = CleanupExpiredDataUseCase(
                data_cleanup_repository=DataCleanupRepositoryImpl(db),
                collection_job_repository=CollectionJobRepositoryImpl(db),
            )
            result = await usecase.execute(request=CleanupRequest())
            logger.info("[스케줄러] 데이터 정리 완료: %s", result.message)
    except Exception as e:
        logger.error("[스케줄러] 데이터 정리 실패: %s", str(e))


async def _run_seasonal_collect(pblntf_ty: str, report_name: str, months_back: int = 3):
    """시즌별 보고서 공시를 집중 수집한다."""
    from app.domains.disclosure.adapter.outbound.external.dart_disclosure_api_client import (
        DartDisclosureApiClient,
    )
    from app.domains.disclosure.adapter.outbound.persistence.collection_job_repository_impl import (
        CollectionJobRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.company_repository_impl import (
        CompanyRepositoryImpl,
    )
    from app.domains.disclosure.adapter.outbound.persistence.disclosure_repository_impl import (
        DisclosureRepositoryImpl,
    )
    from app.domains.disclosure.application.usecase.seasonal_collect_usecase import (
        SeasonalCollectUseCase,
    )

    bgn_de, end_de = _seasonal_date_range(months_back)
    logger.info("[스케줄러] %s 시즌 수집 시작 (%s ~ %s)", report_name, bgn_de, end_de)
    try:
        async with AsyncSessionLocal() as db:
            usecase = SeasonalCollectUseCase(
                dart_disclosure_api=DartDisclosureApiClient(),
                disclosure_repository=DisclosureRepositoryImpl(db),
                company_repository=CompanyRepositoryImpl(db),
                collection_job_repository=CollectionJobRepositoryImpl(db),
            )
            result = await usecase.execute(
                pblntf_ty=pblntf_ty,
                bgn_de=bgn_de,
                end_de=end_de,
            )
            logger.info("[스케줄러] %s 시즌 수집 완료: %s", report_name, result.message)
    except Exception as e:
        logger.error("[스케줄러] %s 시즌 수집 실패: %s", report_name, str(e))


async def job_seasonal_quarterly():
    """분기보고서(A003) 시즌 집중 수집."""
    await _run_seasonal_collect("A003", "분기보고서")


async def job_seasonal_semiannual():
    """반기보고서(A002) 시즌 집중 수집."""
    await _run_seasonal_collect("A002", "반기보고서")


async def job_seasonal_annual():
    """사업보고서(A001) 시즌 집중 수집."""
    await _run_seasonal_collect("A001", "사업보고서", months_back=4)
