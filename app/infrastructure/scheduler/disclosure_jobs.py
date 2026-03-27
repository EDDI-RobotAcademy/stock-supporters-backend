import logging
from datetime import datetime, timedelta

from app.infrastructure.database.database import AsyncSessionLocal
from app.infrastructure.cache.redis_client import redis_client

logger = logging.getLogger(__name__)


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
