import logging

from app.domains.disclosure.adapter.outbound.cache.analysis_cache_adapter import AnalysisCacheAdapter
from app.domains.disclosure.adapter.outbound.external.openai_analysis_client import OpenAIAnalysisClient
from app.domains.disclosure.adapter.outbound.external.openai_embedding_client import OpenAIEmbeddingClient
from app.domains.disclosure.adapter.outbound.persistence.company_repository_impl import CompanyRepositoryImpl
from app.domains.disclosure.adapter.outbound.persistence.disclosure_repository_impl import DisclosureRepositoryImpl
from app.domains.disclosure.adapter.outbound.persistence.rag_chunk_repository_impl import RagChunkRepositoryImpl
from app.domains.disclosure.application.response.analysis_response import AnalysisResponse
from app.domains.disclosure.application.usecase.analyze_company_usecase import AnalyzeCompanyUseCase
from app.infrastructure.cache.redis_client import redis_client
from app.infrastructure.database.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


class DisclosureAnalysisService:
    """공시 분석 에이전트 서비스 파사드.

    메인 에이전트가 ticker(종목코드)로 호출하는 진입점.
    내부에서 ticker → corp_code 변환, DB/Redis 커넥션을 관리한다.

    Usage:
        service = DisclosureAnalysisService()
        result = await service.analyze(ticker="005930")
    """

    async def analyze(
        self,
        ticker: str,
        analysis_type: str = "full_analysis",
    ) -> AnalysisResponse:
        async with AsyncSessionLocal() as db:
            company_repo = CompanyRepositoryImpl(db)

            # ticker → corp_code 변환
            company = await company_repo.find_by_stock_code(ticker)
            if company is None:
                return AnalysisResponse(
                    status="error",
                    data={"ticker": ticker, "filings": []},
                    error_message=f"종목코드 '{ticker}'에 해당하는 기업을 찾을 수 없습니다.",
                )

            usecase = AnalyzeCompanyUseCase(
                analysis_cache_port=AnalysisCacheAdapter(redis_client),
                disclosure_repository_port=DisclosureRepositoryImpl(db),
                rag_chunk_repository_port=RagChunkRepositoryImpl(db),
                embedding_port=OpenAIEmbeddingClient(),
                llm_analysis_port=OpenAIAnalysisClient(),
                company_repository_port=company_repo,
            )

            return await usecase.execute(
                corp_code=company.corp_code,
                ticker=ticker,
                analysis_type=analysis_type,
            )
