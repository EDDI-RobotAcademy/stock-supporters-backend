import json
import logging
import time

from app.domains.disclosure.application.port.analysis_cache_port import AnalysisCachePort
from app.domains.disclosure.application.port.disclosure_repository_port import DisclosureRepositoryPort
from app.domains.disclosure.application.port.embedding_port import EmbeddingPort
from app.domains.disclosure.application.port.llm_analysis_port import LlmAnalysisPort
from app.domains.disclosure.application.port.rag_chunk_repository_port import RagChunkRepositoryPort
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.response.analysis_response import AnalysisResponse
from app.domains.disclosure.domain.service.analysis_prompt_builder import AnalysisPromptBuilder
from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

logger = logging.getLogger(__name__)

VALID_ANALYSIS_TYPES = {"flow_analysis", "signal_analysis", "full_analysis"}
DEFAULT_CACHE_TTL = 3600
RAG_SEARCH_LIMIT = 5


class AnalyzeCompanyUseCase:

    def __init__(
        self,
        analysis_cache_port: AnalysisCachePort,
        disclosure_repository_port: DisclosureRepositoryPort,
        rag_chunk_repository_port: RagChunkRepositoryPort,
        embedding_port: EmbeddingPort,
        llm_analysis_port: LlmAnalysisPort,
        company_repository_port: CompanyRepositoryPort,
    ):
        self._cache = analysis_cache_port
        self._disclosure_repo = disclosure_repository_port
        self._rag_repo = rag_chunk_repository_port
        self._embedding = embedding_port
        self._llm = llm_analysis_port
        self._company_repo = company_repository_port

    async def execute(self, corp_code: str, ticker: str, analysis_type: str = "full_analysis") -> AnalysisResponse:
        start_time = time.monotonic()

        if analysis_type not in VALID_ANALYSIS_TYPES:
            return self._error_response(
                ticker, 0, f"유효하지 않은 분석 유형입니다: {analysis_type}"
            )

        try:
            return await self._run_analysis(corp_code, ticker, analysis_type, start_time)
        except Exception as e:
            elapsed = int((time.monotonic() - start_time) * 1000)
            logger.error("공시 분석 실패: corp_code=%s, error=%s", corp_code, str(e))
            return self._error_response(ticker, elapsed, str(e))

    async def _run_analysis(
        self, corp_code: str, ticker: str, analysis_type: str, start_time: float
    ) -> AnalysisResponse:
        # 1. 캐시 우선 조회
        cached_result = await self._cache.get(corp_code, analysis_type)
        if cached_result is not None:
            elapsed = int((time.monotonic() - start_time) * 1000)
            logger.info("캐시 적중: corp_code=%s, type=%s", corp_code, analysis_type)
            return AnalysisResponse(
                data={"ticker": ticker, "filings": cached_result.get("filings", [])},
                execution_time_ms=elapsed,
                signal=cached_result.get("signal"),
                confidence=cached_result.get("confidence"),
                summary=cached_result.get("summary"),
                key_points=cached_result.get("key_points", []),
            )

        # 2. 공시 목록 조회
        disclosures = await self._disclosure_repo.find_by_corp_code(corp_code, limit=50)
        if not disclosures:
            elapsed = int((time.monotonic() - start_time) * 1000)
            return AnalysisResponse(
                data={"ticker": ticker, "filings": []},
                execution_time_ms=elapsed,
                summary="분석할 공시 데이터가 없습니다.",
            )

        # 3. 공시 분류
        event_disclosures = [
            d for d in disclosures
            if DisclosureClassifier.classify_group(d.report_nm) == "event"
        ]

        # 4. RAG 검색
        analysis_query = self._build_analysis_query(corp_code, disclosures, event_disclosures)
        rag_contexts = await self._search_rag_contexts(analysis_query, corp_code)

        # 5. 프롬프트 생성 및 LLM 호출
        analysis_disclosures = event_disclosures if (analysis_type == "signal_analysis" and event_disclosures) else disclosures
        prompt, system_message = self._build_prompt(analysis_type, analysis_disclosures, rag_contexts)
        llm_result = await self._call_llm_analysis(prompt, system_message)

        # 6. 공시 목록 구조화
        filings = [
            {
                "title": d.report_nm,
                "filed_at": d.rcept_dt.isoformat(),
                "type": DisclosureClassifier.classify_group(d.report_nm),
            }
            for d in disclosures[:10]
        ]

        # 7. 캐시 저장용 데이터 구성
        cache_data = {
            "filings": filings,
            "signal": llm_result.get("signal"),
            "confidence": llm_result.get("confidence"),
            "summary": llm_result.get("summary"),
            "key_points": llm_result.get("key_points", []),
        }
        await self._cache.save(corp_code, analysis_type, cache_data, DEFAULT_CACHE_TTL)

        elapsed = int((time.monotonic() - start_time) * 1000)
        logger.info("분석 완료: corp_code=%s, %dms", corp_code, elapsed)

        return AnalysisResponse(
            data={"ticker": ticker, "filings": filings},
            execution_time_ms=elapsed,
            signal=llm_result.get("signal"),
            confidence=llm_result.get("confidence"),
            summary=llm_result.get("summary"),
            key_points=llm_result.get("key_points", []),
        )

    def _build_analysis_query(self, corp_code: str, disclosures: list, event_disclosures: list) -> str:
        parts = [f"기업코드 {corp_code} 공시 분석"]
        if event_disclosures:
            parts.append(" ".join(d.report_nm for d in event_disclosures[:5]))
        elif disclosures:
            parts.append(" ".join(d.report_nm for d in disclosures[:3]))
        return " ".join(parts)

    async def _search_rag_contexts(self, query: str, corp_code: str) -> list:
        try:
            query_embedding = await self._embedding.generate(query)
            rag_chunks = await self._rag_repo.search_similar(
                embedding=query_embedding, limit=RAG_SEARCH_LIMIT, corp_code=corp_code,
            )
            return rag_chunks
        except Exception as e:
            logger.warning("RAG 검색 실패, 근거 없이 분석 진행: %s", str(e))
            return []

    def _build_prompt(self, analysis_type: str, disclosures: list, rag_contexts: list) -> tuple:
        if analysis_type == "flow_analysis":
            return AnalysisPromptBuilder.build_flow_analysis_prompt(disclosures, rag_contexts)
        elif analysis_type == "signal_analysis":
            return AnalysisPromptBuilder.build_signal_analysis_prompt(disclosures, rag_contexts)
        else:
            return AnalysisPromptBuilder.build_full_analysis_prompt(disclosures, rag_contexts)

    async def _call_llm_analysis(self, prompt: str, system_message: str) -> dict:
        try:
            raw_response = await self._llm.analyze(prompt, system_message)
            return self._parse_llm_response(raw_response)
        except Exception as e:
            logger.error("LLM 분석 실패: %s", str(e))
            return {
                "signal": "neutral",
                "confidence": 0.0,
                "summary": f"LLM 분석 중 오류 발생: {str(e)}",
                "key_points": [],
            }

    @staticmethod
    def _parse_llm_response(raw_response: str) -> dict:
        text = raw_response.strip()
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        try:
            parsed = json.loads(text)
            return {
                "signal": parsed.get("signal", "neutral"),
                "confidence": parsed.get("confidence", 0.5),
                "summary": parsed.get("summary", ""),
                "key_points": parsed.get("key_points", []),
            }
        except (json.JSONDecodeError, ValueError):
            logger.warning("LLM 응답 JSON 파싱 실패")
            return {
                "signal": "neutral",
                "confidence": 0.5,
                "summary": raw_response[:500],
                "key_points": [],
            }

    @staticmethod
    def _error_response(ticker: str, elapsed: int, message: str) -> AnalysisResponse:
        return AnalysisResponse(
            status="error",
            data={"ticker": ticker, "filings": []},
            error_message=message,
            execution_time_ms=elapsed,
        )
