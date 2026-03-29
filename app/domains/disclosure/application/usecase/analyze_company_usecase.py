import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from app.domains.disclosure.application.port.analysis_cache_port import AnalysisCachePort
from app.domains.disclosure.application.port.dart_disclosure_api_port import DartDisclosureApiPort
from app.domains.disclosure.application.port.disclosure_repository_port import DisclosureRepositoryPort
from app.domains.disclosure.application.port.embedding_port import EmbeddingPort
from app.domains.disclosure.application.port.llm_analysis_port import LlmAnalysisPort
from app.domains.disclosure.application.port.rag_chunk_repository_port import RagChunkRepositoryPort
from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.application.response.analysis_response import AnalysisResponse
from app.domains.disclosure.domain.entity.disclosure import Disclosure
from app.domains.disclosure.domain.service.analysis_prompt_builder import AnalysisPromptBuilder
from app.domains.disclosure.domain.service.disclosure_classifier import DisclosureClassifier

logger = logging.getLogger(__name__)

VALID_ANALYSIS_TYPES = {"flow_analysis", "signal_analysis", "full_analysis"}
DEFAULT_CACHE_TTL = 3600
RAG_SEARCH_LIMIT = 5


@dataclass
class AnalysisContext:
    """DB 단계에서 수집된 데이터를 LLM 단계로 전달하는 컨텍스트."""
    ticker: str
    analysis_type: str
    disclosures: list = field(default_factory=list)
    rag_contexts: list = field(default_factory=list)
    filings: list = field(default_factory=list)
    is_lightweight: bool = False
    registered: Optional[bool] = None
    empty: bool = False


class AnalyzeCompanyUseCase:

    def __init__(
        self,
        analysis_cache_port: AnalysisCachePort,
        disclosure_repository_port: DisclosureRepositoryPort,
        rag_chunk_repository_port: RagChunkRepositoryPort,
        embedding_port: EmbeddingPort,
        llm_analysis_port: LlmAnalysisPort,
        company_repository_port: CompanyRepositoryPort,
        dart_disclosure_api_port: DartDisclosureApiPort,
    ):
        self._cache = analysis_cache_port
        self._disclosure_repo = disclosure_repository_port
        self._rag_repo = rag_chunk_repository_port
        self._embedding = embedding_port
        self._llm = llm_analysis_port
        self._company_repo = company_repository_port
        self._dart_api = dart_disclosure_api_port

    # ------------------------------------------------------------------
    # Phase 1: DB 의존 작업 — 세션이 열려있는 동안 호출
    # ------------------------------------------------------------------

    async def gather_context(
        self, corp_code: str, ticker: str, analysis_type: str,
    ) -> AnalysisContext:
        """DB/외부 API에서 분석에 필요한 데이터를 수집한다. (DB 세션 필요)"""
        if analysis_type not in VALID_ANALYSIS_TYPES:
            raise ValueError(f"유효하지 않은 분석 유형: {analysis_type}")

        disclosures = await self._disclosure_repo.find_by_corp_code(corp_code, limit=50)

        if not disclosures:
            return await self._gather_lightweight_context(corp_code, ticker, analysis_type)

        # 수집 대상 기업이면 last_requested_at 갱신
        await self._company_repo.mark_as_collect_target(corp_code)

        # 공시 분류
        event_disclosures = [
            d for d in disclosures
            if DisclosureClassifier.classify_group(d.report_nm) == "event"
        ]

        # RAG 검색 (임베딩 생성 → pgvector 유사도 검색)
        analysis_query = self._build_analysis_query(corp_code, disclosures, event_disclosures)
        rag_contexts = await self._search_rag_contexts(analysis_query, corp_code)

        # signal_analysis 시 이벤트 공시 우선 사용
        analysis_disclosures = (
            event_disclosures
            if (analysis_type == "signal_analysis" and event_disclosures)
            else disclosures
        )

        filings = [
            {
                "title": d.report_nm,
                "filed_at": d.rcept_dt.isoformat(),
                "type": DisclosureClassifier.classify_group(d.report_nm),
            }
            for d in disclosures[:10]
        ]

        return AnalysisContext(
            ticker=ticker,
            analysis_type=analysis_type,
            disclosures=analysis_disclosures,
            rag_contexts=rag_contexts,
            filings=filings,
        )

    async def _gather_lightweight_context(
        self, corp_code: str, ticker: str, analysis_type: str,
    ) -> AnalysisContext:
        """DB에 공시가 없는 기업: DART 직접 조회로 컨텍스트를 수집한다."""
        logger.info("경량 분석 컨텍스트 수집: corp_code=%s (DB 미수집 기업)", corp_code)

        end_date = datetime.now().strftime("%Y%m%d")
        bgn_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")

        dart_items = await self._dart_api.fetch_all_pages(
            bgn_de=bgn_date, end_de=end_date, corp_code=corp_code,
        )

        registered = await self._company_repo.mark_as_collect_target(corp_code)
        if not registered:
            logger.warning("수집 대상 등록 실패 (DB에 기업 없음): corp_code=%s", corp_code)

        if not dart_items:
            return AnalysisContext(
                ticker=ticker, analysis_type=analysis_type,
                is_lightweight=True, registered=registered, empty=True,
            )

        filings = [
            {
                "title": item.report_nm,
                "filed_at": f"{item.rcept_dt[:4]}-{item.rcept_dt[4:6]}-{item.rcept_dt[6:]}",
                "type": DisclosureClassifier.classify_group(item.report_nm),
            }
            for item in dart_items[:10]
        ]

        temp_disclosures = [
            Disclosure(
                rcept_no=item.rcept_no,
                corp_code=item.corp_code,
                report_nm=item.report_nm,
                rcept_dt=datetime.strptime(item.rcept_dt, "%Y%m%d").date(),
                pblntf_ty=item.pblntf_ty,
                pblntf_detail_ty=item.pblntf_detail_ty,
                rm=item.rm,
                disclosure_group=DisclosureClassifier.classify_group(item.report_nm),
                source_mode="ondemand",
                is_core=DisclosureClassifier.is_core_disclosure(item.report_nm),
            )
            for item in dart_items
        ]

        return AnalysisContext(
            ticker=ticker, analysis_type=analysis_type,
            disclosures=temp_disclosures, filings=filings,
            is_lightweight=True, registered=registered,
        )

    # ------------------------------------------------------------------
    # Phase 2: LLM 분석 — DB 세션 불필요, 세션 해제 후 호출 가능
    # ------------------------------------------------------------------

    async def analyze_from_context(
        self, context: AnalysisContext, start_time: float,
    ) -> AnalysisResponse:
        """수집된 컨텍스트를 바탕으로 LLM 분석 + 캐시 저장을 수행한다. (DB 세션 불필요)"""
        ticker = context.ticker
        analysis_type = context.analysis_type

        # 빈 결과 처리
        if context.empty:
            elapsed = int((time.monotonic() - start_time) * 1000)
            message = (
                "해당 기업의 최근 6개월 공시가 없습니다. 수집 대상으로 등록되었습니다."
                if context.registered
                else "해당 기업의 최근 6개월 공시가 없습니다. 기업 정보가 DB에 없어 수집 대상 등록에 실패했습니다."
            )
            return AnalysisResponse(
                data={"ticker": ticker, "filings": []},
                execution_time_ms=elapsed,
                summary=message,
            )

        # 프롬프트 생성 → LLM 호출
        prompt, system_message = self._build_prompt(
            analysis_type, context.disclosures, context.rag_contexts,
        )
        llm_result = await self._call_llm_analysis(prompt, system_message)

        # 캐시 저장
        cache_data = {
            "filings": context.filings,
            "signal": llm_result.get("signal"),
            "confidence": llm_result.get("confidence"),
            "summary": llm_result.get("summary"),
            "key_points": llm_result.get("key_points", []),
        }
        await self._cache.save(ticker, analysis_type, cache_data, DEFAULT_CACHE_TTL)

        elapsed = int((time.monotonic() - start_time) * 1000)

        if context.is_lightweight:
            logger.info("경량 분석 완료: %dms, 수집대상등록=%s", elapsed, context.registered)
        else:
            logger.info("분석 완료: %dms", elapsed)

        return AnalysisResponse(
            data={"ticker": ticker, "filings": context.filings},
            execution_time_ms=elapsed,
            signal=llm_result.get("signal"),
            confidence=llm_result.get("confidence"),
            summary=llm_result.get("summary"),
            key_points=llm_result.get("key_points", []),
        )

    # ------------------------------------------------------------------
    # 단일 호출 진입점 (세션 분리 없이 사용할 때)
    # ------------------------------------------------------------------

    async def execute(
        self, corp_code: str, ticker: str, analysis_type: str = "full_analysis",
    ) -> AnalysisResponse:
        start_time = time.monotonic()

        if analysis_type not in VALID_ANALYSIS_TYPES:
            return self._error_response(
                ticker, 0, f"유효하지 않은 분석 유형입니다: {analysis_type}"
            )

        try:
            context = await self.gather_context(corp_code, ticker, analysis_type)
            return await self.analyze_from_context(context, start_time)
        except Exception as e:
            elapsed = int((time.monotonic() - start_time) * 1000)
            logger.error("공시 분석 실패: corp_code=%s, error=%s", corp_code, str(e))
            return self._error_response(ticker, elapsed, str(e))

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------

    @staticmethod
    def _build_analysis_query(corp_code: str, disclosures: list, event_disclosures: list) -> str:
        parts = [f"기업코드 {corp_code} 공시 분석"]
        if event_disclosures:
            parts.append(" ".join(d.report_nm for d in event_disclosures[:5]))
        elif disclosures:
            parts.append(" ".join(d.report_nm for d in disclosures[:3]))
        return " ".join(parts)

    async def _search_rag_contexts(self, query: str, corp_code: str) -> list:
        try:
            query_embedding = await self._embedding.generate(query)
            return await self._rag_repo.search_similar(
                embedding=query_embedding, limit=RAG_SEARCH_LIMIT, corp_code=corp_code,
            )
        except Exception as e:
            logger.warning("RAG 검색 실패, 근거 없이 분석 진행: %s", str(e))
            return []

    @staticmethod
    def _build_prompt(analysis_type: str, disclosures: list, rag_contexts: list) -> tuple:
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

            # signal: overall_signal (signal/full) 또는 trend_analysis 기반 (flow)
            signal = parsed.get("overall_signal") or parsed.get("signal", "neutral")

            # confidence: signal/full 프롬프트에만 존재
            confidence = parsed.get("confidence", 0.5)
            if isinstance(confidence, str):
                try:
                    confidence = float(confidence)
                except ValueError:
                    confidence = 0.5

            # summary: 분석 유형별로 다른 키 사용
            summary = (
                parsed.get("investment_summary")
                or parsed.get("timeline_summary")
                or parsed.get("company_overview")
                or parsed.get("summary", "")
            )

            # key_points: key_events, signals 등 구조화된 데이터에서 추출
            key_points = []
            for event in parsed.get("key_events", []):
                date = event.get("date", "")
                desc = event.get("event", "")
                key_points.append(f"[{date}] {desc}" if date else desc)
            for sig in parsed.get("signals", []):
                direction = sig.get("direction", "")
                desc = sig.get("description", "")
                key_points.append(f"[{direction}] {desc}" if direction else desc)
            if not key_points:
                key_points = parsed.get("risk_factors", []) + parsed.get("positive_signals", [])
            if not key_points:
                key_points = parsed.get("key_points", [])

            return {
                "signal": signal,
                "confidence": confidence,
                "summary": summary,
                "key_points": key_points,
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
