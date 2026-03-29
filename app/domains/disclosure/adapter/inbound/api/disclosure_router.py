from fastapi import APIRouter, Query

from app.domains.disclosure.application.response.analysis_response import AnalysisResponse
from app.domains.disclosure.application.service.disclosure_analysis_service import (
    DisclosureAnalysisService,
)

router = APIRouter(prefix="/disclosure", tags=["Disclosure"])

_service = DisclosureAnalysisService()


@router.get("/analyze", response_model=AnalysisResponse)
async def analyze_disclosure(
    ticker: str = Query(..., description="종목코드 (예: 005930)"),
    analysis_type: str = Query(
        "full_analysis",
        description="분석 유형: flow_analysis | signal_analysis | full_analysis",
    ),
) -> AnalysisResponse:
    """공시 분석 테스트 엔드포인트.

    ticker(종목코드)를 받아 해당 기업의 공시를 분석한 결과를 반환한다.
    """
    return await _service.analyze(ticker=ticker, analysis_type=analysis_type)
