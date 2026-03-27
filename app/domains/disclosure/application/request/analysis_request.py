from pydantic import BaseModel, Field


class AnalysisRequest(BaseModel):
    corp_code: str = Field(..., description="기업 코드")
    analysis_type: str = Field(
        default="full_analysis",
        description="분석 유형 (flow_analysis, signal_analysis, full_analysis)",
    )
