from typing import Optional

from pydantic import BaseModel, Field


class FilingInfo(BaseModel):
    title: str
    filed_at: str
    type: str


class AnalysisResponse(BaseModel):
    agent_name: str = Field(default="disclosure")
    status: str = Field(default="success")
    data: dict = Field(default_factory=dict)
    error_message: Optional[str] = Field(default=None)
    execution_time_ms: int = Field(default=0)
    signal: Optional[str] = Field(default=None)
    confidence: Optional[float] = Field(default=None)
    summary: Optional[str] = Field(default=None)
    key_points: list[str] = Field(default_factory=list)
