from typing import TypedDict

from stock.application.port.disclosure_analysis_agent_port import (
    DisclosureAnalysisAgentPort,
)


class State(TypedDict):
    input_text: str
    processed_text: str


class DisclosureAnalysisAgentGateway(DisclosureAnalysisAgentPort):
    # DisclosureAnalysisAgentPort의 실제 구현체
    # 호출받아서 공시 분석 관련 분석 작업을 처리함

    def call() -> str:
        pass
