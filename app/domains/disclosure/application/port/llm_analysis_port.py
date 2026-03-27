from abc import ABC, abstractmethod


class LlmAnalysisPort(ABC):
    """LLM 분석 호출을 추상화하는 포트 인터페이스"""

    @abstractmethod
    async def analyze(self, prompt: str, system_message: str) -> str:
        """프롬프트와 시스템 메시지를 기반으로 LLM 분석을 수행한다."""
        pass
