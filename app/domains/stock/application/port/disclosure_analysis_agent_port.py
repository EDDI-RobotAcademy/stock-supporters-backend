from abc import ABC, abstractmethod

class DisclosureAnalysisAgentPort(ABC):
    # 기업 공시 분석 포트

    @abstractmethod
    def call(self) -> str:
        pass