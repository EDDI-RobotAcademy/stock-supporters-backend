from abc import ABC, abstractmethod
from typing import Optional


class AnalysisCachePort(ABC):
    """분석 결과 캐시를 위한 포트 인터페이스"""

    @abstractmethod
    async def get(self, ticker: str, analysis_type: str) -> Optional[dict]:
        """캐시된 분석 결과를 조회한다."""
        pass

    @abstractmethod
    async def save(
        self, ticker: str, analysis_type: str, result: dict, ttl_seconds: int = 3600
    ) -> None:
        """분석 결과를 TTL과 함께 캐시에 저장한다."""
        pass

    @abstractmethod
    async def delete(self, ticker: str, analysis_type: str) -> bool:
        """캐시된 분석 결과를 삭제한다."""
        pass

    @abstractmethod
    async def exists(self, ticker: str, analysis_type: str) -> bool:
        """캐시에 분석 결과가 존재하는지 확인한다."""
        pass
