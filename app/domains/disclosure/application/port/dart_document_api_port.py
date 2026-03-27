from abc import ABC, abstractmethod


class DartDocumentApiPort(ABC):

    @abstractmethod
    async def fetch_document(self, rcept_no: str) -> str:
        """DART에서 공시 원문을 가져와 텍스트로 반환한다.

        Args:
            rcept_no: 접수번호

        Returns:
            공시 원문 텍스트
        """
        pass
