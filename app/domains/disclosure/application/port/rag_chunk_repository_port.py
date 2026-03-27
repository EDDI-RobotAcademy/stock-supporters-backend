from abc import ABC, abstractmethod
from typing import Optional

from app.domains.disclosure.domain.entity.rag_document_chunk import RagDocumentChunk


class RagChunkRepositoryPort(ABC):

    @abstractmethod
    async def upsert_bulk(self, chunks: list[RagDocumentChunk]) -> int:
        pass

    @abstractmethod
    async def find_by_rcept_no(self, rcept_no: str) -> list[RagDocumentChunk]:
        pass

    @abstractmethod
    async def search_similar(
        self,
        embedding: list[float],
        limit: int = 10,
        corp_code: Optional[str] = None,
    ) -> list[RagDocumentChunk]:
        pass
