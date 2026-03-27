from abc import ABC, abstractmethod
from typing import Optional

from app.domains.disclosure.domain.entity.disclosure_document import DisclosureDocument


class DisclosureDocumentRepositoryPort(ABC):

    @abstractmethod
    async def upsert(self, document: DisclosureDocument) -> DisclosureDocument:
        pass

    @abstractmethod
    async def find_by_rcept_no(self, rcept_no: str) -> list[DisclosureDocument]:
        pass

    @abstractmethod
    async def find_by_rcept_no_and_type(
        self, rcept_no: str, document_type: str
    ) -> Optional[DisclosureDocument]:
        pass

    @abstractmethod
    async def find_not_stored_in_rag(self, limit: int = 100) -> list[DisclosureDocument]:
        pass
