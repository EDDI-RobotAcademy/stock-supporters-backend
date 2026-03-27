from abc import ABC, abstractmethod
from typing import Optional

from app.domains.disclosure.domain.entity.company_collection_target import CompanyCollectionTarget


class CompanyCollectionTargetRepositoryPort(ABC):

    @abstractmethod
    async def save(self, target: CompanyCollectionTarget) -> CompanyCollectionTarget:
        pass

    @abstractmethod
    async def find_by_corp_code(self, corp_code: str) -> Optional[CompanyCollectionTarget]:
        pass

    @abstractmethod
    async def find_all_enabled(self) -> list[CompanyCollectionTarget]:
        pass

    @abstractmethod
    async def disable_by_corp_code(self, corp_code: str) -> bool:
        pass
