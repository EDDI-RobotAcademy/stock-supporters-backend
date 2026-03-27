from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

from app.domains.disclosure.domain.entity.disclosure import Disclosure


class DisclosureRepositoryPort(ABC):

    @abstractmethod
    async def upsert_bulk(self, disclosures: list[Disclosure]) -> int:
        pass

    @abstractmethod
    async def find_by_rcept_no(self, rcept_no: str) -> Optional[Disclosure]:
        pass

    @abstractmethod
    async def find_by_corp_code(
        self, corp_code: str, limit: int = 50
    ) -> list[Disclosure]:
        pass

    @abstractmethod
    async def find_latest_rcept_dt(self) -> Optional[date]:
        pass

    @abstractmethod
    async def exists_by_rcept_no(self, rcept_no: str) -> bool:
        pass
