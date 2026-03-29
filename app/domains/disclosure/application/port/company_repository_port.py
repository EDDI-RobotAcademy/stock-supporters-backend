from abc import ABC, abstractmethod
from typing import Optional

from app.domains.disclosure.domain.entity.company import Company


class CompanyRepositoryPort(ABC):

    @abstractmethod
    async def save(self, company: Company) -> Company:
        pass

    @abstractmethod
    async def save_bulk(self, companies: list[Company]) -> int:
        pass

    @abstractmethod
    async def find_by_corp_code(self, corp_code: str) -> Optional[Company]:
        pass

    @abstractmethod
    async def find_by_stock_code(self, stock_code: str) -> Optional[Company]:
        pass

    @abstractmethod
    async def find_top300(self) -> list[Company]:
        pass

    @abstractmethod
    async def find_all_active(self) -> list[Company]:
        pass

    @abstractmethod
    async def update_top300_flags(self, top300_corp_codes: list[str]) -> int:
        pass

    @abstractmethod
    async def mark_as_collect_target(self, corp_code: str) -> bool:
        pass

    @abstractmethod
    async def find_collect_targets(self, recent_days: int = 30) -> list[Company]:
        pass
