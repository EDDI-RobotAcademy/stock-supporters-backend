from abc import ABC, abstractmethod
from typing import Optional

from app.domains.disclosure.domain.entity.company_data_coverage import CompanyDataCoverage


class CompanyDataCoverageRepositoryPort(ABC):

    @abstractmethod
    async def save(self, coverage: CompanyDataCoverage) -> CompanyDataCoverage:
        pass

    @abstractmethod
    async def upsert(self, coverage: CompanyDataCoverage) -> CompanyDataCoverage:
        pass

    @abstractmethod
    async def find_by_corp_code(self, corp_code: str) -> Optional[CompanyDataCoverage]:
        pass

    @abstractmethod
    async def find_uncovered_companies(self) -> list[str]:
        """Returns corp_codes of active companies that have no coverage record."""
        pass
