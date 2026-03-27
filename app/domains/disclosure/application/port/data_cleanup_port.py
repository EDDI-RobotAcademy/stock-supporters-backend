from abc import ABC, abstractmethod
from datetime import date


class DataCleanupPort(ABC):

    @abstractmethod
    async def delete_old_disclosures(self, before_date: date) -> int:
        pass

    @abstractmethod
    async def delete_old_collection_jobs(self, before_date: date) -> int:
        pass

    @abstractmethod
    async def delete_orphaned_rag_chunks(self) -> int:
        pass

    @abstractmethod
    async def get_data_stats(self) -> dict:
        pass
