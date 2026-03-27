from abc import ABC, abstractmethod
from typing import Optional

from app.domains.disclosure.domain.entity.collection_job import CollectionJob
from app.domains.disclosure.domain.entity.collection_job_item import CollectionJobItem


class CollectionJobRepositoryPort(ABC):

    @abstractmethod
    async def save_job(self, job: CollectionJob) -> CollectionJob:
        pass

    @abstractmethod
    async def update_job(self, job: CollectionJob) -> CollectionJob:
        pass

    @abstractmethod
    async def save_items(self, items: list[CollectionJobItem]) -> int:
        pass

    @abstractmethod
    async def find_latest_by_job_name(self, job_name: str) -> Optional[CollectionJob]:
        pass
