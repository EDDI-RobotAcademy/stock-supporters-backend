from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.disclosure.application.port.company_collection_target_repository_port import (
    CompanyCollectionTargetRepositoryPort,
)
from app.domains.disclosure.domain.entity.company_collection_target import CompanyCollectionTarget
from app.domains.disclosure.infrastructure.mapper.company_collection_target_mapper import (
    CompanyCollectionTargetMapper,
)
from app.domains.disclosure.infrastructure.orm.company_collection_target_orm import (
    CompanyCollectionTargetOrm,
)


class CompanyCollectionTargetRepositoryImpl(CompanyCollectionTargetRepositoryPort):
    def __init__(self, db: AsyncSession):
        self._db = db

    async def save(self, target: CompanyCollectionTarget) -> CompanyCollectionTarget:
        orm = CompanyCollectionTargetMapper.to_orm(target)
        self._db.add(orm)
        await self._db.commit()
        await self._db.refresh(orm)
        return CompanyCollectionTargetMapper.to_entity(orm)

    async def find_by_corp_code(self, corp_code: str) -> Optional[CompanyCollectionTarget]:
        stmt = select(CompanyCollectionTargetOrm).where(
            CompanyCollectionTargetOrm.corp_code == corp_code
        )
        result = await self._db.execute(stmt)
        orm = result.scalar_one_or_none()
        if orm is None:
            return None
        return CompanyCollectionTargetMapper.to_entity(orm)

    async def find_all_enabled(self) -> list[CompanyCollectionTarget]:
        stmt = select(CompanyCollectionTargetOrm).where(
            CompanyCollectionTargetOrm.is_enabled.is_(True)
        )
        result = await self._db.execute(stmt)
        return [CompanyCollectionTargetMapper.to_entity(orm) for orm in result.scalars().all()]

    async def disable_by_corp_code(self, corp_code: str) -> bool:
        stmt = (
            update(CompanyCollectionTargetOrm)
            .where(CompanyCollectionTargetOrm.corp_code == corp_code)
            .values(is_enabled=False)
        )
        result = await self._db.execute(stmt)
        await self._db.commit()
        return result.rowcount > 0
