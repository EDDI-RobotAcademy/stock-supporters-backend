from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.disclosure.application.port.company_data_coverage_repository_port import (
    CompanyDataCoverageRepositoryPort,
)
from app.domains.disclosure.domain.entity.company_data_coverage import CompanyDataCoverage
from app.domains.disclosure.infrastructure.mapper.company_data_coverage_mapper import (
    CompanyDataCoverageMapper,
)
from app.domains.disclosure.infrastructure.orm.company_data_coverage_orm import (
    CompanyDataCoverageOrm,
)
from app.domains.disclosure.infrastructure.orm.company_orm import CompanyOrm


class CompanyDataCoverageRepositoryImpl(CompanyDataCoverageRepositoryPort):
    def __init__(self, db: AsyncSession):
        self._db = db

    async def save(self, coverage: CompanyDataCoverage) -> CompanyDataCoverage:
        orm = CompanyDataCoverageMapper.to_orm(coverage)
        self._db.add(orm)
        await self._db.commit()
        await self._db.refresh(orm)
        return CompanyDataCoverageMapper.to_entity(orm)

    async def upsert(self, coverage: CompanyDataCoverage) -> CompanyDataCoverage:
        values = {
            "corp_code": coverage.corp_code,
            "has_b001": coverage.has_b001,
            "has_d002_d005": coverage.has_d002_d005,
            "has_d001": coverage.has_d001,
            "has_e001": coverage.has_e001,
            "has_c001": coverage.has_c001,
            "has_a001": coverage.has_a001,
            "has_a002": coverage.has_a002,
            "has_a003": coverage.has_a003,
            "has_event_documents": coverage.has_event_documents,
            "last_collected_at": coverage.last_collected_at,
            "last_on_demand_at": coverage.last_on_demand_at,
        }

        stmt = (
            insert(CompanyDataCoverageOrm)
            .values(values)
            .on_conflict_do_update(
                index_elements=["corp_code"],
                set_={
                    "has_b001": insert(CompanyDataCoverageOrm).excluded.has_b001,
                    "has_d002_d005": insert(CompanyDataCoverageOrm).excluded.has_d002_d005,
                    "has_d001": insert(CompanyDataCoverageOrm).excluded.has_d001,
                    "has_e001": insert(CompanyDataCoverageOrm).excluded.has_e001,
                    "has_c001": insert(CompanyDataCoverageOrm).excluded.has_c001,
                    "has_a001": insert(CompanyDataCoverageOrm).excluded.has_a001,
                    "has_a002": insert(CompanyDataCoverageOrm).excluded.has_a002,
                    "has_a003": insert(CompanyDataCoverageOrm).excluded.has_a003,
                    "has_event_documents": insert(CompanyDataCoverageOrm).excluded.has_event_documents,
                    "last_collected_at": insert(CompanyDataCoverageOrm).excluded.last_collected_at,
                    "last_on_demand_at": insert(CompanyDataCoverageOrm).excluded.last_on_demand_at,
                },
            )
            .returning(CompanyDataCoverageOrm.id)
        )

        result = await self._db.execute(stmt)
        await self._db.commit()

        row_id = result.scalar_one()
        return await self._find_by_id(row_id)

    async def _find_by_id(self, coverage_id: int) -> CompanyDataCoverage:
        stmt = select(CompanyDataCoverageOrm).where(CompanyDataCoverageOrm.id == coverage_id)
        result = await self._db.execute(stmt)
        orm = result.scalar_one()
        return CompanyDataCoverageMapper.to_entity(orm)

    async def find_by_corp_code(self, corp_code: str) -> Optional[CompanyDataCoverage]:
        stmt = select(CompanyDataCoverageOrm).where(
            CompanyDataCoverageOrm.corp_code == corp_code
        )
        result = await self._db.execute(stmt)
        orm = result.scalar_one_or_none()
        if orm is None:
            return None
        return CompanyDataCoverageMapper.to_entity(orm)

    async def find_uncovered_companies(self) -> list[str]:
        stmt = (
            select(CompanyOrm.corp_code)
            .outerjoin(
                CompanyDataCoverageOrm,
                CompanyOrm.corp_code == CompanyDataCoverageOrm.corp_code,
            )
            .where(
                CompanyOrm.is_active.is_(True),
                CompanyDataCoverageOrm.id.is_(None),
            )
            .order_by(CompanyOrm.market_cap_rank.asc())
        )
        result = await self._db.execute(stmt)
        return list(result.scalars().all())
