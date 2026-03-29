from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select, update, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.disclosure.application.port.company_repository_port import CompanyRepositoryPort
from app.domains.disclosure.domain.entity.company import Company
from app.domains.disclosure.infrastructure.mapper.company_mapper import CompanyMapper
from app.domains.disclosure.infrastructure.orm.company_orm import CompanyOrm


class CompanyRepositoryImpl(CompanyRepositoryPort):
    def __init__(self, db: AsyncSession):
        self._db = db

    async def save(self, company: Company) -> Company:
        orm = CompanyMapper.to_orm(company)
        self._db.add(orm)
        await self._db.commit()
        await self._db.refresh(orm)
        return CompanyMapper.to_entity(orm)

    async def save_bulk(self, companies: list[Company]) -> int:
        if not companies:
            return 0

        values = [
            {
                "corp_code": c.corp_code,
                "corp_name": c.corp_name,
                "stock_code": c.stock_code,
                "market_type": c.market_type,
                "market_cap_rank": c.market_cap_rank,
                "is_top300": c.is_top300,
                "is_active": c.is_active,
            }
            for c in companies
        ]

        excluded = insert(CompanyOrm).excluded
        stmt = (
            insert(CompanyOrm)
            .values(values)
            .on_conflict_do_update(
                index_elements=["corp_code"],
                set_={
                    "corp_name": excluded.corp_name,
                    "stock_code": excluded.stock_code,
                    "market_type": excluded.market_type,
                    "market_cap_rank": excluded.market_cap_rank,
                    "is_top300": excluded.is_top300,
                    "is_active": excluded.is_active,
                },
            )
            .returning(CompanyOrm.id)
        )

        result = await self._db.execute(stmt)
        await self._db.commit()
        return len(result.fetchall())

    async def find_by_corp_code(self, corp_code: str) -> Optional[Company]:
        stmt = select(CompanyOrm).where(CompanyOrm.corp_code == corp_code)
        result = await self._db.execute(stmt)
        orm = result.scalar_one_or_none()
        if orm is None:
            return None
        return CompanyMapper.to_entity(orm)

    async def find_by_stock_code(self, stock_code: str) -> Optional[Company]:
        stmt = select(CompanyOrm).where(CompanyOrm.stock_code == stock_code)
        result = await self._db.execute(stmt)
        orm = result.scalar_one_or_none()
        if orm is None:
            return None
        return CompanyMapper.to_entity(orm)

    async def find_top300(self) -> list[Company]:
        stmt = (
            select(CompanyOrm)
            .where(CompanyOrm.is_top300.is_(True), CompanyOrm.is_active.is_(True))
            .order_by(CompanyOrm.market_cap_rank.asc())
        )
        result = await self._db.execute(stmt)
        return [CompanyMapper.to_entity(orm) for orm in result.scalars().all()]

    async def find_all_active(self) -> list[Company]:
        stmt = select(CompanyOrm).where(CompanyOrm.is_active.is_(True))
        result = await self._db.execute(stmt)
        return [CompanyMapper.to_entity(orm) for orm in result.scalars().all()]

    async def update_top300_flags(self, top300_corp_codes: list[str]) -> int:
        # 기존 top300 플래그 초기화
        await self._db.execute(
            update(CompanyOrm).where(CompanyOrm.is_top300.is_(True)).values(is_top300=False)
        )

        if not top300_corp_codes:
            await self._db.commit()
            return 0

        # 새로운 top300 설정 + 순위 부여 + 수집 대상 플래그 활성화 (벌크 CASE WHEN)
        from sqlalchemy import case
        case_expr = case(
            *[(CompanyOrm.corp_code == code, rank) for rank, code in enumerate(top300_corp_codes, start=1)],
        )
        await self._db.execute(
            update(CompanyOrm)
            .where(CompanyOrm.corp_code.in_(top300_corp_codes))
            .values(is_top300=True, is_collect_target=True, market_cap_rank=case_expr)
        )

        await self._db.commit()
        return len(top300_corp_codes)

    async def mark_as_collect_target(self, corp_code: str) -> bool:
        result = await self._db.execute(
            update(CompanyOrm)
            .where(CompanyOrm.corp_code == corp_code)
            .values(is_collect_target=True, last_requested_at=datetime.now())
        )
        await self._db.commit()
        return result.rowcount > 0

    async def find_collect_targets(self, recent_days: int = 30) -> list[Company]:
        cutoff = datetime.now() - timedelta(days=recent_days)
        stmt = (
            select(CompanyOrm)
            .where(
                CompanyOrm.is_active.is_(True),
                CompanyOrm.is_collect_target.is_(True),
                or_(
                    CompanyOrm.is_top300.is_(True),
                    CompanyOrm.last_requested_at >= cutoff,
                ),
            )
            .order_by(CompanyOrm.market_cap_rank.asc().nulls_last())
        )
        result = await self._db.execute(stmt)
        return [CompanyMapper.to_entity(orm) for orm in result.scalars().all()]
