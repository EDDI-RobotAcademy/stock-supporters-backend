from datetime import date

from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.disclosure.application.port.data_cleanup_port import DataCleanupPort
from app.domains.disclosure.infrastructure.orm.collection_job_orm import CollectionJobOrm
from app.domains.disclosure.infrastructure.orm.company_orm import CompanyOrm
from app.domains.disclosure.infrastructure.orm.disclosure_document_orm import DisclosureDocumentOrm
from app.domains.disclosure.infrastructure.orm.disclosure_orm import DisclosureOrm
from app.domains.disclosure.infrastructure.orm.rag_document_chunk_orm import RagDocumentChunkOrm


class DataCleanupRepositoryImpl(DataCleanupPort):
    def __init__(self, db: AsyncSession):
        self._db = db

    async def delete_old_disclosures(self, before_date: date) -> int:
        stmt = (
            delete(DisclosureOrm)
            .where(DisclosureOrm.rcept_dt < before_date)
            .returning(DisclosureOrm.id)
        )
        result = await self._db.execute(stmt)
        await self._db.commit()
        return len(result.fetchall())

    async def delete_old_collection_jobs(self, before_date: date) -> int:
        stmt = (
            delete(CollectionJobOrm)
            .where(CollectionJobOrm.started_at < before_date)
            .returning(CollectionJobOrm.id)
        )
        result = await self._db.execute(stmt)
        await self._db.commit()
        return len(result.fetchall())

    async def delete_orphaned_rag_chunks(self) -> int:
        subquery = select(DisclosureOrm.rcept_no)
        stmt = (
            delete(RagDocumentChunkOrm)
            .where(~RagDocumentChunkOrm.rcept_no.in_(subquery))
            .returning(RagDocumentChunkOrm.id)
        )
        result = await self._db.execute(stmt)
        await self._db.commit()
        return len(result.fetchall())

    async def get_data_stats(self) -> dict:
        disclosure_count = await self._db.execute(
            select(func.count()).select_from(DisclosureOrm)
        )
        document_count = await self._db.execute(
            select(func.count()).select_from(DisclosureDocumentOrm)
        )
        rag_chunk_count = await self._db.execute(
            select(func.count()).select_from(RagDocumentChunkOrm)
        )
        company_count = await self._db.execute(
            select(func.count()).select_from(CompanyOrm)
        )
        job_count = await self._db.execute(
            select(func.count()).select_from(CollectionJobOrm)
        )

        return {
            "disclosure_count": disclosure_count.scalar_one(),
            "document_count": document_count.scalar_one(),
            "rag_chunk_count": rag_chunk_count.scalar_one(),
            "company_count": company_count.scalar_one(),
            "job_count": job_count.scalar_one(),
        }
