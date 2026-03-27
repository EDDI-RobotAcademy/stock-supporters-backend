from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.disclosure.application.port.disclosure_document_repository_port import (
    DisclosureDocumentRepositoryPort,
)
from app.domains.disclosure.domain.entity.disclosure_document import DisclosureDocument
from app.domains.disclosure.infrastructure.mapper.disclosure_document_mapper import (
    DisclosureDocumentMapper,
)
from app.domains.disclosure.infrastructure.orm.disclosure_document_orm import (
    DisclosureDocumentOrm,
)


class DisclosureDocumentRepositoryImpl(DisclosureDocumentRepositoryPort):
    def __init__(self, db: AsyncSession):
        self._db = db

    async def upsert(self, document: DisclosureDocument) -> DisclosureDocument:
        now = datetime.now()

        values = {
            "rcept_no": document.rcept_no,
            "document_type": document.document_type,
            "raw_text": document.raw_text,
            "parsed_json": document.parsed_json,
            "summary_text": document.summary_text,
            "stored_in_rag": document.stored_in_rag,
            "collected_at": document.collected_at or now,
            "created_at": document.created_at or now,
            "updated_at": now,
        }

        stmt = (
            insert(DisclosureDocumentOrm)
            .values(values)
            .on_conflict_do_update(
                constraint="uq_disclosure_documents_rcept_no_document_type",
                set_={
                    "raw_text": values["raw_text"],
                    "parsed_json": values["parsed_json"],
                    "summary_text": values["summary_text"],
                    "stored_in_rag": values["stored_in_rag"],
                    "updated_at": now,
                },
            )
            .returning(DisclosureDocumentOrm.id)
        )

        result = await self._db.execute(stmt)
        row = result.fetchone()
        await self._db.commit()

        # 저장된 레코드를 조회하여 반환
        saved = await self._find_by_id(row[0])
        return saved

    async def _find_by_id(self, document_id: int) -> DisclosureDocument:
        stmt = select(DisclosureDocumentOrm).where(DisclosureDocumentOrm.id == document_id)
        result = await self._db.execute(stmt)
        orm = result.scalar_one()
        return DisclosureDocumentMapper.to_entity(orm)

    async def find_by_rcept_no(self, rcept_no: str) -> list[DisclosureDocument]:
        stmt = (
            select(DisclosureDocumentOrm)
            .where(DisclosureDocumentOrm.rcept_no == rcept_no)
            .order_by(DisclosureDocumentOrm.document_type)
        )
        result = await self._db.execute(stmt)
        return [DisclosureDocumentMapper.to_entity(orm) for orm in result.scalars().all()]

    async def find_by_rcept_no_and_type(
        self, rcept_no: str, document_type: str
    ) -> Optional[DisclosureDocument]:
        stmt = select(DisclosureDocumentOrm).where(
            DisclosureDocumentOrm.rcept_no == rcept_no,
            DisclosureDocumentOrm.document_type == document_type,
        )
        result = await self._db.execute(stmt)
        orm = result.scalar_one_or_none()
        if orm is None:
            return None
        return DisclosureDocumentMapper.to_entity(orm)

    async def find_not_stored_in_rag(self, limit: int = 100) -> list[DisclosureDocument]:
        stmt = (
            select(DisclosureDocumentOrm)
            .where(DisclosureDocumentOrm.stored_in_rag == False)  # noqa: E712
            .where(DisclosureDocumentOrm.raw_text.isnot(None))
            .order_by(DisclosureDocumentOrm.collected_at.asc())
            .limit(limit)
        )
        result = await self._db.execute(stmt)
        return [DisclosureDocumentMapper.to_entity(orm) for orm in result.scalars().all()]
