from app.domains.disclosure.domain.entity.rag_document_chunk import RagDocumentChunk
from app.domains.disclosure.infrastructure.orm.rag_document_chunk_orm import RagDocumentChunkOrm


class RagDocumentChunkMapper:

    @staticmethod
    def to_entity(orm: RagDocumentChunkOrm) -> RagDocumentChunk:
        embedding = list(orm.embedding) if orm.embedding is not None else None
        return RagDocumentChunk(
            chunk_id=orm.id,
            rcept_no=orm.rcept_no,
            corp_code=orm.corp_code,
            disclosure_document_id=orm.disclosure_document_id,
            report_nm=orm.report_nm,
            document_type=orm.document_type,
            section_title=orm.section_title,
            chunk_index=orm.chunk_index,
            chunk_text=orm.chunk_text,
            chunk_hash=orm.chunk_hash,
            embedding=embedding,
            created_at=orm.created_at,
            updated_at=orm.updated_at,
        )

    @staticmethod
    def to_orm(entity: RagDocumentChunk) -> RagDocumentChunkOrm:
        return RagDocumentChunkOrm(
            rcept_no=entity.rcept_no,
            corp_code=entity.corp_code,
            disclosure_document_id=entity.disclosure_document_id,
            report_nm=entity.report_nm,
            document_type=entity.document_type,
            section_title=entity.section_title,
            chunk_index=entity.chunk_index,
            chunk_text=entity.chunk_text,
            chunk_hash=entity.chunk_hash,
            embedding=entity.embedding,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
