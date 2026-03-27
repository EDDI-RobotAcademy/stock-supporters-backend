from app.domains.disclosure.domain.entity.disclosure_document import DisclosureDocument
from app.domains.disclosure.infrastructure.orm.disclosure_document_orm import DisclosureDocumentOrm


class DisclosureDocumentMapper:

    @staticmethod
    def to_entity(orm: DisclosureDocumentOrm) -> DisclosureDocument:
        return DisclosureDocument(
            document_id=orm.id,
            rcept_no=orm.rcept_no,
            document_type=orm.document_type,
            raw_text=orm.raw_text,
            parsed_json=orm.parsed_json,
            summary_text=orm.summary_text,
            stored_in_rag=orm.stored_in_rag,
            collected_at=orm.collected_at,
            created_at=orm.created_at,
            updated_at=orm.updated_at,
        )

    @staticmethod
    def to_orm(entity: DisclosureDocument) -> DisclosureDocumentOrm:
        return DisclosureDocumentOrm(
            rcept_no=entity.rcept_no,
            document_type=entity.document_type,
            raw_text=entity.raw_text,
            parsed_json=entity.parsed_json,
            summary_text=entity.summary_text,
            stored_in_rag=entity.stored_in_rag,
            collected_at=entity.collected_at,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
