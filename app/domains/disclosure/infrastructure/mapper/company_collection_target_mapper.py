from app.domains.disclosure.domain.entity.company_collection_target import CompanyCollectionTarget
from app.domains.disclosure.infrastructure.orm.company_collection_target_orm import CompanyCollectionTargetOrm


class CompanyCollectionTargetMapper:

    @staticmethod
    def to_entity(orm: CompanyCollectionTargetOrm) -> CompanyCollectionTarget:
        return CompanyCollectionTarget(
            target_id=orm.id,
            corp_code=orm.corp_code,
            target_type=orm.target_type,
            is_enabled=orm.is_enabled,
            added_reason=orm.added_reason,
            added_at=orm.added_at,
            updated_at=orm.updated_at,
        )

    @staticmethod
    def to_orm(entity: CompanyCollectionTarget) -> CompanyCollectionTargetOrm:
        return CompanyCollectionTargetOrm(
            corp_code=entity.corp_code,
            target_type=entity.target_type,
            is_enabled=entity.is_enabled,
            added_reason=entity.added_reason,
            added_at=entity.added_at,
            updated_at=entity.updated_at,
        )
