from app.domains.disclosure.domain.entity.company import Company
from app.domains.disclosure.infrastructure.orm.company_orm import CompanyOrm


class CompanyMapper:

    @staticmethod
    def to_entity(orm: CompanyOrm) -> Company:
        return Company(
            company_id=orm.id,
            corp_code=orm.corp_code,
            corp_name=orm.corp_name,
            stock_code=orm.stock_code,
            market_type=orm.market_type,
            market_cap_rank=orm.market_cap_rank,
            is_top300=orm.is_top300,
            is_active=orm.is_active,
            created_at=orm.created_at,
            updated_at=orm.updated_at,
        )

    @staticmethod
    def to_orm(entity: Company) -> CompanyOrm:
        return CompanyOrm(
            corp_code=entity.corp_code,
            corp_name=entity.corp_name,
            stock_code=entity.stock_code,
            market_type=entity.market_type,
            market_cap_rank=entity.market_cap_rank,
            is_top300=entity.is_top300,
            is_active=entity.is_active,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
