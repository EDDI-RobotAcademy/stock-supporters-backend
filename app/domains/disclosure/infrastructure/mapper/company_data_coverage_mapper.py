from app.domains.disclosure.domain.entity.company_data_coverage import CompanyDataCoverage
from app.domains.disclosure.infrastructure.orm.company_data_coverage_orm import CompanyDataCoverageOrm


class CompanyDataCoverageMapper:

    @staticmethod
    def to_entity(orm: CompanyDataCoverageOrm) -> CompanyDataCoverage:
        return CompanyDataCoverage(
            coverage_id=orm.id,
            corp_code=orm.corp_code,
            has_b001=orm.has_b001,
            has_d002_d005=orm.has_d002_d005,
            has_d001=orm.has_d001,
            has_e001=orm.has_e001,
            has_c001=orm.has_c001,
            has_a001=orm.has_a001,
            has_a002=orm.has_a002,
            has_a003=orm.has_a003,
            has_event_documents=orm.has_event_documents,
            last_collected_at=orm.last_collected_at,
            last_on_demand_at=orm.last_on_demand_at,
            created_at=orm.created_at,
            updated_at=orm.updated_at,
        )

    @staticmethod
    def to_orm(entity: CompanyDataCoverage) -> CompanyDataCoverageOrm:
        return CompanyDataCoverageOrm(
            corp_code=entity.corp_code,
            has_b001=entity.has_b001,
            has_d002_d005=entity.has_d002_d005,
            has_d001=entity.has_d001,
            has_e001=entity.has_e001,
            has_c001=entity.has_c001,
            has_a001=entity.has_a001,
            has_a002=entity.has_a002,
            has_a003=entity.has_a003,
            has_event_documents=entity.has_event_documents,
            last_collected_at=entity.last_collected_at,
            last_on_demand_at=entity.last_on_demand_at,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
