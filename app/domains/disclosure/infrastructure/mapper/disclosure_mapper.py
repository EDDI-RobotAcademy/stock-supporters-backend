from app.domains.disclosure.domain.entity.disclosure import Disclosure
from app.domains.disclosure.infrastructure.orm.disclosure_orm import DisclosureOrm


class DisclosureMapper:

    @staticmethod
    def to_entity(orm: DisclosureOrm) -> Disclosure:
        return Disclosure(
            disclosure_id=orm.id,
            rcept_no=orm.rcept_no,
            corp_code=orm.corp_code,
            report_nm=orm.report_nm,
            rcept_dt=orm.rcept_dt,
            pblntf_ty=orm.pblntf_ty,
            pblntf_detail_ty=orm.pblntf_detail_ty,
            rm=orm.rm,
            disclosure_group=orm.disclosure_group,
            source_mode=orm.source_mode,
            is_core=orm.is_core,
            created_at=orm.created_at,
            updated_at=orm.updated_at,
        )

    @staticmethod
    def to_orm(entity: Disclosure) -> DisclosureOrm:
        return DisclosureOrm(
            rcept_no=entity.rcept_no,
            corp_code=entity.corp_code,
            report_nm=entity.report_nm,
            rcept_dt=entity.rcept_dt,
            pblntf_ty=entity.pblntf_ty,
            pblntf_detail_ty=entity.pblntf_detail_ty,
            rm=entity.rm,
            disclosure_group=entity.disclosure_group,
            source_mode=entity.source_mode,
            is_core=entity.is_core,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
