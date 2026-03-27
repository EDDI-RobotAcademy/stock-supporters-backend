from app.domains.disclosure.domain.entity.collection_job import CollectionJob
from app.domains.disclosure.domain.entity.collection_job_item import CollectionJobItem
from app.domains.disclosure.infrastructure.orm.collection_job_orm import CollectionJobOrm
from app.domains.disclosure.infrastructure.orm.collection_job_item_orm import CollectionJobItemOrm


class CollectionJobMapper:

    @staticmethod
    def to_entity(orm: CollectionJobOrm) -> CollectionJob:
        return CollectionJob(
            job_id=orm.id,
            job_name=orm.job_name,
            job_type=orm.job_type,
            started_at=orm.started_at,
            finished_at=orm.finished_at,
            status=orm.status,
            collected_count=orm.collected_count,
            saved_count=orm.saved_count,
            message=orm.message,
            created_at=orm.created_at,
        )

    @staticmethod
    def to_orm(entity: CollectionJob) -> CollectionJobOrm:
        return CollectionJobOrm(
            job_name=entity.job_name,
            job_type=entity.job_type,
            started_at=entity.started_at,
            finished_at=entity.finished_at,
            status=entity.status,
            collected_count=entity.collected_count,
            saved_count=entity.saved_count,
            message=entity.message,
            created_at=entity.created_at,
        )


class CollectionJobItemMapper:

    @staticmethod
    def to_entity(orm: CollectionJobItemOrm) -> CollectionJobItem:
        return CollectionJobItem(
            item_id=orm.id,
            job_id=orm.job_id,
            rcept_no=orm.rcept_no,
            corp_code=orm.corp_code,
            status=orm.status,
            message=orm.message,
            created_at=orm.created_at,
        )

    @staticmethod
    def to_orm(entity: CollectionJobItem) -> CollectionJobItemOrm:
        return CollectionJobItemOrm(
            job_id=entity.job_id,
            rcept_no=entity.rcept_no,
            corp_code=entity.corp_code,
            status=entity.status,
            message=entity.message,
            created_at=entity.created_at,
        )
