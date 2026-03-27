import logging
from datetime import date, datetime, timedelta

from app.domains.disclosure.application.port.collection_job_repository_port import (
    CollectionJobRepositoryPort,
)
from app.domains.disclosure.application.port.data_cleanup_port import DataCleanupPort
from app.domains.disclosure.application.request.cleanup_request import CleanupRequest
from app.domains.disclosure.application.response.cleanup_response import CleanupResponse
from app.domains.disclosure.domain.entity.collection_job import CollectionJob

logger = logging.getLogger(__name__)


class CleanupExpiredDataUseCase:
    def __init__(
        self,
        data_cleanup_repository: DataCleanupPort,
        collection_job_repository: CollectionJobRepositoryPort,
    ):
        self._data_cleanup_repository = data_cleanup_repository
        self._collection_job_repository = collection_job_repository

    async def execute(self, request: CleanupRequest) -> CleanupResponse:
        job = CollectionJob(
            job_name="cleanup_expired_data",
            job_type="cleanup",
            started_at=datetime.now(),
            status="running",
        )
        job = await self._collection_job_repository.save_job(job)

        try:
            today = date.today()
            disclosure_cutoff = today - timedelta(days=request.disclosure_retention_days)
            job_cutoff = today - timedelta(days=request.job_retention_days)

            deleted_disclosures = await self._data_cleanup_repository.delete_old_disclosures(
                disclosure_cutoff
            )
            logger.info("만료 공시 %d건 삭제 완료 (기준일: %s)", deleted_disclosures, disclosure_cutoff)

            deleted_jobs = await self._data_cleanup_repository.delete_old_collection_jobs(
                job_cutoff
            )
            logger.info("만료 작업 %d건 삭제 완료 (기준일: %s)", deleted_jobs, job_cutoff)

            deleted_orphaned_chunks = await self._data_cleanup_repository.delete_orphaned_rag_chunks()
            logger.info("고아 RAG 청크 %d건 삭제 완료", deleted_orphaned_chunks)

            total_deleted = deleted_disclosures + deleted_jobs + deleted_orphaned_chunks
            message = (
                f"데이터 정리 완료: 공시 {deleted_disclosures}건, "
                f"작업 {deleted_jobs}건, "
                f"고아 청크 {deleted_orphaned_chunks}건 삭제"
            )

            job.status = "success"
            job.finished_at = datetime.now()
            job.collected_count = total_deleted
            job.saved_count = 0
            job.message = message
            await self._collection_job_repository.update_job(job)

            return CleanupResponse(
                deleted_disclosures=deleted_disclosures,
                deleted_jobs=deleted_jobs,
                deleted_orphaned_chunks=deleted_orphaned_chunks,
                message=message,
            )

        except Exception as e:
            job.status = "failed"
            job.finished_at = datetime.now()
            job.message = f"데이터 정리 실패: {str(e)}"
            await self._collection_job_repository.update_job(job)
            raise
