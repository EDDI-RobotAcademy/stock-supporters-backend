import logging
from datetime import datetime

from app.domains.disclosure.application.port.dart_document_api_port import DartDocumentApiPort
from app.domains.disclosure.application.port.disclosure_document_repository_port import (
    DisclosureDocumentRepositoryPort,
)
from app.domains.disclosure.application.port.disclosure_repository_port import (
    DisclosureRepositoryPort,
)
from app.domains.disclosure.application.response.store_document_response import (
    BatchStoreDocumentResponse,
)
from app.domains.disclosure.domain.entity.disclosure_document import DisclosureDocument
from app.domains.disclosure.domain.service.disclosure_document_parser import (
    DisclosureDocumentParser,
)

logger = logging.getLogger(__name__)


class BatchStoreDocumentsUseCase:
    def __init__(
        self,
        dart_document_api: DartDocumentApiPort,
        disclosure_document_repository: DisclosureDocumentRepositoryPort,
        disclosure_repository: DisclosureRepositoryPort,
    ):
        self._dart_doc_api = dart_document_api
        self._doc_repo = disclosure_document_repository
        self._disclosure_repo = disclosure_repository

    async def execute(
        self,
        document_type: str = "core_document",
        limit: int = 50,
    ) -> BatchStoreDocumentResponse:
        """문서가 아직 저장되지 않은 공시들의 원문을 일괄 수집한다.

        RAG에 저장되지 않은 문서 중 raw_text가 없는 것들을 대상으로 한다.
        """
        # 아직 저장되지 않은 문서 조회
        not_stored = await self._doc_repo.find_not_stored_in_rag(limit=limit)

        # raw_text가 없는 문서만 필터
        targets = [doc for doc in not_stored if not doc.raw_text]

        if not targets:
            logger.info("수집 대상 문서가 없습니다.")
            return BatchStoreDocumentResponse(
                total_target=0,
                success_count=0,
                fail_count=0,
                message="수집 대상 문서가 없습니다.",
            )

        parser = DisclosureDocumentParser()
        success_count = 0
        fail_count = 0

        for target in targets:
            try:
                raw_text = await self._dart_doc_api.fetch_document(target.rcept_no)

                parsed_json = parser.parse(raw_text)
                summary_text = parser.generate_summary(raw_text)

                document = DisclosureDocument(
                    rcept_no=target.rcept_no,
                    document_type=target.document_type,
                    raw_text=raw_text,
                    parsed_json=parsed_json,
                    summary_text=summary_text,
                    stored_in_rag=False,
                    collected_at=datetime.now(),
                )

                await self._doc_repo.upsert(document)
                success_count += 1

                logger.info(
                    "배치 문서 저장 성공: rcept_no=%s, type=%s",
                    target.rcept_no,
                    target.document_type,
                )

            except Exception as e:
                fail_count += 1
                logger.error(
                    "배치 문서 저장 실패: rcept_no=%s, error=%s",
                    target.rcept_no,
                    str(e),
                )

        message = f"배치 문서 수집 완료: 대상 {len(targets)}건, 성공 {success_count}건, 실패 {fail_count}건"
        logger.info(message)

        return BatchStoreDocumentResponse(
            total_target=len(targets),
            success_count=success_count,
            fail_count=fail_count,
            message=message,
        )
