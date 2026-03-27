import logging
from datetime import datetime

from app.common.exception.app_exception import AppException
from app.domains.disclosure.application.port.dart_document_api_port import DartDocumentApiPort
from app.domains.disclosure.application.port.disclosure_document_repository_port import (
    DisclosureDocumentRepositoryPort,
)
from app.domains.disclosure.application.port.disclosure_repository_port import (
    DisclosureRepositoryPort,
)
from app.domains.disclosure.application.request.store_document_request import (
    StoreDocumentRequest,
)
from app.domains.disclosure.application.response.store_document_response import (
    StoreDocumentResponse,
)
from app.domains.disclosure.domain.entity.disclosure_document import DisclosureDocument
from app.domains.disclosure.domain.service.disclosure_document_parser import (
    DisclosureDocumentParser,
)

logger = logging.getLogger(__name__)


class StoreDisclosureDocumentUseCase:
    def __init__(
        self,
        dart_document_api: DartDocumentApiPort,
        disclosure_document_repository: DisclosureDocumentRepositoryPort,
        disclosure_repository: DisclosureRepositoryPort,
    ):
        self._dart_doc_api = dart_document_api
        self._doc_repo = disclosure_document_repository
        self._disclosure_repo = disclosure_repository

    async def execute(self, request: StoreDocumentRequest) -> StoreDocumentResponse:
        # 공시 존재 여부 확인
        exists = await self._disclosure_repo.exists_by_rcept_no(request.rcept_no)
        if not exists:
            raise AppException(
                status_code=404,
                message=f"접수번호 '{request.rcept_no}'에 해당하는 공시가 존재하지 않습니다.",
            )

        # 이미 저장된 문서가 있는지 확인
        existing = await self._doc_repo.find_by_rcept_no_and_type(
            request.rcept_no, request.document_type
        )
        if existing and existing.raw_text:
            logger.info(
                "이미 저장된 문서가 존재합니다: rcept_no=%s, type=%s",
                request.rcept_no,
                request.document_type,
            )
            return StoreDocumentResponse(
                rcept_no=request.rcept_no,
                document_type=request.document_type,
                stored=False,
                parsed=existing.parsed_json is not None,
                message="이미 저장된 문서가 존재합니다.",
            )

        # DART에서 원문 가져오기
        try:
            raw_text = await self._dart_doc_api.fetch_document(request.rcept_no)
        except RuntimeError as e:
            logger.error(
                "DART 문서 가져오기 실패: rcept_no=%s, error=%s",
                request.rcept_no,
                str(e),
            )
            raise AppException(
                status_code=502,
                message=f"DART 문서 가져오기 실패: {str(e)}",
            )

        # 파싱
        parser = DisclosureDocumentParser()
        parsed_json = parser.parse(raw_text)
        summary_text = parser.generate_summary(raw_text)

        # 저장
        document = DisclosureDocument(
            rcept_no=request.rcept_no,
            document_type=request.document_type,
            raw_text=raw_text,
            parsed_json=parsed_json,
            summary_text=summary_text,
            stored_in_rag=False,
            collected_at=datetime.now(),
        )

        saved = await self._doc_repo.upsert(document)

        logger.info(
            "공시 문서 저장 완료: rcept_no=%s, type=%s, text_length=%d",
            saved.rcept_no,
            saved.document_type,
            len(raw_text),
        )

        return StoreDocumentResponse(
            rcept_no=saved.rcept_no,
            document_type=saved.document_type,
            stored=True,
            parsed=True,
            message=f"문서 저장 및 파싱 완료 (텍스트 길이: {len(raw_text)}자)",
        )
