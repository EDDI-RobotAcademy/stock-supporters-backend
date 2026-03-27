import logging
from datetime import datetime

from app.domains.disclosure.application.port.disclosure_document_repository_port import (
    DisclosureDocumentRepositoryPort,
)
from app.domains.disclosure.application.port.disclosure_repository_port import (
    DisclosureRepositoryPort,
)
from app.domains.disclosure.application.port.embedding_port import EmbeddingPort
from app.domains.disclosure.application.port.rag_chunk_repository_port import (
    RagChunkRepositoryPort,
)
from app.domains.disclosure.application.response.rag_store_response import (
    RagStoreResponse,
)
from app.domains.disclosure.domain.entity.rag_document_chunk import RagDocumentChunk
from app.domains.disclosure.domain.service.text_chunker import TextChunker

logger = logging.getLogger(__name__)

MIN_TEXT_LENGTH = 200


class StoreRagChunksUseCase:
    def __init__(
        self,
        disclosure_document_repository: DisclosureDocumentRepositoryPort,
        disclosure_repository: DisclosureRepositoryPort,
        rag_chunk_repository: RagChunkRepositoryPort,
        embedding_port: EmbeddingPort,
    ):
        self._doc_repo = disclosure_document_repository
        self._disclosure_repo = disclosure_repository
        self._rag_repo = rag_chunk_repository
        self._embedding = embedding_port
        self._chunker = TextChunker()

    async def execute(self, limit: int = 50) -> RagStoreResponse:
        """RAG에 저장되지 않은 공시 문서를 청크로 분할하고 임베딩과 함께 저장한다."""
        # 1. RAG 미저장 문서 조회
        documents = await self._doc_repo.find_not_stored_in_rag(limit=limit)

        if not documents:
            return RagStoreResponse(
                total_documents=0,
                processed_documents=0,
                total_chunks_stored=0,
                skipped_documents=0,
                message="RAG에 저장할 대상 문서가 없습니다.",
            )

        total_documents = len(documents)
        processed_count = 0
        skipped_count = 0
        total_chunks = 0

        for doc in documents:
            # 2. 서술형 공시 필터링 (raw_text 길이가 200자 미만이면 건너뜀)
            if not doc.raw_text or len(doc.raw_text) < MIN_TEXT_LENGTH:
                skipped_count += 1
                # 텍스트가 너무 짧아도 stored_in_rag 플래그를 세워서 재처리 방지
                doc.stored_in_rag = True
                doc.updated_at = datetime.now()
                await self._doc_repo.upsert(doc)
                logger.info(
                    "텍스트가 짧아 RAG 저장 건너뜀: rcept_no=%s, length=%d",
                    doc.rcept_no,
                    len(doc.raw_text) if doc.raw_text else 0,
                )
                continue

            try:
                # 3. 공시 정보 조회 (메타데이터 확보)
                disclosure = await self._disclosure_repo.find_by_rcept_no(doc.rcept_no)
                if not disclosure:
                    skipped_count += 1
                    logger.warning(
                        "공시 정보를 찾을 수 없습니다: rcept_no=%s", doc.rcept_no
                    )
                    continue

                # 4. 텍스트 청킹
                chunk_dicts = self._chunker.chunk_text(doc.raw_text)
                if not chunk_dicts:
                    skipped_count += 1
                    doc.stored_in_rag = True
                    doc.updated_at = datetime.now()
                    await self._doc_repo.upsert(doc)
                    continue

                # 5. 임베딩 생성
                chunk_texts = [c["chunk_text"] for c in chunk_dicts]
                embeddings = await self._embedding.generate_batch(chunk_texts)

                # 6. RagDocumentChunk 엔티티 생성
                chunks = []
                for chunk_dict, embedding in zip(chunk_dicts, embeddings):
                    chunk = RagDocumentChunk(
                        rcept_no=doc.rcept_no,
                        corp_code=disclosure.corp_code,
                        disclosure_document_id=doc.document_id,
                        report_nm=disclosure.report_nm,
                        document_type=doc.document_type,
                        section_title=chunk_dict["section_title"],
                        chunk_index=chunk_dict["chunk_index"],
                        chunk_text=chunk_dict["chunk_text"],
                        chunk_hash=chunk_dict["chunk_hash"],
                        embedding=embedding,
                    )
                    chunks.append(chunk)

                # 7. 벡터 DB에 저장
                stored = await self._rag_repo.upsert_bulk(chunks)
                total_chunks += stored

                # 8. stored_in_rag 플래그 업데이트
                doc.stored_in_rag = True
                doc.updated_at = datetime.now()
                await self._doc_repo.upsert(doc)

                processed_count += 1
                logger.info(
                    "RAG 저장 완료: rcept_no=%s, chunks=%d",
                    doc.rcept_no,
                    stored,
                )

            except Exception as e:
                skipped_count += 1
                logger.error(
                    "RAG 저장 실패: rcept_no=%s, error=%s",
                    doc.rcept_no,
                    str(e),
                )
                continue

        return RagStoreResponse(
            total_documents=total_documents,
            processed_documents=processed_count,
            total_chunks_stored=total_chunks,
            skipped_documents=skipped_count,
            message=f"RAG 저장 완료: {processed_count}/{total_documents}개 문서 처리, "
            f"{total_chunks}개 청크 저장",
        )
