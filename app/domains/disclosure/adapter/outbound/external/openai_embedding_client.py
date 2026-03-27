import logging

from openai import AsyncOpenAI

from app.domains.disclosure.application.port.embedding_port import EmbeddingPort
from app.infrastructure.config.settings import get_settings

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536
BATCH_SIZE = 100


class OpenAIEmbeddingClient(EmbeddingPort):

    def __init__(self):
        settings = get_settings()
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)

    async def generate(self, text: str) -> list[float]:
        """단일 텍스트의 임베딩을 생성한다."""
        response = await self._client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text,
            dimensions=EMBEDDING_DIMENSIONS,
        )
        return response.data[0].embedding

    async def generate_batch(self, texts: list[str]) -> list[list[float]]:
        """여러 텍스트의 임베딩을 배치로 생성한다.

        OpenAI API의 입력 제한을 고려하여 BATCH_SIZE 단위로 나누어 호출한다.
        """
        if not texts:
            return []

        all_embeddings: list[list[float]] = []

        for i in range(0, len(texts), BATCH_SIZE):
            batch = texts[i : i + BATCH_SIZE]
            response = await self._client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=batch,
                dimensions=EMBEDDING_DIMENSIONS,
            )
            # API 응답의 index 순서로 정렬하여 입력 순서 보장
            sorted_data = sorted(response.data, key=lambda x: x.index)
            all_embeddings.extend([item.embedding for item in sorted_data])

        logger.info("임베딩 생성 완료: %d개 텍스트", len(texts))
        return all_embeddings
