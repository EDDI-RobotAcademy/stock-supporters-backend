import json
import logging
from typing import Optional

import redis.asyncio as aioredis

from app.domains.disclosure.application.port.analysis_cache_port import AnalysisCachePort
from app.domains.disclosure.domain.value_object.cache_key import CacheKey

logger = logging.getLogger(__name__)


class AnalysisCacheAdapter(AnalysisCachePort):
    """Redis를 사용한 분석 결과 캐시 어댑터"""

    DEFAULT_TTL_SECONDS = 3600

    def __init__(self, redis: aioredis.Redis):
        self._redis = redis

    async def get(self, ticker: str, analysis_type: str) -> Optional[dict]:
        """캐시된 분석 결과를 조회한다. Redis 오류 시 None을 반환한다."""
        try:
            key = CacheKey.generate(ticker, analysis_type)
            data = await self._redis.get(key)
            if data is None:
                return None
            return json.loads(data)
        except (aioredis.RedisError, json.JSONDecodeError) as e:
            logger.warning(f"캐시 조회 실패 [ticker={ticker}, type={analysis_type}]: {e}")
            return None

    async def save(
        self, ticker: str, analysis_type: str, result: dict, ttl_seconds: int = 3600
    ) -> None:
        """분석 결과를 TTL과 함께 캐시에 저장한다. Redis 오류 시 로그만 남긴다."""
        try:
            key = CacheKey.generate(ticker, analysis_type)
            serialized = json.dumps(result, ensure_ascii=False, default=str)
            await self._redis.setex(key, ttl_seconds, serialized)
        except (aioredis.RedisError, TypeError, ValueError) as e:
            logger.error(f"캐시 저장 실패 [ticker={ticker}, type={analysis_type}]: {e}")

    async def delete(self, ticker: str, analysis_type: str) -> bool:
        """캐시된 분석 결과를 삭제한다. 삭제 성공 시 True를 반환한다."""
        try:
            key = CacheKey.generate(ticker, analysis_type)
            deleted_count = await self._redis.delete(key)
            return deleted_count > 0
        except aioredis.RedisError as e:
            logger.error(f"캐시 삭제 실패 [ticker={ticker}, type={analysis_type}]: {e}")
            return False

    async def exists(self, ticker: str, analysis_type: str) -> bool:
        """캐시에 분석 결과가 존재하는지 확인한다."""
        try:
            key = CacheKey.generate(ticker, analysis_type)
            return await self._redis.exists(key) > 0
        except aioredis.RedisError as e:
            logger.warning(f"캐시 존재 확인 실패 [ticker={ticker}, type={analysis_type}]: {e}")
            return False
