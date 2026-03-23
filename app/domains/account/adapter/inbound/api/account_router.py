import logging
from typing import Optional
from urllib.parse import quote

import redis.asyncio as aioredis
from fastapi import APIRouter, Cookie, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.exception.app_exception import AppException
from app.domains.account.adapter.outbound.cache.account_token_cache_impl import AccountTokenCacheImpl
from app.domains.account.adapter.outbound.cache.temp_token_cache_impl import TempTokenCacheImpl
from app.domains.account.adapter.outbound.persistence.account_save_repository_impl import AccountSaveRepositoryImpl
from app.domains.account.application.request.create_account_request import CreateAccountRequest
from app.domains.account.application.usecase.create_account_usecase import CreateAccountUseCase
from app.infrastructure.cache.redis_client import get_redis
from app.infrastructure.config.settings import get_settings
from app.infrastructure.database.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/account", tags=["account"])

settings = get_settings()



@router.post("/sign-up")
async def sign_up(
    request: CreateAccountRequest,
    temp_token: Optional[str] = Cookie(default=None),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    if not temp_token:
        raise AppException(status_code=401, message="임시 토큰이 없습니다.")

    result = await CreateAccountUseCase(
        account_save_port=AccountSaveRepositoryImpl(db),
        temp_token_port=TempTokenCacheImpl(redis),
        account_token_cache_port=AccountTokenCacheImpl(redis, settings.session_ttl_seconds),
    ).execute(
        nickname=request.nickname,
        email=request.email,
        temp_token_value=temp_token,
    )

    response = RedirectResponse(url=settings.cors_allowed_frontend_url, status_code=302)
    response.set_cookie(
        key="user_token",
        value=result.user_token,
        httponly=True,
        path="/",
        max_age=settings.session_ttl_seconds,
    )
    response.set_cookie(
        key="nickname",
        value=quote(result.nickname or ""),
        httponly=True,
        path="/",
        max_age=settings.session_ttl_seconds,
    )
    response.set_cookie(
        key="email",
        value=result.email,
        httponly=True,
        path="/",
        max_age=settings.session_ttl_seconds,
    )
    response.delete_cookie(key="temp_token")
    return response
