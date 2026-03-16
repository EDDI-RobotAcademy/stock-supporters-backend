from fastapi import APIRouter, Query

from app.common.response.base_response import BaseResponse
from app.domains.news.adapter.outbound.external.serp_news_search_provider import (
    SerpNewsSearchProvider,
)
from app.domains.news.application.request.search_news_request import SearchNewsRequest
from app.domains.news.application.response.search_news_response import (
    SearchNewsResponse,
)
from app.domains.news.application.usecase.search_news_usecase import SearchNewsUseCase
from app.infrastructure.config.settings import get_settings

router = APIRouter(prefix="/news", tags=["News"])


@router.get("/search", response_model=BaseResponse[SearchNewsResponse])
async def search_news(
    keyword: str = Query(..., min_length=1, description="검색 키워드"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    page_size: int = Query(10, ge=1, le=100, description="페이지 크기"),
):
    """인증 없이 뉴스를 검색하고 페이징된 결과를 반환한다."""
    settings = get_settings()
    provider = SerpNewsSearchProvider(api_key=settings.serp_api_key)
    usecase = SearchNewsUseCase(news_search_provider=provider)

    request = SearchNewsRequest(keyword=keyword, page=page, page_size=page_size)
    result = await usecase.execute(request)

    return BaseResponse.ok(data=result)
