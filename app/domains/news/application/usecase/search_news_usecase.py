from app.domains.news.application.port.news_search_provider import NewsSearchProvider
from app.domains.news.application.request.search_news_request import SearchNewsRequest
from app.domains.news.application.response.search_news_response import (
    NewsArticleResponse,
    SearchNewsResponse,
)


class SearchNewsUseCase:
    def __init__(self, news_search_provider: NewsSearchProvider):
        self._provider = news_search_provider

    async def execute(self, request: SearchNewsRequest) -> SearchNewsResponse:
        result = await self._provider.search(
            keyword=request.keyword,
            page=request.page,
            page_size=request.page_size,
        )

        articles = [
            NewsArticleResponse(
                title=article.title,
                snippet=article.snippet,
                source=article.source,
                published_at=article.published_at,
                link=article.link,
            )
            for article in result.articles
        ]

        return SearchNewsResponse(
            articles=articles,
            total_count=result.total_count,
            page=request.page,
            page_size=request.page_size,
        )
