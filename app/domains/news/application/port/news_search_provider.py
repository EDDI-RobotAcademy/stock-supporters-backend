from abc import ABC, abstractmethod

from app.domains.news.domain.entity.news_article import NewsArticle


class NewsSearchResult:
    """뉴스 검색 결과를 담는 값 객체"""

    def __init__(self, articles: list[NewsArticle], total_count: int):
        self.articles = articles
        self.total_count = total_count


class NewsSearchProvider(ABC):
    """외부 뉴스 검색 서비스에 대한 포트"""

    @abstractmethod
    async def search(
        self, keyword: str, page: int, page_size: int
    ) -> NewsSearchResult:
        pass
