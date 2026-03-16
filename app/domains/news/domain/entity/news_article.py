class NewsArticle:
    """뉴스 기사 도메인 엔티티 — 순수 Python, 외부 의존성 없음"""

    def __init__(
        self,
        title: str,
        snippet: str,
        source: str,
        published_at: str,
        link: str | None = None,
    ):
        self.title = title
        self.snippet = snippet
        self.source = source
        self.published_at = published_at
        self.link = link
