from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class KrxMarketCapInfo:
    stock_code: str
    corp_name: str
    market_cap: int
    rank: int


class KrxMarketCapPort(ABC):

    @abstractmethod
    async def fetch_top_by_market_cap(self, count: int = 300) -> list[KrxMarketCapInfo]:
        pass
