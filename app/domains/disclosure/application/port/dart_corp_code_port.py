from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class DartCorpInfo:
    corp_code: str
    corp_name: str
    stock_code: str
    modify_date: str


class DartCorpCodePort(ABC):

    @abstractmethod
    async def fetch_all_corp_codes(self) -> list[DartCorpInfo]:
        pass
