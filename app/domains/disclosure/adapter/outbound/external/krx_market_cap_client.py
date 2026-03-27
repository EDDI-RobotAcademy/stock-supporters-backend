import asyncio
import logging
from datetime import datetime, timedelta
from functools import partial

from pykrx import stock as pykrx_stock

from app.domains.disclosure.application.port.krx_market_cap_port import (
    KrxMarketCapPort,
    KrxMarketCapInfo,
)

logger = logging.getLogger(__name__)


class KrxMarketCapClient(KrxMarketCapPort):

    async def fetch_top_by_market_cap(self, count: int = 300) -> list[KrxMarketCapInfo]:
        target_date = await self._find_latest_trading_date()

        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(
            None,
            partial(pykrx_stock.get_market_cap_by_ticker, target_date),
        )

        if df.empty:
            logger.warning("KRX 시가총액 데이터가 비어있습니다. (날짜: %s)", target_date)
            return []

        df = df.sort_values("시가총액", ascending=False).head(count)

        result: list[KrxMarketCapInfo] = []
        for rank, (ticker, row) in enumerate(df.iterrows(), start=1):
            result.append(
                KrxMarketCapInfo(
                    stock_code=str(ticker),
                    corp_name=str(pykrx_stock.get_market_ticker_name(ticker)),
                    market_cap=int(row["시가총액"]),
                    rank=rank,
                )
            )

        logger.info(
            "KRX 시가총액 상위 %d개 기업 수집 완료 (날짜: %s)", len(result), target_date
        )
        return result

    async def _find_latest_trading_date(self) -> str:
        loop = asyncio.get_running_loop()
        today = datetime.now()

        for days_back in range(7):
            candidate = (today - timedelta(days=days_back)).strftime("%Y%m%d")
            df = await loop.run_in_executor(
                None,
                partial(pykrx_stock.get_market_cap_by_ticker, candidate),
            )
            if not df.empty:
                logger.info("최근 거래일: %s", candidate)
                return candidate

        return today.strftime("%Y%m%d")
