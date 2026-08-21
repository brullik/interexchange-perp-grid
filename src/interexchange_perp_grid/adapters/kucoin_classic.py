from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]


class ClassicKucoinFuturesExchange(ccxtpro.kucoinfutures):  # type: ignore[misc]
    """Pinned KuCoin Futures Classic transport with symmetric public streams."""

    _MAX_BBO_SYMBOLS = 100
    _MAX_BBO_SUBSCRIPTIONS_PER_SESSION = 400

    def describe(self) -> Any:
        return self.deep_extend(
            super().describe(),
            {
                "has": {
                    "unWatchBidsAsks": True,
                    "watchPositions": True,
                }
            },
        )

    async def is_uta_enabled(self) -> bool:
        return False

    async def watch_bids_asks(
        self,
        symbols: list[str] | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        params = {} if params is None else dict(params)
        await self.load_markets()
        selected = self.market_symbols(symbols, None, False, True, False)
        if not selected:
            raise ValueError("KuCoin Futures BBO subscription requires symbols")
        if len(selected) > self._MAX_BBO_SUBSCRIPTIONS_PER_SESSION:
            raise ValueError("KuCoin Futures BBO universe exceeds 400 subscriptions per session")
        if params:
            raise ValueError("unqualified KuCoin Futures BBO parameters")
        base_watch = super().watch_bids_asks
        results = await asyncio.gather(
            *(
                base_watch(list(selected[offset : offset + self._MAX_BBO_SYMBOLS]), {})
                for offset in range(0, len(selected), self._MAX_BBO_SYMBOLS)
            )
        )
        merged: dict[str, object] = {}
        for result in results:
            if not isinstance(result, Mapping):
                raise TypeError("KuCoin Futures watch_bids_asks must return a mapping")
            merged.update(result)
        return merged

    async def un_watch_bids_asks(
        self,
        symbols: list[str] | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        params = {} if params is None else dict(params)
        await self.load_markets()
        selected = self.market_symbols(symbols, None, False, True, False)
        if not selected:
            raise ValueError("KuCoin Futures BBO unsubscribe requires symbols")
        if params:
            raise ValueError("unqualified KuCoin Futures BBO parameters")
        url = await self.negotiate(False, True)
        operations = []
        for offset in range(0, len(selected), self._MAX_BBO_SYMBOLS):
            batch = list(selected[offset : offset + self._MAX_BBO_SYMBOLS])
            market_ids = self.market_ids(batch)
            topic = "/contractMarket/tickerV2:" + ",".join(market_ids)
            message_hashes = [f"unsubscribe:bidask@{symbol}" for symbol in batch]
            subscribed_hashes = [f"bidask@{symbol}" for symbol in batch]
            subscription = {
                "messageHashes": message_hashes,
                "subMessageHashes": subscribed_hashes,
                "symbols": batch,
                "topic": "bidsasks",
                "unsubscribe": True,
            }
            operations.append(
                self.un_subscribe_multiple(
                    url,
                    message_hashes,
                    topic,
                    message_hashes,
                    {},
                    subscription,
                )
            )
        return await asyncio.gather(*operations)

    async def watch_positions(
        self,
        symbols: list[str] | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        if symbols:
            raise ValueError("KuCoin Futures Classic position stream is account-wide")
        if since is not None or limit is not None:
            raise ValueError("KuCoin Futures Classic position stream does not support cursors")
        params = {} if params is None else dict(params)
        uta, params = self.handle_option_and_params(params, "watchPositions", "uta", False)
        if uta or params:
            raise ValueError("KuCoin Futures UTA position stream is not qualified")
        await self.load_markets()
        url = await self.negotiate(True, True)
        return await self.subscribe(
            url,
            "positions",
            "/contract/positionAll",
            {"privateChannel": True},
        )

    def handle_position(self, client: Any, message: Any) -> None:
        if isinstance(message, Mapping) and message.get("topic") == "/contract/positionAll":
            raw = message.get("data")
            if not isinstance(raw, Mapping):
                return
            position = self.parse_position(dict(raw))
            client.resolve([position], "positions")
            return
        super().handle_position(client, message)
