from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]


class SequenceQualifiedBingxExchange(ccxtpro.bingx):  # type: ignore[misc]
    """Pinned BingX USDT-swap transport with qualified sequenced L2.

    CCXT 4.5.58's limited linear depth snapshots do not carry ``lastUpdateId``.
    BingX's official ``incrDepth`` topic provides snapshot/update sequence
    evidence with symmetric ``sub``/``unsub`` frames. Official ``bookTicker``
    is per-symbol only, so broad BBO remains capability-gated instead of being
    disguised as the forbidden unbounded per-symbol fallback.
    """

    async def watch_order_book(
        self,
        symbol: str,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        params = {} if params is None else dict(params)
        await self.load_markets()
        market = self.market(symbol)
        if not market["swap"] or not market["linear"]:
            raise ValueError("BingX sequenced L2 supports linear swaps only")
        if params:
            raise ValueError("unqualified BingX sequenced L2 parameters")
        data_type = market["id"] + "@incrDepth"
        message_hash = self.get_message_hash("orderbook", market["symbol"])
        request = {
            "id": self.uuid(),
            "reqType": "sub",
            "dataType": data_type,
        }
        subscription = {
            "id": request["id"],
            "unsubscribe": False,
            "limit": limit,
        }
        url = self.urls["api"]["ws"]["linear"]
        order_book = await self.watch(url, message_hash, request, data_type, subscription)
        return order_book.limit() if hasattr(order_book, "limit") else order_book

    async def un_watch_order_book(
        self,
        symbol: str,
        params: dict[str, object] | None = None,
    ) -> Any:
        params = {} if params is None else dict(params)
        if params:
            raise ValueError("unqualified BingX sequenced L2 parameters")
        await self.load_markets()
        market = self.market(symbol)
        data_type = market["id"] + "@incrDepth"
        subscribed_hash = self.get_message_hash("orderbook", market["symbol"])
        message_hash = "unsubscribe::" + subscribed_hash
        return await self.un_watch(
            message_hash,
            subscribed_hash,
            message_hash,
            data_type,
            "orderbook",
            market,
            "unWatchOrderBook",
            {},
        )

    def handle_message(self, client: Any, message: Any) -> None:
        if isinstance(message, Mapping):
            data_type = str(message.get("dataType", ""))
            if data_type.endswith("@incrDepth"):
                self._handle_incremental_order_book(client, message)
                return
        super().handle_message(client, message)

    def _handle_incremental_order_book(
        self,
        client: Any,
        message: Mapping[str, object],
    ) -> None:
        data = message.get("data")
        if not isinstance(data, Mapping):
            return
        data_type = str(message.get("dataType", ""))
        market_id = str(data.get("s") or data_type.split("@", 1)[0])
        market = self.safe_market(market_id, None, "-", "swap")
        symbol = market["symbol"]
        sequence = self.safe_integer(data, "lastUpdateId")
        action = str(data.get("action") or message.get("action") or "").lower()
        if sequence is None or action not in {"all", "update"}:
            return
        order_book = self.orderbooks.get(symbol)
        if order_book is None:
            subscription = client.subscriptions.get(data_type, {})
            depth = self.safe_integer(subscription, "limit", 50)
            order_book = self.order_book({}, depth)
            self.orderbooks[symbol] = order_book
        timestamp = self.safe_integer_2(data, "T", "ts")
        if timestamp is None:
            timestamp = self.safe_integer_2(message, "T", "ts")
        if action == "all":
            snapshot = self.parse_order_book(data, symbol, timestamp, "bids", "asks", 0, 1)
            snapshot["nonce"] = sequence
            order_book.reset(snapshot)
            order_book["ipegSequenceReset"] = True
            order_book["ipegSequenceContiguous"] = True
            order_book["ipegSequenceDesynced"] = False
        else:
            previous = order_book.get("nonce")
            desynced = order_book.get("ipegSequenceDesynced") is True
            contiguous = not desynced and isinstance(previous, int) and sequence == previous + 1
            order_book["ipegSequenceReset"] = False
            order_book["ipegSequenceContiguous"] = contiguous
            order_book["timestamp"] = timestamp
            order_book["datetime"] = self.iso8601(timestamp)
            if contiguous:
                order_book["nonce"] = sequence
                for delta in data.get("bids", []):
                    self.handle_delta(order_book["bids"], delta)
                for delta in data.get("asks", []):
                    self.handle_delta(order_book["asks"], delta)
            else:
                order_book["ipegSequenceDesynced"] = True
        client.resolve(order_book, self.get_message_hash("orderbook", symbol))
