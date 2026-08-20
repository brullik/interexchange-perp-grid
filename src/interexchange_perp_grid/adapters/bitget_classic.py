from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]


class ClassicBitgetExchange(ccxtpro.bitget):  # type: ignore[misc]
    """Pinned Bitget Classic transport with a matching batch ticker unsubscribe.

    CCXT 4.5.58 implements the Classic ticker subscription but leaves both batch
    ticker unsubscribe methods as NotSupported stubs.  Bitget's Classic protocol
    accepts the same topic array with ``op=unsubscribe``; this narrow override adds
    that missing half without opting into UTA.
    """

    _MAX_CLASSIC_FRAME_BYTES = 4096

    async def _ticker_batches(
        self,
        symbols: list[str] | None = None,
        params: dict[str, object] | None = None,
        *,
        operation: str,
    ) -> tuple[tuple[str, ...], ...]:
        params = {} if params is None else dict(params)
        await self.load_markets()
        symbols = self.market_symbols(symbols, None, False)
        if not symbols:
            raise ValueError("Bitget Classic ticker operation requires symbols")
        uta, params = self.handle_option_and_params(
            params,
            "watchTickers",
            "uta",
            False,
        )
        if uta:
            raise ValueError("Bitget UTA transport is not qualified")
        market = self.market(symbols[0])
        inst_type, params = self.get_inst_type("watchTickers", market, False, params)
        if params:
            raise ValueError("unqualified Bitget Classic ticker parameters")
        batches: list[tuple[str, ...]] = []
        current_symbols: list[str] = []
        current_topics: list[dict[str, object]] = []
        for symbol in symbols:
            current = self.market(symbol)
            topic = {
                "instType": inst_type,
                "channel": "ticker",
                "instId": current["id"],
            }
            candidate_topics = [*current_topics, topic]
            frame = json.dumps(
                {"op": operation, "args": candidate_topics},
                separators=(",", ":"),
            ).encode()
            if len(frame) > self._MAX_CLASSIC_FRAME_BYTES:
                if not current_symbols:
                    raise ValueError("one Bitget Classic ticker topic exceeds frame limit")
                batches.append(tuple(current_symbols))
                current_symbols = []
                current_topics = []
                frame = json.dumps(
                    {"op": operation, "args": [topic]},
                    separators=(",", ":"),
                ).encode()
                if len(frame) > self._MAX_CLASSIC_FRAME_BYTES:
                    raise ValueError("one Bitget Classic ticker topic exceeds frame limit")
            current_symbols.append(current["symbol"])
            current_topics.append(topic)
        batches.append(tuple(current_symbols))
        return tuple(batches)

    async def watch_tickers(
        self,
        symbols: list[str] | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        batches = await self._ticker_batches(symbols, params, operation="subscribe")
        base_watch_tickers = super().watch_tickers
        results = await asyncio.gather(*(base_watch_tickers(list(batch), {}) for batch in batches))
        merged: dict[str, object] = {}
        for result in results:
            if not isinstance(result, Mapping):
                raise TypeError("Bitget Classic watch_tickers must return a mapping")
            merged.update(result)
        return merged

    async def un_watch_tickers(
        self,
        symbols: list[str] | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        batches = await self._ticker_batches(symbols, params, operation="unsubscribe")
        url = self.urls["api"]["ws"]["public"]
        operations = []
        for batch in batches:
            market = self.market(batch[0])
            inst_type, remaining = self.get_inst_type("watchTickers", market, False, {})
            if remaining:
                raise ValueError("unqualified Bitget Classic ticker parameters")
            topics = [
                {
                    "instType": inst_type,
                    "channel": "ticker",
                    "instId": self.market(symbol)["id"],
                }
                for symbol in batch
            ]
            message_hashes = [f"unsubscribe:ticker:{symbol}" for symbol in batch]
            operations.append(
                self.watch_multiple(
                    url,
                    message_hashes,
                    {"op": "unsubscribe", "args": topics},
                    message_hashes,
                )
            )
        return await asyncio.gather(*operations)

    def handle_message(self, client: Any, message: Any) -> None:
        if isinstance(message, Mapping) and message.get("op") == "unsubscribe":
            self.handle_un_subscription_status(client, message)
            return
        super().handle_message(client, message)

    def handle_un_subscription_status(self, client: Any, message: Any) -> Any:
        """Route each Classic batch acknowledgement with its own ``arg`` payload."""
        args = self.safe_list(message, "args")
        if args is None:
            return super().handle_un_subscription_status(client, message)
        for arg in args:
            if not isinstance(arg, dict):
                continue
            item = dict(message)
            item["arg"] = arg
            channel = self.safe_string(arg, "channel", "")
            if channel == "ticker":
                self.handle_ticker_un_subscription(client, item)
            elif channel.startswith("books"):
                self.handle_order_book_un_subscription(client, item)
        return message

    def handle_order_book(self, client: Any, message: Any) -> None:
        super().handle_order_book(client, message)
        if not isinstance(message, Mapping):
            return
        arg = message.get("arg")
        data = message.get("data")
        if not isinstance(arg, Mapping) or not isinstance(data, list) or not data:
            return
        raw = data[0]
        if not isinstance(raw, Mapping):
            return
        sequence = self.safe_integer(raw, "seq")
        market_id = self.safe_string(arg, "instId")
        if sequence is None or market_id is None:
            return
        market = self.safe_market(market_id, None, None, "contract")
        order_book = self.orderbooks.get(market["symbol"])
        if order_book is not None:
            order_book["nonce"] = sequence
            order_book["ipegSequenceReset"] = False
            order_book["ipegSequenceContiguous"] = True
