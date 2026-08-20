from __future__ import annotations

from typing import Any

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]


class SequenceQualifiedMexcExchange(ccxtpro.mexc):  # type: ignore[misc]
    """Pinned MEXC linear-swap transport with exact incremental-depth continuity."""

    async def un_watch_tickers(
        self,
        symbols: list[str] | None = None,
        params: dict[str, object] | None = None,
    ) -> Any:
        """Await the official all-contract unsubscribe acknowledgement.

        CCXT 4.5.58 constructs the correct ``unsub.tickers`` frame but drops its
        ``watch_multiple`` coroutine instead of awaiting it.
        """
        params = {} if params is None else dict(params)
        await self.load_markets()
        qualified = self.market_symbols(symbols, None)
        first_symbol = self.safe_string(qualified, 0)
        market = self.market(first_symbol) if first_symbol is not None else None
        market_type, params = self.handle_market_type_and_params("watchTickers", market, params)
        if market_type != "swap":
            raise ValueError("MEXC batch ticker unsubscribe supports swaps only")
        url = self.urls["api"]["ws"]["swap"]
        message_hashes = ["unsubscribe:ticker"]
        request = self.extend({"method": "unsub.tickers", "params": {}}, params)
        await self.watch_multiple(url, message_hashes, request, message_hashes)
        self.handle_unsubscriptions(self.client(url), message_hashes)
        return None

    def handle_delta(self, orderbook: Any, delta: Any) -> None:
        previous = self.safe_integer(orderbook, "nonce")
        sequence = self.safe_integer_n(delta, ["version", "r", "fromVersion"])
        if orderbook.get("ipegSequenceDesynced") is True:
            orderbook["ipegSequenceReset"] = False
            orderbook["ipegSequenceContiguous"] = False
            return
        if previous is None or sequence is None:
            orderbook["ipegSequenceReset"] = False
            orderbook["ipegSequenceContiguous"] = False
            orderbook["ipegSequenceDesynced"] = True
            return
        if sequence <= previous:
            return
        contiguous = sequence == previous + 1
        orderbook["ipegSequenceReset"] = False
        orderbook["ipegSequenceContiguous"] = contiguous
        if not contiguous:
            orderbook["ipegSequenceDesynced"] = True
            return
        super().handle_delta(orderbook, delta)
