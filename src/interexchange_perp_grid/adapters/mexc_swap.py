from __future__ import annotations

from typing import Any

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]


class SequenceQualifiedMexcExchange(ccxtpro.mexc):  # type: ignore[misc]
    """Pinned MEXC linear-swap transport with exact incremental-depth continuity."""

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
