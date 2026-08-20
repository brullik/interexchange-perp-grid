from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from interexchange_perp_grid.adapters.bingx_swap import SequenceQualifiedBingxExchange
from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter, normalize_market
from interexchange_perp_grid.domain import Venue


def _market() -> dict[str, object]:
    return {
        "id": "BTC-USDT",
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "settleId": "USDT",
        "type": "swap",
        "spot": False,
        "margin": False,
        "swap": True,
        "future": False,
        "option": False,
        "contract": True,
        "linear": True,
        "inverse": False,
        "active": True,
        "precision": {"amount": 0.0001, "price": 0.1},
        "limits": {
            "amount": {"min": 0.0001, "max": None},
            "price": {"min": None, "max": None},
            "cost": {"min": 2, "max": None},
            "leverage": {"min": None, "max": None},
        },
        "contractSize": 1,
        "taker": 0.0005,
        "expiry": None,
        "info": {
            "symbol": "BTC-USDT",
            "tradeMinQuantity": "0.0001",
            "tradeMinUSDT": "2",
            "currency": "USDT",
            "asset": "BTC",
            "apiStateOpen": "true",
            "apiStateClose": "true",
        },
    }


class CapturingBingxExchange(SequenceQualifiedBingxExchange):
    def __init__(self) -> None:
        super().__init__({"newUpdates": True, "options": {"defaultType": "swap"}})
        self.set_markets([_market()])
        self.watch_frames: list[dict[str, object]] = []
        self.unwatch_frames: list[tuple[str, dict[str, object]]] = []

    async def load_markets(
        self,
        reload: bool = False,
        params: dict[str, object] | None = None,
    ) -> Any:
        del reload, params
        return self.markets

    async def watch(
        self,
        url: str,
        message_hash: str,
        request: dict[str, object] | None = None,
        subscribe_hash: str | None = None,
        subscription: dict[str, object] | None = None,
    ) -> Any:
        del url, subscribe_hash, subscription
        assert request is not None
        self.watch_frames.append(request)
        return {"bids": [[100, 2]], "asks": [[101, 3]], "nonce": 7}

    async def un_watch(self, *args: Any, **kwargs: Any) -> Any:
        del kwargs
        self.unwatch_frames.append((str(args[3]), dict(args[7])))
        return {"ok": True}


@pytest.mark.asyncio
async def test_bingx_incremental_depth_subscribe_and_unsubscribe_are_symmetric() -> None:
    exchange = CapturingBingxExchange()

    await exchange.watch_order_book("BTC/USDT:USDT", 50)
    await exchange.un_watch_order_book("BTC/USDT:USDT")

    assert exchange.watch_frames[0]["dataType"] == "BTC-USDT@incrDepth"
    assert exchange.watch_frames[0]["reqType"] == "sub"
    assert exchange.unwatch_frames == [("BTC-USDT@incrDepth", {})]


class ResolveClient:
    def __init__(self) -> None:
        self.subscriptions: dict[str, object] = {
            "BTC-USDT@incrDepth": {"limit": 50},
        }
        self.values: list[tuple[str, dict[str, object]]] = []

    def resolve(self, value: object, message_hash: str) -> None:
        assert isinstance(value, Mapping)
        self.values.append((message_hash, dict(value)))


def test_bingx_handlers_normalize_bbo_and_enforce_incremental_sequence() -> None:
    exchange = SequenceQualifiedBingxExchange({"newUpdates": True})
    exchange.set_markets([_market()])
    client = ResolveClient()

    exchange.handle_message(
        client,
        {
            "dataType": "BTC-USDT@incrDepth",
            "data": {
                "action": "all",
                "lastUpdateId": 7,
                "bids": [["100", "2"]],
                "asks": [["101", "3"]],
                "T": 2,
            },
        },
    )
    exchange.handle_message(
        client,
        {
            "dataType": "BTC-USDT@incrDepth",
            "data": {
                "action": "update",
                "lastUpdateId": 8,
                "bids": [["100", "4"]],
                "asks": [],
                "T": 3,
            },
        },
    )
    exchange.handle_message(
        client,
        {
            "dataType": "BTC-USDT@incrDepth",
            "data": {
                "action": "update",
                "lastUpdateId": 10,
                "bids": [],
                "asks": [],
                "T": 4,
            },
        },
    )
    exchange.handle_message(
        client,
        {
            "dataType": "BTC-USDT@incrDepth",
            "data": {
                "action": "update",
                "lastUpdateId": 11,
                "bids": [["100", "9"]],
                "asks": [],
                "T": 5,
            },
        },
    )

    _, snapshot = client.values[0]
    _, contiguous = client.values[1]
    _, gap = client.values[2]
    _, still_desynced = client.values[3]
    assert snapshot["nonce"] == 7
    assert snapshot["ipegSequenceReset"] is True
    assert contiguous["nonce"] == 8
    assert contiguous["ipegSequenceContiguous"] is True
    assert gap["nonce"] == 8
    assert gap["ipegSequenceContiguous"] is False
    assert still_desynced["nonce"] == 8
    assert still_desynced["ipegSequenceContiguous"] is False
    assert still_desynced["bids"] == [[100.0, 4.0]]


def test_bingx_exact_linear_usdt_contract_metadata_is_qualified() -> None:
    instrument = normalize_market(Venue.BINGX, _market())

    assert instrument is not None
    assert instrument.minimum_notional == 2
    assert instrument.minimum_amount_contracts == instrument.amount_step_contracts
    assert instrument.no_fixed_minimum_notional is False


def test_pinned_bingx_adapter_rejects_per_symbol_bbo_and_exposes_sequenced_l2() -> None:
    adapter = CcxtProAdapter(Venue.BINGX)

    assert adapter._bbo_stream_kind() is None
    assert adapter._exchange.has["watchOrderBook"] is True
    assert adapter._exchange.has["unWatchOrderBook"] is True
