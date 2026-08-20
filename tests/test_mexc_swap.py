from __future__ import annotations

from decimal import Decimal
from typing import ClassVar

import pytest

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter, normalize_market
from interexchange_perp_grid.adapters.mexc_swap import SequenceQualifiedMexcExchange
from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import VenueOrderRequest


def _market(*, api_allowed: bool = True) -> dict[str, object]:
    return {
        "id": "BTC_USDT",
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
        "precision": {"amount": 1, "price": 0.5},
        "limits": {
            "amount": {"min": 1, "max": 1_000_000},
            "price": {"min": None, "max": None},
            "cost": {"min": None, "max": None},
        },
        "contractSize": 0.0001,
        "taker": 0.0006,
        "expiry": None,
        "info": {
            "symbol": "BTC_USDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "contractSize": 0.0001,
            "priceUnit": 0.5,
            "volUnit": 1,
            "minVol": 1,
            "state": 0,
            "apiAllowed": api_allowed,
        },
    }


def test_mexc_incremental_depth_gap_latches_until_transport_reinitialises() -> None:
    exchange = SequenceQualifiedMexcExchange({"newUpdates": True})
    book = exchange.order_book({}, 50)
    book.reset({"bids": [[100, 2]], "asks": [[101, 3]], "nonce": 7})

    exchange.handle_delta(book, {"version": 8, "bids": [[100, 4]], "asks": []})
    exchange.handle_delta(book, {"version": 10, "bids": [[100, 9]], "asks": []})
    exchange.handle_delta(book, {"version": 11, "bids": [[100, 12]], "asks": []})

    assert book["nonce"] == 8
    assert book["ipegSequenceContiguous"] is False
    assert book["ipegSequenceDesynced"] is True
    assert next(iter(book["bids"])) == [100.0, 4.0]


def test_mexc_exact_linear_usdt_metadata_is_fail_closed() -> None:
    instrument = normalize_market(Venue.MEXC, _market())

    assert instrument is not None
    assert instrument.contract_size_base == Decimal("0.0001")
    assert instrument.amount_step_contracts == Decimal(1)
    assert instrument.minimum_amount_contracts == Decimal(1)
    assert instrument.minimum_notional is None
    assert instrument.no_fixed_minimum_notional is True
    assert normalize_market(Venue.MEXC, _market(api_allowed=False)) is None
    for raw_field, invalid_value in (
        ("symbol", "ETH_USDT"),
        ("baseCoin", "ETH"),
        ("quoteCoin", "USDC"),
        ("settleCoin", "USDC"),
        ("contractSize", 0.01),
        ("priceUnit", 99),
        ("volUnit", 2),
        ("minVol", 2),
        ("state", 4),
    ):
        malformed = _market()
        info = malformed["info"]
        assert isinstance(info, dict)
        info[raw_field] = invalid_value
        assert normalize_market(Venue.MEXC, malformed) is None


class PrivateCapabilityExchange:
    has: ClassVar[dict[str, bool]] = {
        "watchOrders": True,
        "watchPositions": True,
        "watchBalance": True,
        "fetchBalance": True,
        "fetchPositions": True,
        "createOrder": True,
        "cancelOrder": True,
        "fetchOrder": True,
        "fetchOpenOrders": True,
        "fetchClosedOrders": True,
        "fetchTradingFee": True,
    }

    def __init__(self) -> None:
        self.create_calls = 0
        self.cancel_calls = 0

    async def load_markets(self) -> dict[str, object]:
        return {}

    async def create_order(self, *args: object) -> dict[str, object]:
        del args
        self.create_calls += 1
        return {}

    async def cancel_order(self, *args: object) -> dict[str, object]:
        del args
        self.cancel_calls += 1
        return {}


@pytest.mark.asyncio
async def test_mexc_contract_writes_are_physically_denied_while_api_is_in_maintenance() -> None:
    exchange = PrivateCapabilityExchange()
    adapter = CcxtPrivateAdapter(Venue.MEXC, exchange=exchange)
    instrument = normalize_market(Venue.MEXC, _market())
    assert instrument is not None
    request = VenueOrderRequest(
        Venue.MEXC,
        "mexc-disabled",
        instrument.symbol,
        Side.BUY,
        "limit",
        Decimal(1),
        Decimal(101),
        "IOC",
        {},
    )

    report = await adapter.probe_private_capabilities()
    assert report.submit_order is False
    assert report.cancel_order is False
    assert report.ready is False
    with pytest.raises(RuntimeError, match="submission is not qualified"):
        await adapter.submit_order(request, instrument)
    with pytest.raises(RuntimeError, match="cancellation is not qualified"):
        await adapter.cancel_order("order-1", instrument)
    assert exchange.create_calls == exchange.cancel_calls == 0


def test_pinned_mexc_rejects_unqualified_broad_bbo_and_exposes_sequenced_l2() -> None:
    adapter = CcxtProAdapter(Venue.MEXC)

    assert isinstance(adapter._exchange, SequenceQualifiedMexcExchange)
    assert adapter._bbo_stream_kind() is None
    assert adapter._exchange.has["watchOrderBook"] is True
    assert adapter._exchange.has["unWatchOrderBook"] is True
