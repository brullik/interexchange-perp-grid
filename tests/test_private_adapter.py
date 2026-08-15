from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import (
    PrivateStreamKind,
    SnapshotCompleteness,
    VenueOrderRequest,
)


def instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.01"),
        Decimal("1"),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("5"),
        Decimal("0.0005"),
        "fixture",
    )


class FakePrivateExchange:
    def __init__(self) -> None:
        self.has = {
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
            "fetchMarginMode": True,
            "fetchPositionMode": True,
        }
        self.closed = False
        self.create_calls = 0
        self.account_open_order_calls = 0
        self.account_position_calls = 0
        self.last_account_params: tuple[dict[str, object], dict[str, object]] | None = None
        self.last_open_order_limit: int | None = None

    async def load_markets(self) -> dict[str, object]:
        return {}

    async def fetch_balance(self, params: dict[str, str]) -> dict[str, object]:
        assert params == {"type": "swap"}
        return {
            "total": {"USDT": "100"},
            "free": {"USDT": "80"},
            "marginMode": "cross",
            "positionMode": "oneway",
            "tradingEnabled": True,
            "permissions": ["trade"],
            "withdrawalEnabled": False,
            "transferEnabled": False,
        }

    async def fetch_margin_mode(self, symbol: str) -> dict[str, object]:
        assert symbol == "BTC/USDT:USDT"
        return {"marginMode": "cross"}

    async def fetch_position_mode(self, symbol: str) -> dict[str, object]:
        assert symbol == "BTC/USDT:USDT"
        return {"hedged": False}

    async def watch_orders(self, symbol: str) -> list[dict[str, object]]:
        assert symbol == "BTC/USDT:USDT"
        return [self.order("client-1", amount="10", filled="4", status="open")]

    async def watch_positions(self, symbols: list[str]) -> list[dict[str, object]]:
        assert symbols == ["BTC/USDT:USDT"]
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": "4",
                "entryPrice": "100",
                "markPrice": "101",
            }
        ]

    async def fetch_positions(
        self,
        symbols: list[str] | None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        if symbols is None:
            self.account_position_calls += 1
            position_params = params or {}
            order_params = self.last_account_params[0] if self.last_account_params else {}
            self.last_account_params = (order_params, position_params)
            symbols = ["BTC/USDT:USDT"]
        return await self.watch_positions(symbols)

    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: float | None,
        params: dict[str, object],
    ) -> dict[str, object]:
        del order_type, price
        self.create_calls += 1
        return self.order(
            str(params.get("orderLinkId") or params.get("clOrdId") or "client-1"),
            amount=str(amount),
            filled=str(amount),
            status="closed",
            side=side,
            symbol=symbol,
        )

    async def cancel_order(self, order_id: str, symbol: str) -> dict[str, object]:
        return self.order("client-1", status="canceled", symbol=symbol, order_id=order_id)

    async def fetch_order(self, order_id: str, symbol: str) -> dict[str, object]:
        return self.order(
            "client-1",
            filled="10",
            status="closed",
            symbol=symbol,
            order_id=order_id,
        )

    async def fetch_open_orders(
        self,
        symbol: str | None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del since
        if symbol is None:
            self.account_open_order_calls += 1
            self.last_open_order_limit = limit
            order_params = params or {}
            position_params = self.last_account_params[1] if self.last_account_params else {}
            self.last_account_params = (order_params, position_params)
            symbol = "BTC/USDT:USDT"
        return [self.order("client-open", symbol=symbol)]

    async def fetch_closed_orders(self, symbol: str) -> list[dict[str, object]]:
        return [
            self.order(
                "client-closed",
                filled="10",
                status="closed",
                symbol=symbol,
            )
        ]

    async def fetch_trading_fee(self, symbol: str) -> dict[str, str]:
        assert symbol == "BTC/USDT:USDT"
        return {"taker": "0.00055"}

    async def close(self) -> None:
        self.closed = True

    @staticmethod
    def order(
        client_id: str,
        *,
        amount: str = "10",
        filled: str = "0",
        status: str = "open",
        side: str = "buy",
        symbol: str = "BTC/USDT:USDT",
        order_id: str = "order-1",
    ) -> dict[str, object]:
        return {
            "id": order_id,
            "clientOrderId": client_id,
            "symbol": symbol,
            "side": side,
            "status": status,
            "amount": amount,
            "filled": filled,
            "average": "100" if Decimal(filled) > 0 else None,
            "fee": {"currency": "USDT", "cost": "0.02"},
        }


@pytest.mark.parametrize("venue", [Venue.BYBIT, Venue.OKX, Venue.BINANCE_USDM])
@pytest.mark.asyncio
async def test_wave_one_private_capabilities_and_account_are_normalised(venue: Venue) -> None:
    exchange = FakePrivateExchange()
    adapter = CcxtPrivateAdapter(venue, exchange=exchange)
    report = await adapter.probe_private_capabilities()
    account = await adapter.fetch_account(instrument(venue))
    assert report.ready is True
    assert account.equity_usdt == Decimal("100")
    assert account.free_margin_usdt == Decimal("80")
    assert account.margin_mode == "cross"
    assert account.position_mode == "oneway"
    assert account.permissions == ("trade",)


@pytest.mark.asyncio
async def test_private_streams_preserve_actual_contract_multiplier_and_fee() -> None:
    exchange = FakePrivateExchange()
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=exchange)
    selected = instrument(Venue.BYBIT)
    orders = await adapter.watch_orders(selected)
    positions = await adapter.watch_positions(selected)
    assert orders[0].requested_base_quantity == Decimal("0.10")
    assert orders[0].filled_base_quantity == Decimal("0.04")
    assert orders[0].status.value == "PARTIAL"
    assert positions[0].side == Side.BUY
    assert positions[0].base_quantity == Decimal("0.04")
    assert await adapter.fetch_trading_fee(selected) == Decimal("0.00055")
    found = await adapter.find_order_by_client_id("client-closed", selected)
    assert found is not None
    assert found.filled_base_quantity == Decimal("0.10")


@pytest.mark.asyncio
async def test_private_submit_fetch_and_cancel_remain_normalised() -> None:
    exchange = FakePrivateExchange()
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=exchange)
    selected = instrument(Venue.BYBIT)
    request = VenueOrderRequest(
        Venue.BYBIT,
        "client-submit",
        selected.symbol,
        Side.BUY,
        "limit",
        Decimal("10"),
        Decimal("101"),
        "IOC",
        {"orderLinkId": "client-submit", "timeInForce": "IOC"},
    )
    submitted = await adapter.submit_order(request, selected)
    fetched = await adapter.fetch_order("order-1", selected, "client-1")
    cancelled = await adapter.cancel_order("order-1", selected)
    snapshot = await adapter.fetch_active_snapshot()
    assert submitted.status.value == "FILLED"
    assert submitted.filled_base_quantity == Decimal("0.10")
    assert fetched.status.value == "FILLED"
    assert cancelled.status.value == "CANCELLED"
    assert exchange.create_calls == 1
    assert snapshot.event_watermark == 0


def _linear_market(symbol: str = "BTC/USDT:USDT") -> dict[str, object]:
    base = symbol.split("/", 1)[0]
    return {
        "symbol": symbol,
        "id": f"{base}USDT",
        "base": base,
        "quote": "USDT",
        "settle": "USDT",
        "active": True,
        "created": 1_609_459_200_000,
        "contract": True,
        "swap": True,
        "linear": True,
        "inverse": False,
        "expiry": None,
        "contractSize": "0.01",
        "taker": "0.0005",
        "precision": {"amount": "1", "price": "0.1"},
        "limits": {"amount": {"min": "1"}, "cost": {"min": "5"}},
    }


class LargeAccountWideExchange(FakePrivateExchange):
    async def load_markets(self) -> dict[str, object]:
        symbols = ("BTC/USDT:USDT", *(f"COIN{index}/USDT:USDT" for index in range(100)))
        return {symbol: _linear_market(symbol) for symbol in symbols}


@pytest.mark.parametrize(
    ("venue", "expected_order_params", "expected_position_params", "order_limit"),
    [
        (Venue.BINANCE_USDM, {"type": "future"}, {"type": "future"}, None),
        (
            Venue.BYBIT,
            {"category": "linear", "settleCoin": "USDT"},
            {"category": "linear", "settleCoin": "USDT", "limit": 200},
            50,
        ),
        (Venue.OKX, {"instType": "SWAP"}, {"instType": "SWAP"}, 100),
    ],
)
@pytest.mark.asyncio
async def test_wave1_snapshot_is_account_wide_and_request_bounded(
    venue: Venue,
    expected_order_params: dict[str, object],
    expected_position_params: dict[str, object],
    order_limit: int | None,
) -> None:
    exchange = LargeAccountWideExchange()
    adapter = CcxtPrivateAdapter(venue, exchange=exchange)

    snapshot = await adapter.fetch_active_snapshot()

    assert exchange.account_open_order_calls == 2
    assert exchange.account_position_calls == 2
    assert exchange.last_account_params == (expected_order_params, expected_position_params)
    assert exchange.last_open_order_limit == order_limit
    assert snapshot.account_wide is True
    assert snapshot.request_count == 4
    assert snapshot.latency_ms >= 0
    assert snapshot.completeness == SnapshotCompleteness.COMPLETE
    assert snapshot.raw_open_order_count == 1
    assert snapshot.raw_nonzero_position_count == 1


@pytest.mark.asyncio
async def test_private_event_watermark_is_carried_into_account_snapshot() -> None:
    exchange = AccountWideStreamExchange()
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=exchange)

    first = await adapter.fetch_active_snapshot()
    event = await adapter.watch_account_wide_orders()
    adapter.acknowledge_private_event(event.event_watermark)
    second = await adapter.fetch_active_snapshot()

    assert first.event_watermark == 0
    assert second.event_watermark == 1


@pytest.mark.asyncio
async def test_private_event_watermark_can_be_restored_without_regression() -> None:
    exchange = AccountWideStreamExchange()
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=exchange)
    adapter.seed_private_event_watermark(5)

    restored = await adapter.fetch_active_snapshot()
    event = await adapter.watch_account_wide_orders()
    adapter.acknowledge_private_event(event.event_watermark)
    advanced = await adapter.fetch_active_snapshot()

    assert restored.event_watermark == 5
    assert advanced.event_watermark == 6
    with pytest.raises(ValueError, match="cannot regress"):
        adapter.seed_private_event_watermark(5)


class AccountWideStreamExchange(LargeAccountWideExchange):
    def __init__(self) -> None:
        super().__init__()
        self.stream_params: list[tuple[str, dict[str, object]]] = []

    async def watch_orders(
        self,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        if symbol is not None:
            return await super().watch_orders(symbol)
        assert since is None
        assert limit is None
        self.stream_params.append(("orders", params or {}))
        return [self.order("account-stream-order")]

    async def watch_positions(
        self,
        symbols: list[str] | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        if symbols is not None:
            return await super().watch_positions(symbols)
        assert since is None
        assert limit is None
        self.stream_params.append(("positions", params or {}))
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": "0",
                "entryPrice": "100",
                "markPrice": "101",
            }
        ]

    async def watch_balance(self, params: dict[str, object]) -> dict[str, object]:
        self.stream_params.append(("account", params))
        return {
            "total": {"USDT": "100"},
            "free": {"USDT": "80"},
            "tradingEnabled": True,
            "permissions": ["trade"],
        }


@pytest.mark.parametrize(
    ("venue", "expected_stream_params"),
    [
        (
            Venue.BINANCE_USDM,
            [
                ("orders", {"type": "future"}),
                ("positions", {"type": "future"}),
                ("account", {"type": "future"}),
            ],
        ),
        (Venue.BYBIT, [("orders", {}), ("positions", {}), ("account", {})]),
        (
            Venue.OKX,
            [
                ("orders", {"type": "swap"}),
                ("positions", {"instType": "SWAP"}),
                ("account", {}),
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_account_wide_private_streams_are_normalised_and_monotonic(
    venue: Venue,
    expected_stream_params: list[tuple[str, dict[str, object]]],
) -> None:
    exchange = AccountWideStreamExchange()
    adapter = CcxtPrivateAdapter(venue, exchange=exchange)

    order_event = await adapter.watch_account_wide_orders()
    position_event = await adapter.watch_account_wide_positions()
    account_event = await adapter.watch_account_wide_balance()

    assert [order_event.kind, position_event.kind, account_event.kind] == [
        PrivateStreamKind.ORDERS,
        PrivateStreamKind.POSITIONS,
        PrivateStreamKind.ACCOUNT,
    ]
    assert [
        order_event.event_watermark,
        position_event.event_watermark,
        account_event.event_watermark,
    ] == [1, 2, 3]
    assert order_event.orders[0].client_order_id == "account-stream-order"
    assert position_event.positions[0].base_quantity == 0
    assert account_event.account is not None
    assert account_event.account.free_margin_usdt == Decimal("80")
    assert all(
        event.source_monotonic_ns > 0 for event in (order_event, position_event, account_event)
    )
    assert exchange.stream_params == expected_stream_params


class OneWayClosedPositionStreamExchange(AccountWideStreamExchange):
    def __init__(self, side: str | None) -> None:
        super().__init__()
        self.side = side

    async def watch_positions(
        self,
        symbols: list[str] | None = None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del symbols, since, limit
        self.stream_params.append(("positions", params or {}))
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "side": self.side,
                "contracts": "0",
                "entryPrice": None,
                "markPrice": "101",
            }
        ]


@pytest.mark.parametrize(
    ("venue", "side"),
    [
        (Venue.BYBIT, None),
        (Venue.BINANCE_USDM, "both"),
    ],
)
@pytest.mark.asyncio
async def test_one_way_side_less_zero_position_closes_both_cached_sides(
    venue: Venue,
    side: str | None,
) -> None:
    adapter = CcxtPrivateAdapter(
        venue,
        exchange=OneWayClosedPositionStreamExchange(side),
    )

    event = await adapter.watch_account_wide_positions()

    assert {(position.side, position.base_quantity) for position in event.positions} == {
        (Side.BUY, Decimal(0)),
        (Side.SELL, Decimal(0)),
    }
    assert event.unknown_active_records == ()


@pytest.mark.asyncio
async def test_transport_return_advances_watermark_before_event_normalisation_waits() -> None:
    exchange = AccountWideStreamExchange()
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=exchange)
    await adapter._private_event_lock.acquire()
    event_task = asyncio.create_task(adapter.watch_account_wide_orders())
    while not exchange.stream_params:
        await asyncio.sleep(0)

    snapshot = await adapter.fetch_active_snapshot()

    assert snapshot.event_watermark == 1
    assert snapshot.completeness == SnapshotCompleteness.UNKNOWN
    assert any(
        record.reason == "PRIVATE_EVENT_DELIVERY_PENDING"
        for record in snapshot.unknown_active_records
    )
    adapter._private_event_lock.release()
    event = await event_task
    assert event.event_watermark == 1
    adapter.acknowledge_private_event(event.event_watermark)


class MalformedStreamExchange(LargeAccountWideExchange):
    async def watch_orders(self, symbol: str) -> list[dict[str, object]]:
        assert symbol == "BTC/USDT:USDT"
        return [{"symbol": symbol, "status": "open"}]


@pytest.mark.asyncio
async def test_malformed_symbol_watcher_does_not_advance_account_wide_watermark() -> None:
    exchange = MalformedStreamExchange()
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=exchange)

    with pytest.raises(ValueError, match="private order amount is unavailable"):
        await adapter.watch_orders(instrument(Venue.BYBIT))
    snapshot = await adapter.fetch_active_snapshot()

    assert snapshot.event_watermark == 0


class MalformedActiveExchange(FakePrivateExchange):
    async def load_markets(self) -> dict[str, object]:
        return {"BTC/USDT:USDT": _linear_market()}

    async def fetch_open_orders(
        self,
        symbol: str | None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del symbol, since, limit, params
        return [self.order("unknown-order", symbol="ETH/USDT:USDT")]

    async def fetch_positions(
        self,
        symbols: list[str] | None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del symbols, params
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "unexpected",
                "contracts": "2",
                "entryPrice": "100",
                "markPrice": "101",
            }
        ]


@pytest.mark.parametrize("venue", [Venue.BYBIT, Venue.OKX, Venue.BINANCE_USDM])
@pytest.mark.asyncio
async def test_wave1_active_snapshot_never_drops_malformed_raw_records(venue: Venue) -> None:
    adapter = CcxtPrivateAdapter(venue, exchange=MalformedActiveExchange())
    snapshot = await adapter.fetch_active_snapshot()
    assert snapshot.raw_open_order_count == 1
    assert snapshot.raw_nonzero_position_count == 1
    assert snapshot.open_orders == ()
    assert snapshot.positions == ()
    reasons = {record.reason for record in snapshot.unknown_active_records}
    assert "UNKNOWN_SYMBOL" in reasons
    assert "FIRST_ACCOUNT_WIDE_SAMPLE_INCOMPLETE" in reasons
    assert "MALFORMED_OR_UNKNOWN_SIDE" in reasons
    assert snapshot.completeness == SnapshotCompleteness.UNKNOWN


class PageLimitActiveExchange(LargeAccountWideExchange):
    async def fetch_open_orders(
        self,
        symbol: str | None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        assert symbol is None
        assert since is None
        assert limit == 50
        assert params == {"category": "linear", "settleCoin": "USDT"}
        return [self.order(f"client-{index}") for index in range(limit)]


@pytest.mark.asyncio
async def test_account_wide_page_limit_is_unknown_instead_of_silently_truncated() -> None:
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=PageLimitActiveExchange())

    snapshot = await adapter.fetch_active_snapshot()

    assert snapshot.completeness == SnapshotCompleteness.UNKNOWN
    assert any(
        record.reason == "ACCOUNT_WIDE_RESULT_AT_PAGE_LIMIT"
        for record in snapshot.unknown_active_records
    )


class ExplicitCursorActiveExchange(LargeAccountWideExchange):
    async def fetch_open_orders(
        self,
        symbol: str | None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del symbol, since, limit, params
        order = self.order("client-cursor")
        order["info"] = {"nextPageCursor": "EXPLICIT-MORE"}
        return [order]


@pytest.mark.asyncio
async def test_explicit_continuation_cursor_is_authoritative_below_page_limit() -> None:
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=ExplicitCursorActiveExchange())

    snapshot = await adapter.fetch_active_snapshot()

    assert snapshot.raw_open_order_count == 1
    assert snapshot.completeness == SnapshotCompleteness.UNKNOWN
    assert any(
        record.reason == "ACCOUNT_WIDE_RESULT_HAS_MORE_PAGES"
        and record.raw_record.get("next_page_cursor") == "EXPLICIT-MORE"
        for record in snapshot.unknown_active_records
    )


class StateChangesBetweenSamplesExchange(LargeAccountWideExchange):
    def __init__(self) -> None:
        super().__init__()
        self._position_sample = 0

    async def fetch_open_orders(
        self,
        symbol: str | None,
        since: int | None = None,
        limit: int | None = None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del symbol, since, limit, params
        return []

    async def fetch_positions(
        self,
        symbols: list[str] | None,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        del symbols, params
        self._position_sample += 1
        if self._position_sample == 1:
            return []
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": "1",
                "entryPrice": "100",
                "markPrice": "101",
            }
        ]


@pytest.mark.asyncio
async def test_state_change_across_account_wide_samples_never_returns_complete_flat() -> None:
    adapter = CcxtPrivateAdapter(Venue.BYBIT, exchange=StateChangesBetweenSamplesExchange())

    snapshot = await adapter.fetch_active_snapshot()

    assert snapshot.raw_nonzero_position_count == 1
    assert snapshot.completeness == SnapshotCompleteness.UNKNOWN
    assert any(
        record.reason == "ACCOUNT_WIDE_SNAPSHOT_UNSTABLE"
        for record in snapshot.unknown_active_records
    )
