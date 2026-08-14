from __future__ import annotations

from decimal import Decimal

import pytest

from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import VenueOrderRequest


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

    async def fetch_positions(self, symbols: list[str]) -> list[dict[str, object]]:
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

    async def fetch_open_orders(self, symbol: str) -> list[dict[str, object]]:
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
    assert submitted.status.value == "FILLED"
    assert submitted.filled_base_quantity == Decimal("0.10")
    assert fetched.status.value == "FILLED"
    assert cancelled.status.value == "CANCELLED"
    assert exchange.create_calls == 1
