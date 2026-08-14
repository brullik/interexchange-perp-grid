from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from interexchange_perp_grid.adapters.ccxt_pro import (
    CcxtProAdapter,
    _decimal,
    _mapping,
    _supported,
)
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateCapabilityReport,
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)


@dataclass(frozen=True, slots=True)
class PrivateCredentials:
    api_key: str
    secret: str
    password: str | None = None

    def __post_init__(self) -> None:
        if not self.api_key or not self.secret:
            raise ValueError("private API key and secret are required")

    @classmethod
    def from_environment(
        cls,
        venue: Venue,
        environ: Mapping[str, str] | None = None,
    ) -> PrivateCredentials:
        source = os.environ if environ is None else environ
        prefix = venue.value.upper()
        key = source.get(f"IPEG_{prefix}_API_KEY", "")
        secret = source.get(f"IPEG_{prefix}_API_SECRET", "")
        password = source.get(f"IPEG_{prefix}_API_PASSWORD") or None
        return cls(key, secret, password)


class CcxtPrivateAdapter:
    """CCXT Pro private transport; only trading/account operations are exposed."""

    def __init__(
        self,
        venue: Venue,
        credentials: PrivateCredentials | None = None,
        exchange: Any | None = None,
    ) -> None:
        self.venue = venue
        self._exchange: Any = exchange or CcxtProAdapter._build_exchange(venue)
        if credentials is not None:
            self._exchange.apiKey = credentials.api_key
            self._exchange.secret = credentials.secret
            if credentials.password is not None:
                self._exchange.password = credentials.password

    def _has(self, capability: str) -> bool:
        return _supported(_mapping(self._exchange.has).get(capability))

    async def probe_private_capabilities(self) -> PrivateCapabilityReport:
        await self._exchange.load_markets()
        values = {
            "order_stream": self._has("watchOrders"),
            "position_stream": self._has("watchPositions"),
            "balance_stream": self._has("watchBalance"),
            "fetch_balance": self._has("fetchBalance"),
            "fetch_positions": self._has("fetchPositions"),
            "submit_order": self._has("createOrder"),
            "cancel_order": self._has("cancelOrder"),
            "fetch_order": self._has("fetchOrder")
            or (self._has("fetchOpenOrders") and self._has("fetchClosedOrders")),
            "fetch_fee": self._has("fetchTradingFee") or self._has("fetchTradingFees"),
        }
        return PrivateCapabilityReport(
            venue=self.venue,
            checked_at=datetime.now(UTC),
            missing=tuple(name for name, supported in values.items() if not supported),
            **values,
        )

    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot:
        raw = await self._exchange.fetch_balance({"type": "swap"})
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT fetch_balance must return a mapping")
        total = _currency_value(raw, "total", "USDT")
        free = _currency_value(raw, "free", "USDT")
        info = _mapping(raw.get("info"))
        margin_mode = _string_or_none(raw.get("marginMode") or info.get("marginMode"))
        position_mode = _string_or_none(raw.get("positionMode") or info.get("positionMode"))
        if self._has("fetchMarginMode"):
            mode_raw = await self._exchange.fetch_margin_mode(instrument.symbol)
            margin_mode = _string_or_none(_mapping(mode_raw).get("marginMode")) or margin_mode
        if self._has("fetchPositionMode"):
            position_raw = await self._exchange.fetch_position_mode(instrument.symbol)
            hedged = _mapping(position_raw).get("hedged")
            if isinstance(hedged, bool):
                position_mode = "hedge" if hedged else "oneway"
        trading_enabled = _optional_bool(
            raw.get("tradingEnabled")
            if raw.get("tradingEnabled") is not None
            else info.get("canTrade")
        )
        permissions_value = raw.get("permissions") or info.get("permissions")
        permissions = (
            tuple(str(value).lower() for value in permissions_value)
            if isinstance(permissions_value, Sequence)
            and not isinstance(permissions_value, (str, bytes))
            else ()
        )
        if total is None or free is None:
            raise ValueError("USDT equity/free margin is unavailable")
        return AccountSnapshot(
            venue=self.venue,
            equity_usdt=total,
            free_margin_usdt=free,
            margin_mode=margin_mode,
            position_mode=position_mode,
            trading_enabled=trading_enabled,
            permissions=permissions,
            observed_at=datetime.now(UTC),
        )

    async def watch_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        raw = await self._exchange.watch_orders(instrument.symbol)
        return _normalise_orders(self.venue, raw, instrument)

    async def watch_positions(self, instrument: Instrument) -> tuple[PositionSnapshot, ...]:
        raw = await self._exchange.watch_positions([instrument.symbol])
        if not isinstance(raw, Sequence):
            raise TypeError("CCXT watch_positions must return a sequence")
        return tuple(
            position
            for value in raw
            if isinstance(value, Mapping)
            and (position := _normalise_position(self.venue, value, instrument)) is not None
        )

    async def watch_balance(self, instrument: Instrument) -> AccountSnapshot:
        raw = await self._exchange.watch_balance({"type": "swap"})
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT watch_balance must return a mapping")
        return await self.fetch_account(instrument)

    async def fetch_positions(self, instrument: Instrument) -> tuple[PositionSnapshot, ...]:
        raw = await self._exchange.fetch_positions([instrument.symbol])
        if not isinstance(raw, Sequence):
            raise TypeError("CCXT fetch_positions must return a sequence")
        return tuple(
            position
            for value in raw
            if isinstance(value, Mapping)
            and (position := _normalise_position(self.venue, value, instrument)) is not None
        )

    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        raw = await self._exchange.create_order(
            request.symbol,
            request.order_type,
            request.side.value.lower(),
            float(request.amount_contracts),
            float(request.price) if request.price is not None else None,
            request.params,
        )
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT create_order must return a mapping")
        return _normalise_order(self.venue, raw, instrument, request.client_order_id)

    async def cancel_order(
        self,
        order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder:
        raw = await self._exchange.cancel_order(order_id, instrument.symbol)
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT cancel_order must return a mapping")
        return _normalise_order(self.venue, raw, instrument, "cancelled-order")

    async def fetch_order(
        self,
        order_id: str,
        instrument: Instrument,
        client_order_id: str,
    ) -> PrivateOrder:
        raw = await self._exchange.fetch_order(order_id, instrument.symbol)
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT fetch_order must return a mapping")
        return _normalise_order(self.venue, raw, instrument, client_order_id)

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder | None:
        calls = []
        if self._has("fetchOpenOrders"):
            calls.append(self._exchange.fetch_open_orders(instrument.symbol))
        if self._has("fetchClosedOrders"):
            calls.append(self._exchange.fetch_closed_orders(instrument.symbol))
        if not calls:
            return None
        batches = await asyncio.gather(*calls)
        for batch in batches:
            for order in _normalise_orders(self.venue, batch, instrument):
                if order.client_order_id == client_order_id:
                    return order
        return None

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal | None:
        if self._has("fetchTradingFee"):
            raw = await self._exchange.fetch_trading_fee(instrument.symbol)
            return _decimal(_mapping(raw).get("taker"))
        raw = await self._exchange.fetch_trading_fees()
        if not isinstance(raw, Mapping):
            return None
        return _decimal(_mapping(raw.get(instrument.symbol)).get("taker"))

    async def close(self) -> None:
        await self._exchange.close()


def _currency_value(raw: Mapping[str, Any], group: str, currency: str) -> Decimal | None:
    grouped = _mapping(raw.get(group))
    value = _decimal(grouped.get(currency))
    if value is not None:
        return value
    currency_row = _mapping(raw.get(currency))
    return _decimal(currency_row.get(group))


def _string_or_none(value: object) -> str | None:
    return str(value).lower() if value is not None and str(value).strip() else None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _normalise_position(
    venue: Venue,
    raw: Mapping[str, Any],
    instrument: Instrument,
) -> PositionSnapshot | None:
    symbol = raw.get("symbol")
    if symbol != instrument.symbol:
        return None
    contracts = _decimal(raw.get("contracts"))
    if contracts is None or contracts == 0:
        return None
    side_value = str(raw.get("side", "")).lower()
    side = Side.BUY if side_value == "long" else Side.SELL if side_value == "short" else None
    if side is None:
        return None
    return PositionSnapshot(
        venue=venue,
        symbol=instrument.symbol,
        side=side,
        base_quantity=abs(contracts) * instrument.contract_size_base,
        entry_price=_decimal(raw.get("entryPrice")),
        mark_price=_decimal(raw.get("markPrice")),
        observed_at=datetime.now(UTC),
    )


def _normalise_orders(
    venue: Venue,
    raw: object,
    instrument: Instrument,
) -> tuple[PrivateOrder, ...]:
    if isinstance(raw, Mapping):
        values: Sequence[object] = (raw,)
    elif isinstance(raw, Sequence):
        values = raw
    else:
        raise TypeError("CCXT order result must be a mapping or sequence")
    return tuple(
        _normalise_order(venue, value, instrument, "unknown-client-id")
        for value in values
        if isinstance(value, Mapping)
    )


def _normalise_order(
    venue: Venue,
    raw: Mapping[str, Any],
    instrument: Instrument,
    fallback_client_id: str,
) -> PrivateOrder:
    amount = _decimal(raw.get("amount"))
    filled = _decimal(raw.get("filled")) or Decimal(0)
    if amount is None or amount <= 0:
        raise ValueError("private order amount is unavailable")
    info = _mapping(raw.get("info"))
    client_id = (
        raw.get("clientOrderId")
        or info.get("clientOrderId")
        or info.get("clOrdId")
        or info.get("orderLinkId")
        or fallback_client_id
    )
    side_value = str(raw.get("side", "")).lower()
    side = Side.BUY if side_value == "buy" else Side.SELL if side_value == "sell" else None
    if side is None:
        raise ValueError("private order side is unavailable")
    status = _order_status(str(raw.get("status", "")), filled, amount)
    fee = _mapping(raw.get("fee"))
    fee_cost = _decimal(fee.get("cost")) if fee.get("currency") in {None, "USDT"} else None
    return PrivateOrder(
        venue=venue,
        order_id=str(raw["id"]) if raw.get("id") is not None else None,
        client_order_id=str(client_id),
        symbol=instrument.symbol,
        side=side,
        status=status,
        requested_base_quantity=amount * instrument.contract_size_base,
        filled_base_quantity=filled * instrument.contract_size_base,
        average_price=_decimal(raw.get("average")),
        fee_usdt=fee_cost,
        observed_at=datetime.now(UTC),
    )


def _order_status(raw: str, filled: Decimal, amount: Decimal) -> PrivateOrderStatus:
    normalized = raw.lower()
    if normalized == "filled" or filled == amount:
        return PrivateOrderStatus.FILLED
    if normalized in {"canceled", "cancelled", "expired"}:
        return PrivateOrderStatus.CANCELLED
    if normalized in {"rejected"}:
        return PrivateOrderStatus.REJECTED
    if filled > 0:
        return PrivateOrderStatus.PARTIAL
    if normalized == "closed":
        return PrivateOrderStatus.CANCELLED
    if normalized in {"open", "new"}:
        return PrivateOrderStatus.OPEN
    return PrivateOrderStatus.UNKNOWN
