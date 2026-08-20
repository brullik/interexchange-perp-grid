from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from interexchange_perp_grid.adapters.ccxt_pro import (
    CcxtProAdapter,
    _decimal,
    _mapping,
    _supported,
    normalize_market,
)
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateActiveSnapshot,
    PrivateCapabilityReport,
    PrivateOrder,
    PrivateOrderStatus,
    PrivateStreamEvent,
    PrivateStreamKind,
    SnapshotCompleteness,
    UnknownActiveRecord,
    VenueOrderRequest,
)


def production_submit_guard_active(environ: Mapping[str, str] | None = None) -> bool:
    source = os.environ if environ is None else environ
    return source.get("IPEG_CI_PRODUCTION_SUBMIT_GUARD", "").lower() in {"1", "true"}


def _enforce_production_submit_guard() -> None:
    if not production_submit_guard_active():
        return
    counter_path = os.environ.get("IPEG_PRODUCTION_SUBMIT_COUNTER_FILE", "")
    if counter_path:
        with Path(counter_path).open("a", encoding="utf-8") as counter:
            counter.write("1\n")
    raise RuntimeError("production submit transport is disabled by the C4 CI guard")


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
        self._production_transport = exchange is None
        self._exchange: Any = exchange or CcxtProAdapter._build_exchange(venue)
        self._private_event_watermark = 0
        self._private_events_pending: set[int] = set()
        self._private_event_lock = asyncio.Lock()
        if credentials is not None:
            self._exchange.apiKey = credentials.api_key
            self._exchange.secret = credentials.secret
            if credentials.password is not None:
                self._exchange.password = credentials.password

    def _has(self, capability: str) -> bool:
        return _supported(_mapping(self._exchange.has).get(capability))

    def seed_private_event_watermark(self, watermark: int) -> None:
        if watermark < self._private_event_watermark:
            raise ValueError("private event watermark cannot regress")
        self._private_event_watermark = watermark

    def acknowledge_private_event(self, watermark: int) -> None:
        self._private_events_pending.discard(watermark)

    def current_private_event_watermark(self) -> int:
        return self._private_event_watermark

    def _advance_private_event_watermark(self) -> int:
        self._private_event_watermark += 1
        return self._private_event_watermark

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
            "fetch_open_orders": self._has("fetchOpenOrders"),
            "fetch_closed_orders": self._has("fetchClosedOrders"),
            "fetch_fee": self._has("fetchTradingFee") or self._has("fetchTradingFees"),
        }
        if self.venue == Venue.MEXC:
            # MEXC's official contract API still marks place/cancel endpoints as maintenance.
            values["submit_order"] = False
            values["cancel_order"] = False
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
            {str(value).lower() for value in permissions_value}
            if isinstance(permissions_value, Sequence)
            and not isinstance(permissions_value, (str, bytes))
            else set()
        )
        if trading_enabled is True:
            permissions.add("trade")
        withdrawal_enabled = _optional_bool(
            raw.get("withdrawalEnabled")
            if raw.get("withdrawalEnabled") is not None
            else info.get("canWithdraw")
        )
        transfer_enabled = _optional_bool(
            raw.get("transferEnabled")
            if raw.get("transferEnabled") is not None
            else info.get("canTransfer")
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
            permissions=tuple(sorted(permissions)),
            observed_at=datetime.now(UTC),
            withdrawal_enabled=withdrawal_enabled,
            transfer_enabled=transfer_enabled,
        )

    async def watch_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        raw = await self._exchange.watch_orders(instrument.symbol)
        return _normalise_orders(self.venue, raw, instrument)

    async def watch_positions(self, instrument: Instrument) -> tuple[PositionSnapshot, ...]:
        raw = await self._exchange.watch_positions([instrument.symbol])
        if not isinstance(raw, Sequence):
            raise TypeError("CCXT watch_positions must return a sequence")
        positions = tuple(
            position
            for value in raw
            if isinstance(value, Mapping)
            and (position := _normalise_position(self.venue, value, instrument)) is not None
        )
        return positions

    async def watch_balance(self, instrument: Instrument) -> AccountSnapshot:
        raw = await self._exchange.watch_balance({"type": "swap"})
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT watch_balance must return a mapping")
        return await self.fetch_account(instrument)

    async def watch_account_wide_orders(self) -> PrivateStreamEvent:
        params = _account_wide_stream_params(self.venue, PrivateStreamKind.ORDERS)
        raw = await self._exchange.watch_orders(None, None, None, params)
        source_ns = time.monotonic_ns()
        watermark = self._advance_private_event_watermark()
        self._private_events_pending.add(watermark)
        try:
            async with self._private_event_lock:
                instruments = await self._linear_instruments()
                orders, unknown = _normalise_account_order_updates(self.venue, raw, instruments)
                return PrivateStreamEvent(
                    self.venue,
                    PrivateStreamKind.ORDERS,
                    watermark,
                    datetime.now(UTC),
                    source_ns,
                    orders=orders,
                    unknown_active_records=unknown,
                )
        except (Exception, asyncio.CancelledError):
            self._private_events_pending.discard(watermark)
            raise

    async def watch_account_wide_positions(self) -> PrivateStreamEvent:
        params = _account_wide_stream_params(self.venue, PrivateStreamKind.POSITIONS)
        raw = await self._exchange.watch_positions(None, None, None, params)
        source_ns = time.monotonic_ns()
        watermark = self._advance_private_event_watermark()
        self._private_events_pending.add(watermark)
        try:
            async with self._private_event_lock:
                instruments = await self._linear_instruments()
                positions, unknown = _normalise_account_position_updates(
                    self.venue, raw, instruments
                )
                return PrivateStreamEvent(
                    self.venue,
                    PrivateStreamKind.POSITIONS,
                    watermark,
                    datetime.now(UTC),
                    source_ns,
                    positions=positions,
                    unknown_active_records=unknown,
                )
        except (Exception, asyncio.CancelledError):
            self._private_events_pending.discard(watermark)
            raise

    async def watch_account_wide_balance(self) -> PrivateStreamEvent:
        params = _account_wide_stream_params(self.venue, PrivateStreamKind.ACCOUNT)
        raw = await self._exchange.watch_balance(params)
        source_ns = time.monotonic_ns()
        watermark = self._advance_private_event_watermark()
        self._private_events_pending.add(watermark)
        try:
            async with self._private_event_lock:
                unknown: tuple[UnknownActiveRecord, ...]
                if not isinstance(raw, Mapping):
                    account = None
                    unknown = (
                        UnknownActiveRecord(
                            self.venue,
                            "ACCOUNT",
                            "NOT_A_MAPPING",
                            _raw_payload(raw),
                        ),
                    )
                else:
                    try:
                        account = _normalise_stream_account(self.venue, raw)
                    except (TypeError, ValueError) as error:
                        account = None
                        unknown = (
                            UnknownActiveRecord(
                                self.venue,
                                "ACCOUNT",
                                f"{type(error).__name__}:{error}",
                                _raw_payload(raw),
                            ),
                        )
                    else:
                        unknown = ()
                return PrivateStreamEvent(
                    self.venue,
                    PrivateStreamKind.ACCOUNT,
                    watermark,
                    datetime.now(UTC),
                    source_ns,
                    account=account,
                    unknown_active_records=unknown,
                )
        except (Exception, asyncio.CancelledError):
            self._private_events_pending.discard(watermark)
            raise

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

    async def fetch_open_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        if not self._has("fetchOpenOrders"):
            raise RuntimeError("fetchOpenOrders is unavailable")
        raw = await self._exchange.fetch_open_orders(instrument.symbol)
        return _normalise_orders(self.venue, raw, instrument)

    async def fetch_all_open_orders(self) -> tuple[PrivateOrder, ...]:
        return (await self.fetch_active_snapshot()).open_orders

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        if not self._has("fetchClosedOrders"):
            raise RuntimeError("fetchClosedOrders is unavailable")
        raw = await self._exchange.fetch_closed_orders(instrument.symbol)
        return _normalise_orders(self.venue, raw, instrument)

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]:
        return (await self.fetch_active_snapshot()).positions

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        if not self._has("fetchOpenOrders") or not self._has("fetchPositions"):
            raise RuntimeError("complete private active snapshot capability is unavailable")
        instruments = await self._linear_instruments()
        order_limit, position_limit = _account_wide_snapshot_limits(self.venue)

        async def sample() -> tuple[tuple[object, ...], tuple[object, ...]]:
            order_params = _account_wide_snapshot_params(
                self.venue,
                PrivateStreamKind.ORDERS,
            )
            position_params = _account_wide_snapshot_params(
                self.venue,
                PrivateStreamKind.POSITIONS,
            )
            if position_limit is not None:
                position_params["limit"] = position_limit
            raw_orders, raw_positions = await asyncio.gather(
                self._exchange.fetch_open_orders(None, None, order_limit, order_params),
                self._exchange.fetch_positions(None, position_params),
            )
            return (
                _require_sequence(raw_orders, "fetch_open_orders"),
                _require_sequence(raw_positions, "fetch_positions"),
            )

        started_ns = time.monotonic_ns()
        before_watermark = self._private_event_watermark
        before_pending = tuple(sorted(self._private_events_pending))
        first_orders, first_positions = await sample()
        middle_watermark = self._private_event_watermark
        middle_pending = tuple(sorted(self._private_events_pending))
        second_orders, second_positions = await sample()
        after_watermark = self._private_event_watermark
        after_pending = tuple(sorted(self._private_events_pending))
        latency_ms = Decimal(time.monotonic_ns() - started_ns) / Decimal(1_000_000)
        first = _normalise_active_snapshot(
            self.venue,
            first_orders,
            first_positions,
            instruments,
            event_watermark=middle_watermark,
            request_count=4,
            latency_ms=latency_ms,
            account_wide=True,
            additional_unknown=_page_warnings(
                self.venue,
                first_orders,
                first_positions,
                order_limit,
                position_limit,
                "FIRST",
            ),
        )
        second = _normalise_active_snapshot(
            self.venue,
            second_orders,
            second_positions,
            instruments,
            event_watermark=after_watermark,
            request_count=4,
            latency_ms=latency_ms,
            account_wide=True,
            additional_unknown=_page_warnings(
                self.venue,
                second_orders,
                second_positions,
                order_limit,
                position_limit,
                "SECOND",
            ),
        )
        samples_match = _active_snapshot_signature(first) == _active_snapshot_signature(second)
        if first.completeness != SnapshotCompleteness.COMPLETE:
            second = _snapshot_unknown(second, "FIRST_ACCOUNT_WIDE_SAMPLE_INCOMPLETE")
        if not samples_match:
            second = _snapshot_unknown(second, "ACCOUNT_WIDE_SNAPSHOT_UNSTABLE")
        if not (before_watermark == middle_watermark == after_watermark):
            second = _snapshot_unknown(second, "PRIVATE_EVENT_DURING_ACCOUNT_WIDE_SNAPSHOT")
        if before_pending or middle_pending or after_pending:
            second = _snapshot_unknown(second, "PRIVATE_EVENT_DELIVERY_PENDING")
        return second

    async def resolve_instrument(self, symbol: str) -> Instrument | None:
        return (await self._linear_instruments()).get(symbol)

    async def list_instruments(self) -> tuple[Instrument, ...]:
        return tuple((await self._linear_instruments()).values())

    async def _linear_instruments(self) -> dict[str, Instrument]:
        raw_markets = await self._exchange.load_markets()
        if not isinstance(raw_markets, Mapping):
            raise TypeError("CCXT load_markets must return a mapping")
        instruments = (
            instrument
            for raw in raw_markets.values()
            if isinstance(raw, Mapping)
            and (instrument := normalize_market(self.venue, raw)) is not None
        )
        return {instrument.symbol: instrument for instrument in instruments}

    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        if self.venue == Venue.MEXC:
            raise RuntimeError("MEXC contract order submission is not qualified")
        if self._production_transport:
            _enforce_production_submit_guard()
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
        if self.venue == Venue.MEXC:
            raise RuntimeError("MEXC contract order cancellation is not qualified")
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


def _normalise_multi_instrument_orders(
    venue: Venue,
    raw: object,
    instruments: Mapping[str, Instrument],
) -> tuple[PrivateOrder, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("CCXT all-orders result must be a sequence")
    orders: list[PrivateOrder] = []
    for value in raw:
        if not isinstance(value, Mapping):
            continue
        instrument = instruments.get(str(value.get("symbol")))
        if instrument is not None:
            orders.append(_normalise_order(venue, value, instrument, "unknown-client-id"))
    return tuple(orders)


def _normalise_account_order_updates(
    venue: Venue,
    raw: object,
    instruments: Mapping[str, Instrument],
) -> tuple[tuple[PrivateOrder, ...], tuple[UnknownActiveRecord, ...]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return (), (UnknownActiveRecord(venue, "OPEN_ORDER", "NOT_A_SEQUENCE", _raw_payload(raw)),)
    orders: list[PrivateOrder] = []
    unknown: list[UnknownActiveRecord] = []
    for value in raw:
        if not isinstance(value, Mapping):
            unknown.append(
                UnknownActiveRecord(venue, "OPEN_ORDER", "NOT_A_MAPPING", _raw_payload(value))
            )
            continue
        instrument = instruments.get(str(value.get("symbol")))
        if instrument is None:
            unknown.append(
                UnknownActiveRecord(venue, "OPEN_ORDER", "UNKNOWN_SYMBOL", _raw_payload(value))
            )
            continue
        try:
            orders.append(_normalise_order(venue, value, instrument, "unknown-client-id"))
        except (TypeError, ValueError) as error:
            unknown.append(
                UnknownActiveRecord(
                    venue,
                    "OPEN_ORDER",
                    f"{type(error).__name__}:{error}",
                    _raw_payload(value),
                )
            )
    return tuple(orders), tuple(unknown)


def _normalise_account_position_updates(
    venue: Venue,
    raw: object,
    instruments: Mapping[str, Instrument],
) -> tuple[tuple[PositionSnapshot, ...], tuple[UnknownActiveRecord, ...]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return (), (UnknownActiveRecord(venue, "POSITION", "NOT_A_SEQUENCE", _raw_payload(raw)),)
    positions: list[PositionSnapshot] = []
    unknown: list[UnknownActiveRecord] = []
    for value in raw:
        if not isinstance(value, Mapping):
            unknown.append(
                UnknownActiveRecord(venue, "POSITION", "NOT_A_MAPPING", _raw_payload(value))
            )
            continue
        instrument = instruments.get(str(value.get("symbol")))
        if instrument is None:
            unknown.append(
                UnknownActiveRecord(venue, "POSITION", "UNKNOWN_SYMBOL", _raw_payload(value))
            )
            continue
        contracts = _decimal(value.get("contracts"))
        side_value = str(value.get("side", "")).lower()
        side = Side.BUY if side_value == "long" else Side.SELL if side_value == "short" else None
        tombstone_sides: tuple[Side, ...] = ()
        if contracts == 0 and side is None:
            if venue == Venue.KUCOIN_FUTURES:
                position_side = str(_mapping(value.get("info")).get("positionSide", "")).upper()
                if position_side == "LONG":
                    tombstone_sides = (Side.BUY,)
                elif position_side == "SHORT":
                    tombstone_sides = (Side.SELL,)
                elif position_side == "BOTH":
                    tombstone_sides = (Side.BUY, Side.SELL)
            elif venue == Venue.BYBIT or (venue == Venue.BINANCE_USDM and side_value == "both"):
                tombstone_sides = (Side.BUY, Side.SELL)
        if tombstone_sides:
            positions.extend(
                PositionSnapshot(
                    venue,
                    instrument.symbol,
                    tombstone_side,
                    Decimal(0),
                    _decimal(value.get("entryPrice")),
                    _decimal(value.get("markPrice")),
                    datetime.now(UTC),
                )
                for tombstone_side in tombstone_sides
            )
            continue
        if contracts is None or side is None:
            unknown.append(
                UnknownActiveRecord(
                    venue,
                    "POSITION",
                    "QUANTITY_OR_SIDE_UNKNOWN",
                    _raw_payload(value),
                )
            )
            continue
        positions.append(
            PositionSnapshot(
                venue,
                instrument.symbol,
                side,
                abs(contracts) * instrument.contract_size_base,
                _decimal(value.get("entryPrice")),
                _decimal(value.get("markPrice")),
                datetime.now(UTC),
            )
        )
    return tuple(positions), tuple(unknown)


def _normalise_stream_account(venue: Venue, raw: Mapping[str, object]) -> AccountSnapshot:
    total = _currency_value(raw, "total", "USDT")
    free = _currency_value(raw, "free", "USDT")
    info = _mapping(raw.get("info"))
    if total is None or free is None:
        raise ValueError("USDT equity/free margin is unavailable")
    trading_enabled = _optional_bool(
        raw.get("tradingEnabled") if raw.get("tradingEnabled") is not None else info.get("canTrade")
    )
    permissions_value = raw.get("permissions") or info.get("permissions")
    permissions = (
        {str(value).lower() for value in permissions_value}
        if isinstance(permissions_value, Sequence)
        and not isinstance(permissions_value, (str, bytes))
        else set()
    )
    if trading_enabled is True:
        permissions.add("trade")
    return AccountSnapshot(
        venue,
        total,
        free,
        _string_or_none(raw.get("marginMode") or info.get("marginMode")),
        _string_or_none(raw.get("positionMode") or info.get("positionMode")),
        trading_enabled,
        tuple(sorted(permissions)),
        datetime.now(UTC),
        _optional_bool(
            raw.get("withdrawalEnabled")
            if raw.get("withdrawalEnabled") is not None
            else info.get("canWithdraw")
        ),
        _optional_bool(
            raw.get("transferEnabled")
            if raw.get("transferEnabled") is not None
            else info.get("canTransfer")
        ),
    )


def _require_sequence(raw: object, operation: str) -> tuple[object, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError(f"CCXT {operation} must return a sequence")
    return tuple(raw)


def _account_wide_snapshot_params(
    venue: Venue,
    kind: PrivateStreamKind,
) -> dict[str, object]:
    if venue == Venue.BITGET:
        params: dict[str, object] = {"productType": "USDT-FUTURES"}
        if kind == PrivateStreamKind.POSITIONS:
            params["marginCoin"] = "USDT"
        return params
    if venue == Venue.KUCOIN_FUTURES:
        if kind == PrivateStreamKind.POSITIONS:
            return {"uta": False}
        return {"type": "swap", "uta": False}
    if venue == Venue.BINGX:
        if kind == PrivateStreamKind.POSITIONS:
            return {"subType": "linear"}
        return {"type": "swap", "subType": "linear"}
    if venue == Venue.MEXC:
        return {"type": "swap"}
    if venue == Venue.BYBIT:
        return {"category": "linear", "settleCoin": "USDT"}
    if venue == Venue.OKX:
        return {"instType": "SWAP"}
    if venue == Venue.BINANCE_USDM:
        return {"type": "future"}
    raise ValueError(f"account-wide private snapshot is not qualified for {venue.value}")


def _account_wide_snapshot_limits(venue: Venue) -> tuple[int | None, int | None]:
    if venue == Venue.BITGET:
        return 100, None
    if venue == Venue.KUCOIN_FUTURES:
        return 50, None
    if venue == Venue.BINGX:
        return None, None
    if venue == Venue.MEXC:
        return None, None
    if venue == Venue.BYBIT:
        return 50, 200
    if venue == Venue.OKX:
        return 100, None
    if venue == Venue.BINANCE_USDM:
        return None, None
    raise ValueError(f"account-wide private snapshot is not qualified for {venue.value}")


def _account_wide_stream_params(
    venue: Venue,
    kind: PrivateStreamKind,
) -> dict[str, object]:
    if venue == Venue.BITGET:
        if kind == PrivateStreamKind.ORDERS:
            return {
                "type": "swap",
                "subType": "linear",
                "productType": "USDT-FUTURES",
                "uta": False,
            }
        if kind == PrivateStreamKind.POSITIONS:
            return {"instType": "USDT-FUTURES", "uta": False}
        return {"type": "swap", "instType": "USDT-FUTURES", "uta": False}
    if venue == Venue.KUCOIN_FUTURES:
        if kind == PrivateStreamKind.POSITIONS:
            return {"uta": False}
        return {"type": "swap", "uta": False}
    if venue == Venue.BINGX:
        return {"type": "swap", "subType": "linear"}
    if venue == Venue.MEXC:
        return {"type": "swap"}
    if venue == Venue.BYBIT:
        # The configured CCXT transport already selects swap/linear. Unconsumed params are
        # merged into Bybit's subscribe frame, whose schema only permits op/req_id/args.
        return {}
    if venue == Venue.OKX:
        if kind == PrivateStreamKind.ORDERS:
            return {"type": "swap"}
        if kind == PrivateStreamKind.POSITIONS:
            return {"instType": "SWAP"}
        return {}
    if venue == Venue.BINANCE_USDM:
        return {"type": "future"}
    raise ValueError(f"account-wide private stream is not qualified for {venue.value}")


def _page_warnings(
    venue: Venue,
    raw_orders: tuple[object, ...],
    raw_positions: tuple[object, ...],
    order_limit: int | None,
    position_limit: int | None,
    sample: str,
) -> tuple[UnknownActiveRecord, ...]:
    warnings: list[UnknownActiveRecord] = []
    for kind, values, limit in (
        ("OPEN_ORDER", raw_orders, order_limit),
        ("POSITION", raw_positions, position_limit),
    ):
        cursor = _next_page_cursor(values)
        if cursor is not None:
            warnings.append(
                UnknownActiveRecord(
                    venue,
                    kind,
                    "ACCOUNT_WIDE_RESULT_HAS_MORE_PAGES",
                    {"sample": sample, "next_page_cursor": cursor},
                )
            )
        elif limit is not None and len(values) >= limit:
            warnings.append(
                UnknownActiveRecord(
                    venue,
                    kind,
                    "ACCOUNT_WIDE_RESULT_AT_PAGE_LIMIT",
                    {"sample": sample, "observed_count": len(values), "page_limit": limit},
                )
            )
    return tuple(warnings)


def _next_page_cursor(values: tuple[object, ...]) -> str | None:
    for value in values:
        if not isinstance(value, Mapping):
            continue
        info = _mapping(value.get("info"))
        cursor = value.get("nextPageCursor") or info.get("nextPageCursor")
        if cursor is not None and str(cursor).strip():
            return str(cursor)
    return None


def _raw_payload(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {"unparsed_value": repr(value)}


def _normalise_active_snapshot(
    venue: Venue,
    raw_orders: tuple[object, ...],
    raw_positions: tuple[object, ...],
    instruments: Mapping[str, Instrument],
    *,
    event_watermark: int = 0,
    request_count: int = 0,
    latency_ms: Decimal = Decimal(0),
    account_wide: bool = False,
    additional_unknown: tuple[UnknownActiveRecord, ...] = (),
) -> PrivateActiveSnapshot:
    orders: list[PrivateOrder] = []
    positions: list[PositionSnapshot] = []
    unknown = list(additional_unknown)
    for value in raw_orders:
        if not isinstance(value, Mapping):
            unknown.append(
                UnknownActiveRecord(venue, "OPEN_ORDER", "NOT_A_MAPPING", _raw_payload(value))
            )
            continue
        instrument = instruments.get(str(value.get("symbol")))
        if instrument is None:
            unknown.append(
                UnknownActiveRecord(venue, "OPEN_ORDER", "UNKNOWN_SYMBOL", _raw_payload(value))
            )
            continue
        try:
            orders.append(_normalise_order(venue, value, instrument, "unknown-client-id"))
        except (TypeError, ValueError) as error:
            unknown.append(
                UnknownActiveRecord(
                    venue,
                    "OPEN_ORDER",
                    f"{type(error).__name__}:{error}",
                    _raw_payload(value),
                )
            )

    raw_nonzero_positions = 0
    for value in raw_positions:
        if not isinstance(value, Mapping):
            raw_nonzero_positions += 1
            unknown.append(
                UnknownActiveRecord(venue, "POSITION", "NOT_A_MAPPING", _raw_payload(value))
            )
            continue
        contracts = _decimal(value.get("contracts"))
        if contracts == 0:
            continue
        raw_nonzero_positions += 1
        instrument = instruments.get(str(value.get("symbol")))
        if instrument is None:
            unknown.append(
                UnknownActiveRecord(venue, "POSITION", "UNKNOWN_SYMBOL", _raw_payload(value))
            )
            continue
        try:
            position = _normalise_position(venue, value, instrument)
        except (TypeError, ValueError) as error:
            position = None
            reason = f"{type(error).__name__}:{error}"
        else:
            reason = "MALFORMED_OR_UNKNOWN_SIDE"
        if position is None:
            unknown.append(UnknownActiveRecord(venue, "POSITION", reason, _raw_payload(value)))
        else:
            positions.append(position)

    complete = (
        not unknown and len(raw_orders) == len(orders) and raw_nonzero_positions == len(positions)
    )
    return PrivateActiveSnapshot(
        venue=venue,
        raw_open_order_count=len(raw_orders),
        raw_nonzero_position_count=raw_nonzero_positions,
        open_orders=tuple(orders),
        positions=tuple(positions),
        unknown_active_records=tuple(unknown),
        completeness=(SnapshotCompleteness.COMPLETE if complete else SnapshotCompleteness.UNKNOWN),
        observed_at=datetime.now(UTC),
        event_watermark=event_watermark,
        request_count=request_count,
        latency_ms=latency_ms,
        account_wide=account_wide,
    )


def _snapshot_unknown(
    snapshot: PrivateActiveSnapshot,
    reason: str,
) -> PrivateActiveSnapshot:
    record = UnknownActiveRecord(
        snapshot.venue,
        "SNAPSHOT",
        reason,
        {"event_watermark": snapshot.event_watermark},
    )
    return replace(
        snapshot,
        unknown_active_records=(*snapshot.unknown_active_records, record),
        completeness=SnapshotCompleteness.UNKNOWN,
    )


def _active_snapshot_signature(snapshot: PrivateActiveSnapshot) -> tuple[object, ...]:
    return (
        snapshot.raw_open_order_count,
        snapshot.raw_nonzero_position_count,
        tuple(
            sorted(
                (
                    order.order_id or "",
                    order.client_order_id,
                    order.symbol,
                    order.side.value,
                    order.status.value,
                    str(order.requested_base_quantity),
                    str(order.filled_base_quantity),
                )
                for order in snapshot.open_orders
            )
        ),
        tuple(
            sorted(
                (
                    position.symbol,
                    position.side.value,
                    str(position.base_quantity),
                )
                for position in snapshot.positions
            )
        ),
        tuple(
            sorted(
                (
                    record.kind,
                    record.reason,
                    repr(
                        sorted(
                            (key, value)
                            for key, value in record.raw_record.items()
                            if key != "sample"
                        )
                    ),
                )
                for record in snapshot.unknown_active_records
            )
        ),
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
        limit_price=_decimal(raw.get("price")),
    )


def _order_status(raw: str, filled: Decimal, amount: Decimal) -> PrivateOrderStatus:
    normalized = raw.lower()
    if normalized == "filled" or filled == amount:
        return PrivateOrderStatus.FILLED
    if normalized in {"canceled", "cancelled", "expired"}:
        return PrivateOrderStatus.PARTIAL if filled > 0 else PrivateOrderStatus.CANCELLED
    if normalized in {"rejected"}:
        return PrivateOrderStatus.REJECTED
    if filled > 0:
        return PrivateOrderStatus.PARTIAL
    if normalized == "closed":
        return PrivateOrderStatus.CANCELLED
    if normalized in {"open", "new"}:
        return PrivateOrderStatus.OPEN
    return PrivateOrderStatus.UNKNOWN
