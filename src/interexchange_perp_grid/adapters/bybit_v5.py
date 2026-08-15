from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, NoReturn

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]

from interexchange_perp_grid.domain import BookLevel, Instrument, OrderBookSnapshot, Venue


class BybitSequenceError(RuntimeError):
    pass


class BybitV5SequenceGuard:
    """Validate documented raw V5 monotonicity before CCXT mutates its local book."""

    def __init__(self) -> None:
        self._sequences: dict[str, tuple[int, int]] = {}

    def qualify(
        self,
        message: Mapping[str, object],
        *,
        commit: bool = True,
    ) -> tuple[dict[str, object], int, int]:
        topic = str(message.get("topic", ""))
        try:
            message_type = str(message.get("type", ""))
            data = message.get("data")
            if (
                not topic.startswith("orderbook.")
                or message_type not in {"snapshot", "delta"}
                or not isinstance(data, Mapping)
            ):
                raise BybitSequenceError("BYBIT_ORDERBOOK_MESSAGE_INVALID")
            update_id = _positive_int(data.get("u"), "u")
            cross_sequence = _positive_int(data.get("seq"), "seq")
            snapshot_reset = message_type == "snapshot" or update_id == 1
            previous = self._sequences.get(topic)
            if not snapshot_reset:
                if previous is None:
                    raise BybitSequenceError("BYBIT_DELTA_BEFORE_SNAPSHOT")
                if update_id <= previous[0]:
                    raise BybitSequenceError("BYBIT_UPDATE_ID_REGRESSION")
                if cross_sequence <= previous[1]:
                    raise BybitSequenceError("BYBIT_CROSS_SEQUENCE_REGRESSION")
            if commit:
                self._sequences[topic] = (update_id, cross_sequence)
            qualified = {str(key): value for key, value in message.items()}
            if snapshot_reset:
                qualified["type"] = "snapshot"
            return qualified, update_id, cross_sequence
        except BybitSequenceError:
            self._sequences.pop(topic, None)
            raise

    def commit(self, topic: str, update_id: int, cross_sequence: int) -> None:
        if not topic:
            raise BybitSequenceError("BYBIT_ORDERBOOK_MESSAGE_INVALID")
        self._sequences[topic] = (update_id, cross_sequence)

    def invalidate(self, topic: str) -> None:
        self._sequences.pop(topic, None)


class SequenceQualifiedBybitExchange(ccxtpro.bybit):  # type: ignore[misc]
    """CCXT Pro transport with native V5 u/seq qualification before assembly."""

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        super().__init__(dict(config or {}))
        self._ipeg_sequence_guard = BybitV5SequenceGuard()

    def handle_order_book(self, client: Any, message: Mapping[str, object]) -> None:
        topic = str(message.get("topic", ""))
        qualified, update_id, cross_sequence = self._ipeg_sequence_guard.qualify(
            message,
            commit=False,
        )
        data = qualified.get("data")
        if not isinstance(data, Mapping):
            self._ipeg_sequence_guard.invalidate(topic)
            raise BybitSequenceError("BYBIT_ORDERBOOK_MESSAGE_INVALID")
        symbol: str | None = None
        try:
            market = self.safe_market(str(data.get("s", "")), None, None, "contract")
            symbol = str(market["symbol"])
            super().handle_order_book(client, qualified)
            orderbook = self.orderbooks[symbol]
            orderbook["nonce"] = update_id
            orderbook["ipegCrossSequence"] = cross_sequence
            orderbook["ipegSequenceReset"] = update_id == 1
            orderbook["ipegSequenceContiguous"] = False
        except Exception:
            self._ipeg_sequence_guard.invalidate(topic)
            if symbol is not None:
                self.orderbooks.pop(symbol, None)
            raise
        self._ipeg_sequence_guard.commit(topic, update_id, cross_sequence)


class BybitV5OrderBookAssembler:
    """Native Bybit V5 snapshot/delta assembler with fail-closed u/seq monotonicity."""

    def __init__(
        self,
        instrument: Instrument,
        *,
        depth: int = 50,
        clock_skew_ms: int | None = None,
    ) -> None:
        if instrument.venue != Venue.BYBIT:
            raise ValueError("Bybit V5 assembler requires a Bybit instrument")
        if depth not in {50, 200, 1000}:
            raise ValueError("Bybit sequence-qualified depth must be 50, 200, or 1000")
        self._instrument = instrument
        self._depth = depth
        self._clock_skew_ms = clock_skew_ms
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._last_update_id: int | None = None
        self._last_cross_sequence: int | None = None
        self._synchronised = False

    def apply(self, message: Mapping[str, object]) -> OrderBookSnapshot:
        try:
            return self._apply_message(message)
        except BybitSequenceError:
            self._reset()
            raise

    def _apply_message(self, message: Mapping[str, object]) -> OrderBookSnapshot:
        topic = str(message.get("topic", ""))
        expected_topic = f"orderbook.{self._depth}.{self._instrument.exchange_symbol}"
        if topic != expected_topic:
            return self._fail("BYBIT_ORDERBOOK_TOPIC_MISMATCH")
        message_type = str(message.get("type", ""))
        data = message.get("data")
        if message_type not in {"snapshot", "delta"} or not isinstance(data, Mapping):
            return self._fail("BYBIT_ORDERBOOK_MESSAGE_INVALID")
        if str(data.get("s", "")) != self._instrument.exchange_symbol:
            return self._fail("BYBIT_ORDERBOOK_SYMBOL_MISMATCH")
        update_id = _positive_int(data.get("u"), "u")
        cross_sequence = _positive_int(data.get("seq"), "seq")
        snapshot_reset = message_type == "snapshot" or update_id == 1
        if snapshot_reset:
            self._bids = _levels(data.get("b"), self._instrument)
            self._asks = _levels(data.get("a"), self._instrument)
        else:
            if not self._synchronised or self._last_update_id is None:
                return self._fail("BYBIT_DELTA_BEFORE_SNAPSHOT")
            if update_id <= self._last_update_id:
                return self._fail("BYBIT_UPDATE_ID_REGRESSION")
            if self._last_cross_sequence is None or cross_sequence <= self._last_cross_sequence:
                return self._fail("BYBIT_CROSS_SEQUENCE_REGRESSION")
            _apply_delta(self._bids, data.get("b"), self._instrument)
            _apply_delta(self._asks, data.get("a"), self._instrument)
        if not self._bids or not self._asks:
            return self._fail("BYBIT_ORDERBOOK_EMPTY")
        self._last_update_id = update_id
        self._last_cross_sequence = cross_sequence
        self._synchronised = True
        received_at = datetime.now(UTC)
        return OrderBookSnapshot(
            venue=Venue.BYBIT,
            symbol=self._instrument.symbol,
            bids=tuple(
                BookLevel(price, quantity)
                for price, quantity in sorted(self._bids.items(), reverse=True)[: self._depth]
            ),
            asks=tuple(
                BookLevel(price, quantity)
                for price, quantity in sorted(self._asks.items())[: self._depth]
            ),
            exchange_timestamp_ms=_optional_int(message.get("ts")),
            received_at=received_at,
            received_monotonic_ns=time.monotonic_ns(),
            sequence_start=update_id,
            sequence_end=update_id,
            is_snapshot=snapshot_reset,
            synchronised=True,
            clock_skew_ms=self._clock_skew_ms,
            sequence_reset=update_id == 1,
            sequence_contiguous=False,
        )

    def _fail(self, reason: str) -> NoReturn:
        raise BybitSequenceError(reason)

    def _reset(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = None
        self._last_cross_sequence = None
        self._synchronised = False


def _levels(raw: object, instrument: Instrument) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for price, quantity in _raw_levels(raw):
        if quantity > 0:
            result[price] = quantity * instrument.contract_size_base
    return result


def _apply_delta(
    book: dict[Decimal, Decimal],
    raw: object,
    instrument: Instrument,
) -> None:
    for price, quantity in _raw_levels(raw):
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity * instrument.contract_size_base


def _raw_levels(raw: object) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise BybitSequenceError("BYBIT_ORDERBOOK_LEVELS_INVALID")
    levels: list[tuple[Decimal, Decimal]] = []
    for item in raw:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)) or len(item) < 2:
            raise BybitSequenceError("BYBIT_ORDERBOOK_LEVEL_INVALID")
        price = _decimal(item[0], "price")
        quantity = _decimal(item[1], "quantity")
        if price <= 0 or quantity < 0:
            raise BybitSequenceError("BYBIT_ORDERBOOK_LEVEL_OUT_OF_RANGE")
        levels.append((price, quantity))
    return tuple(levels)


def _decimal(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise BybitSequenceError(f"BYBIT_{field.upper()}_INVALID") from error
    if not result.is_finite():
        raise BybitSequenceError(f"BYBIT_{field.upper()}_INVALID")
    return result


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise BybitSequenceError(f"BYBIT_{field.upper()}_INVALID")
    try:
        result = int(str(value))
    except ValueError as error:
        raise BybitSequenceError(f"BYBIT_{field.upper()}_INVALID") from error
    if result <= 0:
        raise BybitSequenceError(f"BYBIT_{field.upper()}_INVALID")
    return result


def _optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except ValueError:
        return None
