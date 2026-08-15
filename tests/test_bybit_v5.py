from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import pytest

from interexchange_perp_grid.adapters.bybit_v5 import (
    BybitSequenceError,
    BybitV5OrderBookAssembler,
    BybitV5SequenceGuard,
    SequenceQualifiedBybitExchange,
)
from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.market_data import BookRegistry


def instrument() -> Instrument:
    return Instrument(
        venue=Venue.BYBIT,
        symbol="BTC/USDT:USDT",
        exchange_symbol="BTCUSDT",
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal("0.001"),
        amount_step_contracts=Decimal("0.001"),
        price_tick=Decimal("0.1"),
        minimum_amount_contracts=Decimal("0.001"),
        minimum_notional=Decimal("5"),
        taker_fee_rate=Decimal("0.00055"),
        fee_source="fixture",
    )


def message(
    message_type: str,
    update_id: int,
    cross_sequence: int,
    *,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
) -> dict[str, object]:
    return {
        "topic": "orderbook.50.BTCUSDT",
        "type": message_type,
        "ts": 1_700_000_000_000,
        "data": {
            "s": "BTCUSDT",
            "b": bids if bids is not None else [["100", "2"], ["99", "3"]],
            "a": asks if asks is not None else [["101", "4"], ["102", "5"]],
            "u": update_id,
            "seq": cross_sequence,
        },
    }


def test_snapshot_and_delta_use_native_u_seq_and_contract_multiplier() -> None:
    assembler = BybitV5OrderBookAssembler(instrument(), clock_skew_ms=0)
    registry = BookRegistry()

    snapshot = assembler.apply(message("snapshot", 100, 1000))
    delta = assembler.apply(
        message(
            "delta",
            105,
            1001,
            bids=[["100", "0"], ["98", "7"]],
            asks=[["101", "6"], ["103", "8"]],
        )
    )

    assert snapshot.is_snapshot is True
    assert snapshot.sequence_start == snapshot.sequence_end == 100
    assert snapshot.bids[0].price == Decimal("100")
    assert snapshot.bids[0].base_quantity == Decimal("0.002")
    assert delta.is_snapshot is False
    assert delta.sequence_start == delta.sequence_end == 105
    assert delta.sequence_contiguous is False
    assert tuple(level.price for level in delta.bids) == (Decimal("99"), Decimal("98"))
    assert delta.asks[0].base_quantity == Decimal("0.006")
    assert registry.accept(snapshot, max_age_ms=1000, max_clock_skew_ms=1000).accepted
    assert registry.accept(delta, max_age_ms=1000, max_clock_skew_ms=1000).accepted


def test_ccxt_factory_installs_native_sequence_qualified_bybit_transport() -> None:
    exchange = CcxtProAdapter._build_exchange(Venue.BYBIT)

    assert isinstance(exchange, SequenceQualifiedBybitExchange)


def test_ccxt_native_handler_publishes_qualified_sequence_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange = SequenceQualifiedBybitExchange()
    parent = SequenceQualifiedBybitExchange.__mro__[1]
    observed_types: list[str] = []

    def fake_parent_handler(
        target: Any,
        client: object,
        raw: Mapping[str, object],
    ) -> None:
        del client
        observed_types.append(str(raw["type"]))
        target.orderbooks["BTC/USDT:USDT"] = {}

    monkeypatch.setattr(parent, "handle_order_book", fake_parent_handler)
    monkeypatch.setattr(
        exchange,
        "safe_market",
        lambda *args, **kwargs: {"symbol": "BTC/USDT:USDT"},
    )

    exchange.handle_order_book(object(), message("snapshot", 100, 1000))
    orderbook = exchange.orderbooks["BTC/USDT:USDT"]

    assert observed_types == ["snapshot"]
    assert orderbook["nonce"] == 100
    assert orderbook["ipegCrossSequence"] == 1000
    assert orderbook["ipegSequenceReset"] is False
    assert orderbook["ipegSequenceContiguous"] is False

    exchange.handle_order_book(object(), message("delta", 1, 5))
    orderbook = exchange.orderbooks["BTC/USDT:USDT"]
    assert observed_types[-1] == "snapshot"
    assert orderbook["ipegSequenceReset"] is True


def test_raw_sequence_guard_allows_jumps_and_rejects_regression() -> None:
    guard = BybitV5SequenceGuard()

    qualified, update_id, cross_sequence = guard.qualify(message("snapshot", 100, 1000))

    assert qualified["type"] == "snapshot"
    assert update_id == 100
    assert cross_sequence == 1000
    jumped, update_id, cross_sequence = guard.qualify(message("delta", 105, 1001))
    assert jumped["type"] == "delta"
    assert update_id == 105
    assert cross_sequence == 1001
    with pytest.raises(BybitSequenceError, match="BYBIT_UPDATE_ID_REGRESSION"):
        guard.qualify(message("delta", 105, 1002))
    with pytest.raises(BybitSequenceError, match="BYBIT_DELTA_BEFORE_SNAPSHOT"):
        guard.qualify(message("delta", 106, 1003))

    restarted, update_id, _ = guard.qualify(message("delta", 1, 5))
    assert restarted["type"] == "snapshot"
    assert update_id == 1


def test_update_and_cross_sequence_regressions_clear_local_book() -> None:
    assembler = BybitV5OrderBookAssembler(instrument(), clock_skew_ms=0)
    assembler.apply(message("snapshot", 100, 1000))

    jumped = assembler.apply(message("delta", 105, 1001))
    assert jumped.sequence_end == 105
    with pytest.raises(BybitSequenceError, match="BYBIT_UPDATE_ID_REGRESSION"):
        assembler.apply(message("delta", 105, 1002))
    with pytest.raises(BybitSequenceError, match="BYBIT_DELTA_BEFORE_SNAPSHOT"):
        assembler.apply(message("delta", 106, 1003))

    assembler.apply(message("snapshot", 200, 2000))
    with pytest.raises(BybitSequenceError, match="BYBIT_CROSS_SEQUENCE_REGRESSION"):
        assembler.apply(message("delta", 201, 1999))
    with pytest.raises(BybitSequenceError, match="BYBIT_DELTA_BEFORE_SNAPSHOT"):
        assembler.apply(message("delta", 202, 2001))


def test_ccxt_parent_failure_invalidates_guard_and_discards_partial_book(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange = SequenceQualifiedBybitExchange()
    parent = SequenceQualifiedBybitExchange.__mro__[1]
    calls = 0

    def failing_parent_handler(
        target: Any,
        client: object,
        raw: Mapping[str, object],
    ) -> None:
        nonlocal calls
        del client, raw
        calls += 1
        target.orderbooks["BTC/USDT:USDT"] = {"partial": True}
        raise TypeError("synthetic parent assembly failure")

    monkeypatch.setattr(parent, "handle_order_book", failing_parent_handler)
    monkeypatch.setattr(
        exchange,
        "safe_market",
        lambda *args, **kwargs: {"symbol": "BTC/USDT:USDT"},
    )

    with pytest.raises(TypeError, match="synthetic parent assembly failure"):
        exchange.handle_order_book(object(), message("snapshot", 100, 1000))
    assert "BTC/USDT:USDT" not in exchange.orderbooks
    with pytest.raises(BybitSequenceError, match="BYBIT_DELTA_BEFORE_SNAPSHOT"):
        exchange.handle_order_book(object(), message("delta", 105, 1001))
    assert calls == 1


def test_u_one_resets_book_even_when_message_is_labelled_delta() -> None:
    assembler = BybitV5OrderBookAssembler(instrument(), clock_skew_ms=0)
    registry = BookRegistry()
    original = assembler.apply(message("snapshot", 100, 1000))
    assert registry.accept(original, max_age_ms=1000, max_clock_skew_ms=1000).accepted

    restarted = assembler.apply(message("delta", 1, 5, bids=[["90", "1"]], asks=[["91", "1"]]))

    assert restarted.is_snapshot is True
    assert restarted.sequence_reset is True
    assert tuple(level.price for level in restarted.bids) == (Decimal("90"),)
    assert tuple(level.price for level in restarted.asks) == (Decimal("91"),)
    assert registry.accept(restarted, max_age_ms=1000, max_clock_skew_ms=1000).accepted


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ({"topic": "orderbook.200.BTCUSDT"}, "BYBIT_ORDERBOOK_TOPIC_MISMATCH"),
        ({"data": {"s": "ETHUSDT"}}, "BYBIT_ORDERBOOK_SYMBOL_MISMATCH"),
        ({"data": {"b": [["100", "not-a-number"]]}}, "BYBIT_QUANTITY_INVALID"),
    ],
)
def test_malformed_message_fails_closed_and_requires_new_snapshot(
    mutation: dict[str, object],
    reason: str,
) -> None:
    assembler = BybitV5OrderBookAssembler(instrument(), clock_skew_ms=0)
    assembler.apply(message("snapshot", 100, 1000))
    malformed = message("delta", 101, 1001)
    if "data" in mutation:
        data = malformed["data"]
        assert isinstance(data, dict)
        nested = mutation["data"]
        assert isinstance(nested, dict)
        data.update(nested)
    else:
        malformed.update(mutation)

    with pytest.raises(BybitSequenceError, match=reason):
        assembler.apply(malformed)
    with pytest.raises(BybitSequenceError, match="BYBIT_DELTA_BEFORE_SNAPSHOT"):
        assembler.apply(message("delta", 102, 1002))
