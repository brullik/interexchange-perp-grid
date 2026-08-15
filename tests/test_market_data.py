from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal

from interexchange_perp_grid.domain import BookLevel, OrderBookSnapshot, Venue
from interexchange_perp_grid.market_data import BookRegistry
from interexchange_perp_grid.reason_codes import ReasonCode


def make_book(
    *,
    sequence_start: int | None = 10,
    sequence_end: int | None = 10,
    is_snapshot: bool = True,
    received_monotonic_ns: int | None = None,
    clock_skew_ms: int | None = 0,
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=Venue.BYBIT,
        symbol="BTC/USDT:USDT",
        bids=(BookLevel(Decimal("100"), Decimal("1")),),
        asks=(BookLevel(Decimal("101"), Decimal("1")),),
        exchange_timestamp_ms=1_700_000_000_000,
        received_at=datetime.now(UTC),
        received_monotonic_ns=received_monotonic_ns or time.monotonic_ns(),
        sequence_start=sequence_start,
        sequence_end=sequence_end,
        is_snapshot=is_snapshot,
        synchronised=True,
        clock_skew_ms=clock_skew_ms,
    )


def test_book_registry_detects_sequence_gap_and_staleness() -> None:
    registry = BookRegistry()
    assert registry.accept(make_book(), max_age_ms=1000, max_clock_skew_ms=1000).accepted
    gap = registry.accept(
        make_book(sequence_start=12, sequence_end=12, is_snapshot=False),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )
    assert gap.accepted is False
    assert gap.reason == ReasonCode.BOOK_SEQUENCE_GAP

    stale = registry.accept(
        make_book(
            sequence_start=11,
            sequence_end=11,
            is_snapshot=False,
            received_monotonic_ns=time.monotonic_ns() - 2_000_000_000,
        ),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )
    assert stale.reason == ReasonCode.BOOK_STALE


def test_book_registry_blocks_unknown_sequence_and_clock() -> None:
    registry = BookRegistry()
    unknown_sequence = registry.accept(
        make_book(sequence_start=None, sequence_end=None),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )
    assert unknown_sequence.reason == ReasonCode.BOOK_SEQUENCE_UNKNOWN
    unknown_clock = registry.accept(
        make_book(clock_skew_ms=None),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )
    assert unknown_clock.reason == ReasonCode.CLOCK_SKEW_UNKNOWN
