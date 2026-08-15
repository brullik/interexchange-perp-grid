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
    sequence_reset: bool = False,
    sequence_contiguous: bool = True,
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
        sequence_reset=sequence_reset,
        sequence_contiguous=sequence_contiguous,
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


def test_book_registry_allows_explicit_snapshot_sequence_reset_only() -> None:
    registry = BookRegistry()
    assert registry.accept(make_book(), max_age_ms=1000, max_clock_skew_ms=1000).accepted

    reset = registry.accept(
        make_book(sequence_start=1, sequence_end=1, sequence_reset=True),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )
    invalid_delta_reset = registry.accept(
        make_book(
            sequence_start=2,
            sequence_end=2,
            is_snapshot=False,
            sequence_reset=True,
        ),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )

    assert reset.accepted is True
    assert invalid_delta_reset.reason == ReasonCode.BOOK_SEQUENCE_GAP


def test_book_registry_accepts_monotonic_non_contiguous_native_sequence() -> None:
    registry = BookRegistry()
    assert registry.accept(make_book(), max_age_ms=1000, max_clock_skew_ms=1000).accepted

    jumped = registry.accept(
        make_book(
            sequence_start=15,
            sequence_end=15,
            is_snapshot=False,
            sequence_contiguous=False,
        ),
        max_age_ms=1000,
        max_clock_skew_ms=1000,
    )

    assert jumped.accepted is True
