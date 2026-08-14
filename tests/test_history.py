from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import BookLevel, BookSide, OrderBookSnapshot, Venue
from interexchange_perp_grid.history import (
    ParquetMarketRecorder,
    query_recorded_level_count,
    replay_recorded_levels,
)


@pytest.mark.asyncio
async def test_parquet_duckdb_round_trip_is_exact_and_deterministic(tmp_path: Path) -> None:
    received_at = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    book = OrderBookSnapshot(
        venue=Venue.OKX,
        symbol="BTC/USDT:USDT",
        bids=(BookLevel(Decimal("100.1"), Decimal("0.02")),),
        asks=(BookLevel(Decimal("100.2"), Decimal("0.03")),),
        exchange_timestamp_ms=1_786_708_800_000,
        received_at=received_at,
        received_monotonic_ns=123,
        sequence_start=10,
        sequence_end=10,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=2,
    )
    recorder = ParquetMarketRecorder(tmp_path)
    written = await recorder.append_books((book,))
    assert len(written) == 1
    assert query_recorded_level_count(tmp_path) == 2
    replayed = replay_recorded_levels(tmp_path)
    assert tuple(item.side for item in replayed) == (BookSide.ASK, BookSide.BID)
    assert {item.price for item in replayed} == {Decimal("100.1"), Decimal("100.2")}
    assert all(item.received_at == received_at for item in replayed)
