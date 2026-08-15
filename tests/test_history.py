from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Event
from time import sleep

import pytest

from interexchange_perp_grid.domain import BookLevel, BookSide, OrderBookSnapshot, Venue
from interexchange_perp_grid.history import (
    ParquetMarketRecorder,
    _StagingSession,
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


@pytest.mark.asyncio
async def test_cancelled_recorder_never_publishes_late_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    book = OrderBookSnapshot(
        venue=Venue.OKX,
        symbol="BTC/USDT:USDT",
        bids=(BookLevel(Decimal("100.1"), Decimal("0.02")),),
        asks=(BookLevel(Decimal("100.2"), Decimal("0.03")),),
        exchange_timestamp_ms=1_786_708_800_000,
        received_at=datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        received_monotonic_ns=123,
        sequence_start=10,
        sequence_end=10,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=2,
    )
    recorder = ParquetMarketRecorder(tmp_path)
    stage_started = Event()
    allow_stage = Event()
    stage_finished = Event()
    stage_books = recorder._stage_books_sync

    def delayed_stage(
        books: tuple[OrderBookSnapshot, ...],
        session: _StagingSession,
    ) -> tuple[tuple[Path, Path], ...]:
        stage_started.set()
        allow_stage.wait()
        try:
            return stage_books(books, session)
        finally:
            stage_finished.set()

    monkeypatch.setattr(recorder, "_stage_books_sync", delayed_stage)
    append = asyncio.create_task(recorder.append_books((book,)))
    assert await asyncio.to_thread(stage_started.wait, 1)

    append.cancel()
    with pytest.raises(asyncio.CancelledError):
        await append
    allow_stage.set()
    assert await asyncio.to_thread(stage_finished.wait, 1)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert tuple(tmp_path.rglob("*.parquet")) == ()
    assert tuple(tmp_path.rglob("*.pending")) == ()


@pytest.mark.asyncio
async def test_writer_failure_removes_partial_pending_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    book = _book()

    def partial_write(_table: object, where: Path, **_kwargs: object) -> None:
        where.write_bytes(b"partial parquet")
        raise OSError("fixture write failure")

    monkeypatch.setattr("interexchange_perp_grid.history.pq.write_table", partial_write)
    recorder = ParquetMarketRecorder(tmp_path)
    with pytest.raises(OSError, match="fixture write failure"):
        await recorder.append_books((book,))

    assert tuple(tmp_path.rglob("*.parquet")) == ()
    assert tuple(tmp_path.rglob("*.pending")) == ()


def test_event_loop_shutdown_cleans_late_staging_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = ParquetMarketRecorder(tmp_path)
    stage_started = Event()
    allow_stage = Event()
    stage_books = recorder._stage_books_sync

    def delayed_stage(
        books: tuple[OrderBookSnapshot, ...],
        session: _StagingSession,
    ) -> tuple[tuple[Path, Path], ...]:
        stage_started.set()
        allow_stage.wait()
        return stage_books(books, session)

    monkeypatch.setattr(recorder, "_stage_books_sync", delayed_stage)

    async def cancel_append() -> None:
        append = asyncio.create_task(recorder.append_books((_book(),)))
        assert await asyncio.to_thread(stage_started.wait, 1)
        append.cancel()
        with pytest.raises(asyncio.CancelledError):
            await append
        asyncio.get_running_loop().call_later(0.05, allow_stage.set)

    asyncio.run(cancel_append())
    for _ in range(100):
        if not tuple(tmp_path.rglob("*.pending")):
            break
        sleep(0.01)

    assert tuple(tmp_path.rglob("*.parquet")) == ()
    assert tuple(tmp_path.rglob("*.pending")) == ()


def _book() -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=Venue.OKX,
        symbol="BTC/USDT:USDT",
        bids=(BookLevel(Decimal("100.1"), Decimal("0.02")),),
        asks=(BookLevel(Decimal("100.2"), Decimal("0.03")),),
        exchange_timestamp_ms=1_786_708_800_000,
        received_at=datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        received_monotonic_ns=123,
        sequence_start=10,
        sequence_end=10,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=2,
    )
