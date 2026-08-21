from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from uuid import uuid4

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from interexchange_perp_grid.domain import BookSide, OrderBookSnapshot, Venue


@dataclass(frozen=True, slots=True)
class RecordedBookLevel:
    event_id: str
    venue: Venue
    symbol: str
    side: BookSide
    level: int
    price: Decimal
    base_quantity: Decimal
    exchange_timestamp_ms: int | None
    received_at: datetime
    sequence_end: int | None


class _StagingSession:
    """Share pending-file ownership with a worker that may outlive its event loop."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._aborted = False
        self._staged: list[tuple[Path, Path]] = []

    def register(self, pending: Path, target: Path) -> bool:
        with self._lock:
            if self._aborted:
                return False
            self._staged.append((pending, target))
            return True

    def abort(self) -> None:
        with self._lock:
            self._aborted = True
        self.discard()

    def finish(self) -> tuple[tuple[Path, Path], ...]:
        with self._lock:
            aborted = self._aborted
            staged = tuple(self._staged)
        if aborted:
            self.discard()
            return ()
        return staged

    def discard(self) -> None:
        with self._lock:
            staged = tuple(self._staged)
        for pending, _ in staged:
            try:
                pending.unlink(missing_ok=True)
            except OSError:
                # A writer may still own the file on Windows. Its final session
                # check retries cleanup after the write completes.
                continue


def _event_id(book: OrderBookSnapshot) -> str:
    timestamp = book.exchange_timestamp_ms or int(book.received_at.timestamp() * 1_000)
    sequence = book.sequence_end if book.sequence_end is not None else "unknown"
    return f"{book.venue.value}:{book.symbol}:{timestamp}:{sequence}"


class ParquetMarketRecorder:
    def __init__(self, root: Path) -> None:
        self.root = root

    async def append_books(self, books: tuple[OrderBookSnapshot, ...]) -> tuple[Path, ...]:
        session = _StagingSession()
        worker = asyncio.create_task(
            asyncio.to_thread(self._stage_books_sync, books, session),
            name="stage-parquet-books",
        )
        try:
            staged = await asyncio.shield(worker)
        except asyncio.CancelledError:
            session.abort()
            worker.add_done_callback(self._discard_staged_result)
            raise
        targets: list[Path] = []
        try:
            for pending, target in staged:
                pending.replace(target)
                targets.append(target)
        finally:
            for pending, _ in staged:
                pending.unlink(missing_ok=True)
        return tuple(targets)

    def _stage_books_sync(
        self,
        books: tuple[OrderBookSnapshot, ...],
        session: _StagingSession,
    ) -> tuple[tuple[Path, Path], ...]:
        try:
            for book in books:
                event_id = _event_id(book)
                rows: list[dict[str, object]] = []
                for side, levels in ((BookSide.BID, book.bids), (BookSide.ASK, book.asks)):
                    for level_index, level in enumerate(levels):
                        rows.append(
                            {
                                "event_id": event_id,
                                "venue": book.venue.value,
                                "symbol": book.symbol,
                                "side": side.value,
                                "level": level_index,
                                "price": str(level.price),
                                "base_quantity": str(level.base_quantity),
                                "exchange_timestamp_ms": book.exchange_timestamp_ms,
                                "received_at": book.received_at.isoformat(),
                                "received_monotonic_ns": book.received_monotonic_ns,
                                "sequence_start": book.sequence_start,
                                "sequence_end": book.sequence_end,
                                "is_snapshot": book.is_snapshot,
                                "synchronised": book.synchronised,
                                "clock_skew_ms": book.clock_skew_ms,
                            }
                        )
                if not rows:
                    continue
                partition = (
                    self.root
                    / f"date={book.received_at.date().isoformat()}"
                    / f"venue={book.venue.value}"
                )
                partition.mkdir(parents=True, exist_ok=True)
                target = partition / f"part-{uuid4().hex}.parquet"
                pending = target.with_suffix(".parquet.pending")
                if not session.register(pending, target):
                    return session.finish()
                pq.write_table(pa.Table.from_pylist(rows), pending, compression="zstd")
                staged = session.finish()
                if not staged:
                    return ()
            return session.finish()
        except Exception:
            session.discard()
            raise

    @staticmethod
    def _discard_staged_result(
        worker: asyncio.Task[tuple[tuple[Path, Path], ...]],
    ) -> None:
        try:
            staged = worker.result()
        except (asyncio.CancelledError, Exception):
            return
        for pending, _ in staged:
            pending.unlink(missing_ok=True)


def query_recorded_level_count(root: Path) -> int:
    files = tuple(root.rglob("*.parquet")) if root.is_dir() else ()
    if not files:
        return 0
    parquet_glob = str(root / "**" / "*.parquet").replace("\\", "/")
    with duckdb.connect(":memory:") as database:
        row = database.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning = true)",
            [parquet_glob],
        ).fetchone()
    return int(row[0]) if row is not None else 0


def replay_recorded_levels(root: Path) -> tuple[RecordedBookLevel, ...]:
    parquet_glob = str(root / "**" / "*.parquet").replace("\\", "/")
    with duckdb.connect(":memory:") as database:
        rows = database.execute(
            """
            SELECT event_id, venue, symbol, side, level, price, base_quantity,
                   exchange_timestamp_ms, received_at, sequence_end
            FROM read_parquet(?, hive_partitioning = true)
            ORDER BY received_at, event_id, side, level
            """,
            [parquet_glob],
        ).fetchall()
    return tuple(
        RecordedBookLevel(
            event_id=str(row[0]),
            venue=Venue(str(row[1])),
            symbol=str(row[2]),
            side=BookSide(str(row[3])),
            level=int(row[4]),
            price=Decimal(str(row[5])),
            base_quantity=Decimal(str(row[6])),
            exchange_timestamp_ms=int(row[7]) if row[7] is not None else None,
            received_at=datetime.fromisoformat(str(row[8])),
            sequence_end=int(row[9]) if row[9] is not None else None,
        )
        for row in rows
    )
