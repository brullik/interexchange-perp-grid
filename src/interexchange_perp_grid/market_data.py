from __future__ import annotations

import time
from dataclasses import dataclass

from interexchange_perp_grid.domain import OrderBookSnapshot, Venue
from interexchange_perp_grid.reason_codes import ReasonCode


@dataclass(frozen=True, slots=True)
class DataQualityAssessment:
    accepted: bool
    reason: ReasonCode
    age_ms: int


class BookRegistry:
    def __init__(self) -> None:
        self._last_sequence: dict[tuple[Venue, str], int] = {}
        self._books: dict[tuple[Venue, str], OrderBookSnapshot] = {}

    def accept(
        self,
        book: OrderBookSnapshot,
        *,
        max_age_ms: int,
        max_clock_skew_ms: int,
        now_monotonic_ns: int | None = None,
    ) -> DataQualityAssessment:
        now_ns = time.monotonic_ns() if now_monotonic_ns is None else now_monotonic_ns
        age_ns = now_ns - book.received_monotonic_ns
        age_ms = max(0, age_ns // 1_000_000)
        if age_ns < 0 or not book.synchronised:
            return DataQualityAssessment(False, ReasonCode.BOOK_UNSYNCHRONISED, age_ms)
        if not book.bids or not book.asks:
            return DataQualityAssessment(False, ReasonCode.BOOK_EMPTY, age_ms)
        if book.bids[0].price >= book.asks[0].price:
            return DataQualityAssessment(False, ReasonCode.BOOK_CROSSED, age_ms)
        if age_ms > max_age_ms:
            return DataQualityAssessment(False, ReasonCode.BOOK_STALE, age_ms)
        if book.clock_skew_ms is None:
            return DataQualityAssessment(False, ReasonCode.CLOCK_SKEW_UNKNOWN, age_ms)
        if abs(book.clock_skew_ms) > max_clock_skew_ms:
            return DataQualityAssessment(False, ReasonCode.CLOCK_SKEW_EXCEEDED, age_ms)
        if book.sequence_start is None or book.sequence_end is None:
            return DataQualityAssessment(False, ReasonCode.BOOK_SEQUENCE_UNKNOWN, age_ms)
        if book.sequence_end < book.sequence_start:
            return DataQualityAssessment(False, ReasonCode.BOOK_SEQUENCE_GAP, age_ms)
        if book.sequence_reset and not book.is_snapshot:
            return DataQualityAssessment(False, ReasonCode.BOOK_SEQUENCE_GAP, age_ms)
        key = (book.venue, book.symbol)
        previous = self._last_sequence.get(key)
        if previous is not None and not book.sequence_reset:
            if book.sequence_end <= previous:
                return DataQualityAssessment(False, ReasonCode.BOOK_SEQUENCE_GAP, age_ms)
            if (
                book.sequence_contiguous
                and not book.is_snapshot
                and book.sequence_start != previous + 1
            ):
                return DataQualityAssessment(False, ReasonCode.BOOK_SEQUENCE_GAP, age_ms)
        self._last_sequence[key] = book.sequence_end
        self._books[key] = book
        return DataQualityAssessment(True, ReasonCode.QUOTE_READY, age_ms)

    def get(self, venue: Venue, symbol: str) -> OrderBookSnapshot | None:
        return self._books.get((venue, symbol))

    def retain_keys(self, keys: frozenset[tuple[Venue, str]]) -> None:
        self._last_sequence = {
            key: sequence for key, sequence in self._last_sequence.items() if key in keys
        }
        self._books = {key: book for key, book in self._books.items() if key in keys}

    def discard_keys(self, keys: frozenset[tuple[Venue, str]]) -> None:
        for key in keys:
            self._last_sequence.pop(key, None)
            self._books.pop(key, None)
