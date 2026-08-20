from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from interexchange_perp_grid import public_engine as public_engine_module
from interexchange_perp_grid.adapters.base import ExchangeAdapter
from interexchange_perp_grid.candidate_l2 import (
    CandidateL2BookPlan,
    CandidateL2BookState,
    CandidateL2Plan,
    L2WorkPriority,
    build_candidate_l2_plan,
    evaluate_candidate_l2_routes,
)
from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.domain import (
    BboQuote,
    BookLevel,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.history import ParquetMarketRecorder
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.market_universe import UniverseRoute
from interexchange_perp_grid.public_engine import PublicMarketEngine
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.shadow import OverloadController, WorkClass

CONFIG = Path("config/defaults.yaml")


def instruments(venue: Venue, count: int = 40) -> tuple[Instrument, ...]:
    return tuple(
        Instrument(
            venue,
            f"A{index:03d}/USDT:USDT",
            f"{venue.value}-A{index:03d}",
            f"A{index:03d}",
            "USDT",
            "USDT",
            Decimal(1),
            Decimal("0.001"),
            Decimal("0.1"),
            Decimal("0.001"),
            Decimal(5),
            Decimal("0.0005"),
            "fixture",
            listed_at=datetime(2025, 1, 1, tzinfo=UTC),
        )
        for index in range(count)
    )


class CandidateAdapter(ExchangeAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        self.venue = venue
        self.clock = clock
        self.instruments = instruments(venue)
        self.sequence: dict[str, int] = {}
        self.active_books: dict[str, int] = {}
        self.peak_books: dict[str, int] = {}
        self.book_calls: dict[str, int] = {}
        self.bbo_unwatch_calls: list[tuple[str, ...]] = []
        self.unwatch_calls: dict[str, int] = {}
        self.fail_l2 = False
        self.crossed_l2 = False
        self.quality_mode = "ready"
        self.fail_probe = False
        self.closed = False
        self.close_calls = 0

    async def probe_public_capabilities(self) -> CapabilityReport:
        if self.fail_probe:
            raise ConnectionError("fixture probe outage")
        return CapabilityReport(
            self.venue,
            True,
            True,
            True,
            True,
            True,
            0,
            datetime.now(UTC),
            (),
        )

    async def discover_instruments(self) -> tuple[Instrument, ...]:
        return self.instruments

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        offset = Decimal(list(Venue).index(self.venue))
        return tuple(
            BboQuote(
                self.venue,
                instrument.symbol,
                Decimal(100) + offset,
                Decimal(1),
                Decimal("100.5") + offset,
                Decimal(1),
                1_700_000_000_000,
                datetime.now(UTC),
                self.clock[0],
                0,
            )
            for instrument in self.instruments
            if instrument.symbol in symbols
        )

    async def unwatch_bbo(self, symbols: tuple[str, ...]) -> None:
        self.bbo_unwatch_calls.append(symbols)

    async def watch_order_book(
        self,
        instrument: Instrument,
        limit: int = 50,
    ) -> OrderBookSnapshot:
        del limit
        symbol = instrument.symbol
        self.active_books[symbol] = self.active_books.get(symbol, 0) + 1
        self.peak_books[symbol] = max(
            self.peak_books.get(symbol, 0),
            self.active_books[symbol],
        )
        self.book_calls[symbol] = self.book_calls.get(symbol, 0) + 1
        try:
            await asyncio.sleep(0)
            if self.fail_l2:
                raise ConnectionError("fixture candidate L2 outage")
            sequence = self.sequence.get(symbol, 0) + 1
            self.sequence[symbol] = sequence
            offset = Decimal(list(Venue).index(self.venue))
            bid = Decimal(100) + offset
            ask = bid if self.crossed_l2 else bid + Decimal("0.5")
            sequence_start: int | None = sequence
            sequence_end: int | None = sequence
            received_ns = self.clock[0] - 20_000_000
            clock_skew_ms: int | None = 0
            if self.quality_mode == "unknown_sequence":
                sequence_start = sequence_end = None
            elif self.quality_mode == "sequence_gap":
                sequence_start = sequence + 1
                sequence_end = sequence
            elif self.quality_mode == "stale":
                received_ns = self.clock[0] - 2_000_000_000
            elif self.quality_mode == "clock_unknown":
                clock_skew_ms = None
            return OrderBookSnapshot(
                self.venue,
                symbol,
                (BookLevel(bid, Decimal(1)),),
                (BookLevel(ask, Decimal(1)),),
                1_700_000_000_000,
                datetime.now(UTC),
                received_ns,
                sequence_start,
                sequence_end,
                True,
                True,
                clock_skew_ms,
            )
        finally:
            self.active_books[symbol] -= 1

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        del instrument
        raise AssertionError("candidate L2 must not fetch funding")

    async def unwatch_order_book(self, instrument: Instrument, limit: int = 50) -> None:
        del limit
        self.unwatch_calls[instrument.symbol] = self.unwatch_calls.get(instrument.symbol, 0) + 1

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class CancellationResistantCandidateAdapter(CandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.release = asyncio.Event()
        self.cancelled = 0

    async def watch_order_book(
        self,
        instrument: Instrument,
        limit: int = 50,
    ) -> OrderBookSnapshot:
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            await self.release.wait()
        return await super().watch_order_book(instrument, limit)

    async def close(self) -> None:
        self.release.set()
        await super().close()


class SilentBroadCandidateAdapter(CandidateAdapter):
    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        del symbols
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class OneThenSilentCandidateAdapter(CandidateAdapter):
    async def watch_order_book(
        self,
        instrument: Instrument,
        limit: int = 50,
    ) -> OrderBookSnapshot:
        if self.book_calls.get(instrument.symbol, 0):
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        return await super().watch_order_book(instrument, limit)


class FundingCandidateAdapter(CandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.funding_calls = 0

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_calls += 1
        observed_ms = int(datetime.now(UTC).timestamp() * 1000)
        return FundingSnapshot(
            instrument.venue,
            instrument.symbol,
            Decimal("0.0001"),
            observed_ms + 8 * 60 * 60 * 1000,
            "8h",
            Decimal(101),
            Decimal("100.9"),
            observed_ms,
        )


class GenerationChangingFundingAdapter(FundingCandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.on_funding: Any | None = None

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        result = await super().fetch_funding(instrument)
        if self.on_funding is not None:
            callback = self.on_funding
            self.on_funding = None
            callback()
        return result


class OneThenSilentFundingAdapter(OneThenSilentCandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.funding_calls = 0

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_calls += 1
        observed_ms = int(datetime.now(UTC).timestamp() * 1000)
        return FundingSnapshot(
            instrument.venue,
            instrument.symbol,
            Decimal("0.0001"),
            observed_ms + 8 * 60 * 60 * 1000,
            "8h",
            Decimal(101),
            Decimal("100.9"),
            observed_ms,
        )


class FailingFundingAdapter(FundingCandidateAdapter):
    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_calls += 1
        raise RuntimeError(f"funding unavailable for {instrument.symbol}")


class HeldFundingAdapter(FundingCandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.funding_started = asyncio.Event()
        self.funding_release = asyncio.Event()

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_started.set()
        await self.funding_release.wait()
        return await super().fetch_funding(instrument)


class WrongIdentityFundingAdapter(FundingCandidateAdapter):
    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        result = await super().fetch_funding(instrument)
        return FundingSnapshot(
            Venue.BINANCE_USDM if instrument.venue != Venue.BINANCE_USDM else Venue.OKX,
            f"WRONG-{instrument.symbol}",
            result.rate,
            result.next_funding_timestamp_ms,
            result.interval,
            result.mark_price,
            result.index_price,
            result.exchange_timestamp_ms,
        )


class CancellationResistantFundingAdapter(FundingCandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.funding_started = asyncio.Event()
        self.funding_release = asyncio.Event()

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_calls += 1
        self.funding_started.set()
        while not self.funding_release.is_set():
            try:
                await self.funding_release.wait()
            except asyncio.CancelledError:
                continue
        observed_ms = int(datetime.now(UTC).timestamp() * 1000)
        return FundingSnapshot(
            instrument.venue,
            instrument.symbol,
            Decimal("0.0001"),
            observed_ms + 8 * 60 * 60 * 1000,
            "8h",
            Decimal(101),
            Decimal("100.9"),
            observed_ms,
        )


class LateSuccessfulFundingAdapter(FundingCandidateAdapter):
    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_calls += 1
        deadline = asyncio.get_running_loop().time() + 1.1
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                await asyncio.sleep(remaining)
            except asyncio.CancelledError:
                continue
        observed_ms = int(datetime.now(UTC).timestamp() * 1000)
        return FundingSnapshot(
            instrument.venue,
            instrument.symbol,
            Decimal("0.0001"),
            observed_ms + 8 * 60 * 60 * 1000,
            "8h",
            Decimal(101),
            Decimal("100.9"),
            observed_ms,
        )


class StaleFundingAdapter(FundingCandidateAdapter):
    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        result = await super().fetch_funding(instrument)
        assert result.exchange_timestamp_ms is not None
        return FundingSnapshot(
            result.venue,
            result.symbol,
            result.rate,
            result.next_funding_timestamp_ms,
            result.interval,
            result.mark_price,
            result.index_price,
            result.exchange_timestamp_ms - 365 * 24 * 60 * 60 * 1000,
        )


class InvalidMarkFundingAdapter(FundingCandidateAdapter):
    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        result = await super().fetch_funding(instrument)
        return FundingSnapshot(
            result.venue,
            result.symbol,
            result.rate,
            result.next_funding_timestamp_ms,
            result.interval,
            Decimal(-1),
            Decimal("NaN"),
            result.exchange_timestamp_ms,
        )


class DelayedUnwatchCandidateAdapter(CandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.unwatch_started = asyncio.Event()
        self.unwatch_finished = asyncio.Event()

    async def unwatch_order_book(self, instrument: Instrument, limit: int = 50) -> None:
        self.unwatch_started.set()
        await asyncio.sleep(0.2)
        await super().unwatch_order_book(instrument, limit)
        self.unwatch_finished.set()


class FailingUnwatchCandidateAdapter(CandidateAdapter):
    async def unwatch_order_book(self, instrument: Instrument, limit: int = 50) -> None:
        del instrument, limit
        raise OSError("fixture unsubscribe failed")


class BlockingProbeCandidateAdapter(CandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.probe_started = asyncio.Event()
        self.allow_probe = asyncio.Event()

    async def probe_public_capabilities(self) -> CapabilityReport:
        self.probe_started.set()
        await self.allow_probe.wait()
        return await super().probe_public_capabilities()


class HeldFundingAndUnwatchCandidateAdapter(FundingCandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.funding_started = asyncio.Event()
        self.allow_funding = asyncio.Event()
        self.unwatch_started = asyncio.Event()
        self.allow_unwatch = asyncio.Event()

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        if instrument.base == "A039":
            self.funding_started.set()
            await self.allow_funding.wait()
        return await super().fetch_funding(instrument)

    async def unwatch_order_book(self, instrument: Instrument, limit: int = 50) -> None:
        self.unwatch_started.set()
        await self.allow_unwatch.wait()
        await super().unwatch_order_book(instrument, limit)

    async def close(self) -> None:
        self.allow_unwatch.set()
        await super().close()


class BlockingProbeFundingCandidateAdapter(FundingCandidateAdapter):
    def __init__(self, venue: Venue, clock: list[int]) -> None:
        super().__init__(venue, clock)
        self.probe_started = asyncio.Event()
        self.allow_probe = asyncio.Event()

    async def probe_public_capabilities(self) -> CapabilityReport:
        self.probe_started.set()
        await self.allow_probe.wait()
        return await super().probe_public_capabilities()


def settings(
    tmp_path: Path,
    *,
    maximum_candidates: int = 30,
    debounce_ms: int = 20,
) -> Settings:
    loaded = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    return loaded.model_copy(
        update={
            "universe": loaded.universe.model_copy(
                update={
                    "max_dynamic_l2_candidates": maximum_candidates,
                    "decision_debounce_ms": debounce_ms,
                }
            )
        }
    )


@pytest.mark.asyncio
async def test_candidate_l2_selects_top30_plus_active_and_deduplicates_books(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    result = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
    )

    assert result.stats.candidate_routes == 30
    assert result.stats.active_routes == 1
    assert result.stats.selected_routes == 31
    assert result.stats.known_books <= 62
    assert result.stats.active_watchers == result.stats.known_books
    assert result.stats.decision_updates >= 1
    assert result.stats.decision_latency_p95_ms is not None
    assert result.stats.decision_latency_p95_ms <= Decimal(250)
    assert len(engine._candidate_l2_latency_samples) <= 2048
    assert {route.stable_key for route in result.routes if route.priority == 2} == active
    assert all(route.execution_authorized is False for route in result.routes)
    assert result.execution_authorized is False
    assert Decimal(20) <= result.decision_latency_ms <= Decimal(250)
    assert all(max(adapter.peak_books.values(), default=0) == 1 for adapter in adapters.values())

    await engine.close()
    assert not tuple(
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith("candidate-l2-")
        and not task.done()
    )


@pytest.mark.asyncio
async def test_candidate_l2_feeds_three_route_size_calibration_buckets_from_cached_funding(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)

    first = await engine.scan_route_calibration_observations(2)
    funding_calls = sum(adapter.funding_calls for adapter in adapters.values())
    second = await engine.scan_route_calibration_observations(2)

    assert len(first) == 3
    assert {item.size_bucket_multiplier for item in first} == {
        Decimal("1"),
        Decimal("2"),
        Decimal("5"),
    }
    assert {item.base_quantity for item in first} == {
        Decimal("0.05"),
        Decimal("0.1"),
        Decimal("0.25"),
    }
    assert all(item.stressed_cost_floor_bps is not None for item in first)
    assert all(item.normalized_tick_bps is not None for item in first)
    assert all(item.funding_rate_delta is not None for item in first)
    assert all(item.reason == ReasonCode.QUOTE_READY for item in first)
    assert tuple(
        (item.route, item.size_bucket_multiplier, item.base_quantity, item.reason) for item in first
    ) == tuple(
        (item.route, item.size_bucket_multiplier, item.base_quantity, item.reason)
        for item in second
    )
    assert funding_calls == 2
    assert sum(adapter.funding_calls for adapter in adapters.values()) == funding_calls

    await engine.close()


@pytest.mark.asyncio
async def test_route_calibration_never_mixes_funding_across_adapter_generation(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: GenerationChangingFundingAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)
    for venue, adapter in adapters.items():
        adapter.on_funding = lambda venue=venue: engine._venue_refresh_generations.__setitem__(
            venue,
            engine._venue_refresh_generations.get(venue, 0) + 1,
        )

    raced = await engine.scan_route_calibration_observations(2)
    recovered = await engine.scan_route_calibration_observations(2)

    assert raced == ()
    assert len(recovered) == 3
    assert all(item.reason == ReasonCode.QUOTE_READY for item in recovered)
    await engine.close()


@pytest.mark.asyncio
async def test_route_calibration_revalidates_silent_l2_freshness_after_funding(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: OneThenSilentFundingAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)
    assert all(
        item.reason == ReasonCode.QUOTE_READY
        for item in await engine.scan_route_calibration_observations(2)
    )

    clock[0] += 2_000_000_000
    stale = await engine.scan_route_calibration_observations(2)

    assert len(stale) == 3
    assert {item.reason for item in stale} == {ReasonCode.BOOK_STALE}
    assert all(item.base_quantity is None for item in stale)
    await engine.close()


@pytest.mark.asyncio
async def test_route_plan_removal_emits_invalid_marker_before_reentry(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)
    assert len(await engine.scan_route_calibration_observations(2)) == 3

    await engine.scan_candidate_l2(2, candidates_admitted=False)
    removed = await engine.scan_route_calibration_observations(2)
    await engine.scan_candidate_l2(2)
    restored = await engine.scan_route_calibration_observations(2)

    assert len(removed) == 3
    assert {item.reason for item in removed} == {ReasonCode.BOOK_EMPTY}
    assert len(restored) == 3
    assert {item.reason for item in restored} == {ReasonCode.QUOTE_READY}
    await engine.close()


@pytest.mark.asyncio
async def test_route_calibration_quarantine_during_funding_is_persistently_invalid(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, CandidateAdapter] = {
        venue: FundingCandidateAdapter(venue, clock) for venue in Venue
    }
    held = HeldFundingAdapter(Venue.OKX, clock)
    adapters[Venue.OKX] = held
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)
    sampling = asyncio.create_task(engine.scan_route_calibration_observations(2))
    await asyncio.wait_for(held.funding_started.wait(), timeout=1)
    engine._quarantine(Venue.OKX, "test funding race")
    held.funding_release.set()

    observations = await sampling

    assert len(observations) == 3
    assert {item.reason for item in observations} == {ReasonCode.VENUE_OUTAGE}
    after_quarantine = await engine.scan_route_calibration_observations(2)
    assert {item.reason for item in after_quarantine} == {ReasonCode.VENUE_OUTAGE}
    await engine.close()


@pytest.mark.asyncio
async def test_route_funding_scheduler_is_fair_when_one_venue_fails(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, FundingCandidateAdapter] = {
        Venue.BINANCE_USDM: FailingFundingAdapter(Venue.BINANCE_USDM, clock),
        Venue.BYBIT: FundingCandidateAdapter(Venue.BYBIT, clock),
        Venue.OKX: FundingCandidateAdapter(Venue.OKX, clock),
    }
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=30),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)

    await engine.scan_route_calibration_observations(2)

    planned_venues = {book.instrument.venue for book in engine._candidate_l2_plan.books}
    assert adapters[Venue.BINANCE_USDM].funding_calls > 0
    assert all(
        adapters[venue].funding_calls > 0 for venue in planned_venues - {Venue.BINANCE_USDM}
    ), {venue: adapter.funding_calls for venue, adapter in adapters.items()}
    await engine.close()


@pytest.mark.asyncio
async def test_wrong_identity_funding_is_never_qualified(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: WrongIdentityFundingAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)

    observations = await engine.scan_route_calibration_observations(2)

    assert len(observations) == 3
    assert {item.reason for item in observations} == {ReasonCode.FUNDING_UNKNOWN}
    assert engine._route_calibration_funding_retry_after_ns
    retired_key = (Venue.OKX, "RETIRED-USDT")
    engine._route_calibration_funding_retry_after_ns[retired_key] = 10**30
    await engine.scan_route_calibration_observations(2)
    assert retired_key not in engine._route_calibration_funding_retry_after_ns
    await engine.close()


@pytest.mark.asyncio
async def test_year_stale_funding_is_never_qualified(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: StaleFundingAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)

    observations = await engine.scan_route_calibration_observations(2)

    assert len(observations) == 3
    assert {item.reason for item in observations} == {ReasonCode.FUNDING_UNKNOWN}
    await engine.close()


@pytest.mark.asyncio
async def test_nonfinite_or_nonpositive_mark_index_is_never_qualified(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: InvalidMarkFundingAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)

    observations = await engine.scan_route_calibration_observations(2)

    assert len(observations) == 3
    assert {item.reason for item in observations} == {ReasonCode.FUNDING_UNKNOWN}
    await engine.close()


@pytest.mark.asyncio
async def test_route_funding_hard_deadline_does_not_starve_other_venues(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    held = CancellationResistantFundingAdapter(Venue.BINANCE_USDM, clock)
    adapters: dict[Venue, FundingCandidateAdapter] = {
        Venue.BINANCE_USDM: held,
        Venue.BYBIT: FundingCandidateAdapter(Venue.BYBIT, clock),
        Venue.OKX: FundingCandidateAdapter(Venue.OKX, clock),
    }
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=30),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.scan_candidate_l2(2)
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        await engine.scan_route_calibration_observations(2)
        planned_venues = {book.instrument.venue for book in engine._candidate_l2_plan.books}
        assert loop.time() - started < 0.25
        assert all(
            adapters[venue].funding_calls > 0 for venue in planned_venues - {Venue.BINANCE_USDM}
        )
        healthy_calls = {
            venue: adapters[venue].funding_calls for venue in planned_venues - {Venue.BINANCE_USDM}
        }
        clock[0] += 1_100_000_000
        await engine.scan_route_calibration_observations(2)
        assert all(
            adapters[venue].funding_calls >= previous for venue, previous in healthy_calls.items()
        )
        assert sum(adapters[venue].funding_calls for venue in healthy_calls) > sum(
            healthy_calls.values()
        )
        assert Venue.BINANCE_USDM in engine._quarantined
        assert engine._retiring_route_calibration_funding_transports
        instruments = {book.key: book.instrument for book in engine._candidate_l2_plan.books}
        venue_capacities = engine._route_calibration_funding_capacity_by_venue(instruments)
        assert (
            sum(key[0] == Venue.BINANCE_USDM for key in engine._route_calibration_funding_tasks)
            <= venue_capacities[Venue.BINANCE_USDM]
        )
        held.funding_release.set()
        await asyncio.sleep(0.01)
        engine._cleanup_retired_route_calibration_funding_tasks()
        assert not engine._retiring_route_calibration_funding_tasks
        assert not engine._retiring_route_calibration_funding_transports
        prior_calls = held.funding_calls
        clock[0] += 31_000_000_000
        await engine.refresh_universe(2, reconnected=(Venue.BINANCE_USDM,))
        await engine.scan_candidate_l2(2)
        await engine.scan_route_calibration_observations(2)
        assert held.funding_calls > prior_calls
    finally:
        held.funding_release.set()
        await asyncio.sleep(0.01)
        await engine.close()


@pytest.mark.asyncio
async def test_route_funding_watchdog_rejects_late_success_without_followup_scan(
    tmp_path: Path,
) -> None:
    clock = [time.monotonic_ns()]
    adapters: dict[Venue, FundingCandidateAdapter] = {
        Venue.BINANCE_USDM: LateSuccessfulFundingAdapter(Venue.BINANCE_USDM, clock),
        Venue.BYBIT: FundingCandidateAdapter(Venue.BYBIT, clock),
        Venue.OKX: FundingCandidateAdapter(Venue.OKX, clock),
    }
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=time.monotonic_ns,
    )
    await engine.scan_candidate_l2(2)
    await engine.scan_route_calibration_observations(2)

    await asyncio.sleep(1.2)

    assert Venue.BINANCE_USDM in engine._quarantined
    assert not any(key[0] == Venue.BINANCE_USDM for key in engine._route_calibration_funding)
    await asyncio.sleep(0.1)
    engine._cleanup_retired_route_calibration_funding_tasks()
    assert not engine._retiring_route_calibration_funding_transports
    await engine.close()


@pytest.mark.asyncio
async def test_removed_funding_transport_is_reaped_without_followup_scan(
    tmp_path: Path,
) -> None:
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        recorder=ParquetMarketRecorder(tmp_path),
    )
    release = asyncio.Event()
    key = (Venue.OKX, "BTC/USDT:USDT")

    async def resistant_transport() -> FundingSnapshot:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue
        observed_ms = int(datetime.now(UTC).timestamp() * 1000)
        return FundingSnapshot(
            key[0],
            key[1],
            Decimal("0.0001"),
            observed_ms + 8 * 60 * 60 * 1000,
            "8h",
            Decimal(101),
            Decimal("100.9"),
            observed_ms,
        )

    transport = asyncio.create_task(resistant_transport())
    await asyncio.sleep(0)
    engine._retire_route_calibration_funding_transport(key, transport)

    await asyncio.sleep(1.1)

    assert Venue.OKX in engine._quarantined
    assert transport.done() is False
    release.set()
    await asyncio.sleep(0.01)
    engine._cleanup_retired_route_calibration_funding_tasks()
    assert not engine._retiring_route_calibration_funding_transports
    assert not engine._retiring_route_calibration_funding_watchdogs
    await engine.close()


def test_funding_scheduler_capacity_covers_maximum_supported_plan_before_stale(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    configured = settings(tmp_path, maximum_candidates=30)
    engine = PublicMarketEngine(
        configured,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    maximum_books = 2 * (
        configured.universe.max_dynamic_l2_candidates + configured.risk.max_active_routes
    )
    distribution = {
        Venue.BINANCE_USDM: 40,
        Venue.BYBIT: 20,
        Venue.OKX: 20,
    }
    instruments = {
        (venue, instrument.symbol): instrument
        for venue, count in distribution.items()
        for instrument in adapters[venue].instruments[:count]
    }
    capacities = engine._route_calibration_funding_capacity_by_venue(instruments)
    cycles_before_refresh_lead = (
        configured.strategy.calibration_funding_refresh_seconds
        * 3
        // 4
        // configured.shadow.scan_interval_seconds
    )

    assert maximum_books == 80
    assert len(instruments) == maximum_books
    assert all(
        capacities[venue] * cycles_before_refresh_lead >= count
        for venue, count in distribution.items()
    )
    assert sum(capacities.values()) * cycles_before_refresh_lead >= maximum_books
    assert sum(capacities.values()) < maximum_books


@pytest.mark.asyncio
async def test_decision_latency_p95_includes_worker_compute_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})
    await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    engine._candidate_l2_latency_samples.clear()
    original = evaluate_candidate_l2_routes

    def slow_evaluate(*args: Any, **kwargs: Any) -> Any:
        time.sleep(0.03)
        return original(*args, **kwargs)

    monkeypatch.setattr(public_engine_module, "evaluate_candidate_l2_routes", slow_evaluate)
    engine._mark_candidate_l2_state_changed()
    target = engine._candidate_l2_state_version
    await engine._wait_for_candidate_l2_decision(target, 2)

    latency_p95_ms = engine._candidate_l2_latency_p95_ms()
    assert latency_p95_ms is not None
    assert latency_p95_ms >= Decimal(25)
    await engine.close()


@pytest.mark.asyncio
async def test_overload_sheds_candidate_l2_but_keeps_active_route_books(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    result = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    await engine.close()

    assert result.stats.active_routes == 1
    assert result.stats.candidate_routes == 0
    assert result.stats.known_books == 2
    assert result.routes[0].priority == L2WorkPriority.ACTIVE_ROUTE
    assert result.routes[0].reason == ReasonCode.QUOTE_READY


@pytest.mark.asyncio
async def test_broad_overload_shed_unsubscribes_without_disrupting_active_l2(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    await engine.scan_broad_bbo(2)
    await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    await engine.set_broad_bbo_admitted(False)
    after_shed = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert not engine._bbo_watchers
    assert all(len(adapter.bbo_unwatch_calls) == 1 for adapter in adapters.values())
    assert after_shed.routes[0].reason == ReasonCode.QUOTE_READY
    assert after_shed.stats.active_watchers == 2
    await engine.close()


@pytest.mark.asyncio
async def test_shed_candidate_demand_does_not_oscillate_when_broad_feed_stales(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    await engine.scan_candidate_l2(2, candidates_admitted=True)
    admitted_demand = engine.public_workload().candidate_l2_demand
    assert admitted_demand > 0
    await engine.set_broad_bbo_admitted(False)
    clock[0] += 2_000_000_000
    await engine.scan_candidate_l2(
        2,
        candidates_admitted=False,
        prefilter=(),
    )

    assert engine.public_workload().candidate_l2_demand == admitted_demand
    await engine.close()


@pytest.mark.asyncio
async def test_rapid_broad_disable_before_transport_start_needs_no_unsubscribe(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise()

    await engine._sync_bbo_watchers()
    await engine.set_broad_bbo_admitted(False)

    assert not engine._bbo_watchers
    assert not engine._quarantined
    assert all(not adapter.bbo_unwatch_calls for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_active_l2_does_not_wait_for_shed_broad_bbo(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: SilentBroadCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    result = await engine.scan_candidate_l2(
        1,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert result.stats.active_routes == 1
    assert result.stats.candidate_routes == 0
    assert result.routes[0].reason == ReasonCode.QUOTE_READY
    await engine.close()


@pytest.mark.asyncio
async def test_active_selected_scan_does_not_wait_for_shed_broad_bbo(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active_key = ("A039", Venue.BYBIT.value, Venue.OKX.value)
    active = frozenset({active_key})
    await engine.scan_candidate_l2(
        1,
        active_route_keys=active,
        candidates_admitted=False,
    )
    await engine.set_broad_bbo_admitted(False)

    result = await asyncio.wait_for(
        engine.scan_once(
            "A039",
            Decimal("0.1"),
            1,
            active_route_keys=active,
            entry_work_admitted=False,
        ),
        timeout=0.5,
    )

    assert not engine._bbo_watchers
    assert {
        (quote.key.base, quote.long_venue.value, quote.short_venue.value) for quote in result.quotes
    } == {active_key}
    await engine.close()


@pytest.mark.asyncio
async def test_candidate_l2_reuses_ranked_prefilter_without_second_broad_wait(
    tmp_path: Path,
) -> None:
    from interexchange_perp_grid.bbo_prefilter import BboPrefilterObservation

    clock = [1_000_000_000]
    adapters = {venue: SilentBroadCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    prefilter = (
        BboPrefilterObservation(
            "A000",
            Venue.BYBIT,
            Venue.OKX,
            "A000/USDT:USDT",
            "A000/USDT:USDT",
            Decimal(1),
            Decimal(1),
            Decimal(0),
            clock[0],
            ReasonCode.QUOTE_READY,
        ),
    )

    result = await engine.scan_candidate_l2(1, prefilter=prefilter)

    assert result.stats.candidate_routes == 1
    assert result.routes[0].reason == ReasonCode.QUOTE_READY
    await engine.close()


@pytest.mark.asyncio
async def test_concurrent_candidate_scans_return_their_own_active_plan(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, debounce_ms=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    first_key = ("A000", Venue.BYBIT.value, Venue.OKX.value)
    second_key = ("A001", Venue.BYBIT.value, Venue.OKX.value)

    first, second = await asyncio.gather(
        engine.scan_candidate_l2(
            2,
            active_route_keys=frozenset({first_key}),
            candidates_admitted=False,
        ),
        engine.scan_candidate_l2(
            2,
            active_route_keys=frozenset({second_key}),
            candidates_admitted=False,
        ),
    )

    assert {route.stable_key for route in first.routes} == {first_key}
    assert {route.stable_key for route in second.routes} == {second_key}
    await engine.close()


@pytest.mark.asyncio
async def test_rapid_candidate_removal_before_transport_start_needs_no_unsubscribe(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise()
    universe = engine._universe.snapshot
    assert universe is not None
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})
    plan = build_candidate_l2_plan(
        universe.routes,
        (),
        active_route_keys=active,
        maximum_candidates=30,
        candidates_admitted=False,
    )

    await engine._apply_candidate_l2_plan(plan)
    await engine._apply_candidate_l2_plan(CandidateL2Plan((), (), ()))

    assert not engine._candidate_l2_watchers
    assert not engine._quarantined
    assert all(not adapter.unwatch_calls for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_candidate_books_are_reused_by_selected_route_scan_without_overlap(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})
    await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    result = await engine.scan_once("A039", Decimal("0.1"), 2)

    assert result.quotes
    assert max(adapters[Venue.BYBIT].peak_books.values()) == 1
    assert max(adapters[Venue.OKX].peak_books.values()) == 1
    await engine.close()


@pytest.mark.asyncio
async def test_concurrent_selected_scans_never_overlap_same_book_subscription(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    first, second = await asyncio.gather(
        engine.scan_once("A039", Decimal("0.1"), 2),
        engine.scan_once("A039", Decimal("0.1"), 2),
    )

    assert first.quotes
    assert second.quotes
    assert all(max(adapter.peak_books.values()) == 1 for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_concurrent_candidate_and_selected_scans_share_book_transport_lock(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, debounce_ms=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    candidate, selected = await asyncio.gather(
        engine.scan_candidate_l2(
            2,
            active_route_keys=active,
            candidates_admitted=False,
        ),
        engine.scan_once("A039", Decimal("0.1"), 2),
    )

    assert candidate.routes[0].reason == ReasonCode.QUOTE_READY
    assert selected.quotes
    assert all(max(adapter.peak_books.values()) == 1 for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_selected_scan_rejects_mixed_adapter_generations(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: GenerationChangingFundingAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise()
    old_generation = engine._venue_refresh_generations[Venue.OKX]
    adapters[Venue.OKX].on_funding = lambda: engine._venue_refresh_generations.__setitem__(
        Venue.OKX,
        old_generation + 1,
    )

    result = await engine.scan_once("A039", Decimal("0.1"), 2)

    assert result.quotes == ()
    assert all(not adapter.book_calls for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_overload_selected_scan_touches_only_exact_active_route_legs(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: FundingCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active_key = ("A039", Venue.BYBIT.value, Venue.OKX.value)
    active = frozenset({active_key})
    await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    result = await engine.scan_once(
        "A039",
        Decimal("0.1"),
        2,
        active_route_keys=active,
        entry_work_admitted=False,
    )

    assert adapters[Venue.BINANCE_USDM].book_calls == {}
    assert {
        (quote.key.base, quote.long_venue.value, quote.short_venue.value) for quote in result.quotes
    } == {active_key}
    await engine.close()


@pytest.mark.asyncio
async def test_candidate_read_revalidates_freshness_without_a_new_stream_event(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: OneThenSilentCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})
    ready = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    assert ready.routes[0].reason == ReasonCode.QUOTE_READY

    clock[0] += 500_000_000
    delayed = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    assert delayed.routes[0].reason == ReasonCode.QUOTE_READY
    assert delayed.routes[0].decision_latency_ms == Decimal(520)
    assert delayed.decision_latency_ms >= Decimal(520)
    assert delayed.stats.decision_latency_p95_ms is not None
    assert delayed.stats.decision_latency_p95_ms >= Decimal(520)

    clock[0] += 520_000_000
    stale = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert stale.routes[0].reason == ReasonCode.BOOK_STALE
    await engine.close()


@pytest.mark.asyncio
async def test_any_venue_quarantine_invalidates_candidate_route_state(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})
    ready = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    assert ready.routes[0].reason == ReasonCode.QUOTE_READY

    engine._quarantine(Venue.OKX, "fixture public outage")
    await asyncio.sleep(0)
    failed = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert failed.routes[0].reason == ReasonCode.VENUE_OUTAGE
    await engine.close()


@pytest.mark.asyncio
async def test_active_plan_created_during_venue_outage_is_reason_coded(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise()
    engine._quarantine(Venue.OKX, "fixture public outage")
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    failed = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert failed.routes[0].reason == ReasonCode.VENUE_OUTAGE
    await engine.close()


@pytest.mark.asyncio
async def test_missing_active_route_keeps_venue_outage_reason_after_refresh(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise()
    adapters[Venue.OKX].fail_probe = True
    engine._quarantine(Venue.OKX, "fixture public outage")
    await engine.refresh_universe(2, force=True)
    active = frozenset({("A039", Venue.BYBIT.value, Venue.OKX.value)})

    failed = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert failed.routes[0].reason == ReasonCode.VENUE_OUTAGE
    await engine.close()


@pytest.mark.asyncio
async def test_candidate_plan_churn_coalesces_100k_updates_into_one_debouncer(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1, debounce_ms=10),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise(1)
    snapshot = engine._universe.snapshot
    assert snapshot is not None
    first_route = snapshot.routes[0]
    second_route = next(
        route
        for route in snapshot.routes
        if route.long_instrument.base != first_route.long_instrument.base
    )

    def plan(route: UniverseRoute) -> CandidateL2Plan:
        books = tuple(
            CandidateL2BookPlan(instrument, L2WorkPriority.CANDIDATE_ROUTE)
            for instrument in (route.long_instrument, route.short_instrument)
        )
        return CandidateL2Plan((), (route,), books)

    first = plan(first_route)
    second = plan(second_route)

    latest_version = 0
    for index in range(100_000):
        latest_version = engine._request_candidate_l2_plan(first if index % 2 else second)
    await engine._wait_for_candidate_l2_plan(latest_version, 1)
    await asyncio.sleep(0)

    assert engine._candidate_l2_plan.signature == first.signature
    assert engine._candidate_l2_coalesced_plans >= 99_999
    assert engine._candidate_l2_debouncer is not None
    assert not engine._candidate_l2_debouncer.done()
    assert set(engine._candidate_l2_watchers) == {book.key for book in first.books}
    assert len(engine._candidate_l2_watchers) == 2
    assert all(max(adapter.peak_books.values(), default=0) <= 1 for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_active_priority_refresh_preserves_unchanged_candidate_subscriptions(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=5, debounce_ms=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    initial = await engine.scan_candidate_l2(2)
    active_key = initial.routes[0].stable_key
    watcher_keys = set(engine._candidate_l2_watchers)
    unwatch_before = sum(sum(adapter.unwatch_calls.values()) for adapter in adapters.values())

    refreshed = await engine.scan_candidate_l2(
        2,
        active_route_keys=frozenset({active_key}),
        candidates_admitted=True,
        prefilter=(),
        preserve_existing_candidates=True,
    )

    assert refreshed.stats.active_routes == 1
    assert watcher_keys.issubset(engine._candidate_l2_watchers)
    assert (
        sum(sum(adapter.unwatch_calls.values()) for adapter in adapters.values()) == unwatch_before
    )
    await engine.close()


@pytest.mark.asyncio
async def test_candidate_replacement_unsubscribes_removed_books_without_overlap(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1, debounce_ms=10),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise(1)
    snapshot = engine._universe.snapshot
    assert snapshot is not None
    first_route = snapshot.routes[0]
    second_route = next(
        route
        for route in snapshot.routes
        if route.long_instrument.base != first_route.long_instrument.base
    )

    def plan(route: UniverseRoute) -> CandidateL2Plan:
        return CandidateL2Plan(
            (),
            (route,),
            tuple(
                CandidateL2BookPlan(instrument, L2WorkPriority.CANDIDATE_ROUTE)
                for instrument in (route.long_instrument, route.short_instrument)
            ),
        )

    first = plan(first_route)
    first_version = engine._request_candidate_l2_plan(first)
    await engine._wait_for_candidate_l2_plan(first_version, 1)
    first_tasks = tuple(engine._candidate_l2_watchers.values())
    second = plan(second_route)
    second_version = engine._request_candidate_l2_plan(second)
    await engine._wait_for_candidate_l2_plan(second_version, 1)
    await asyncio.sleep(0)

    assert set(engine._candidate_l2_watchers) == {book.key for book in second.books}
    assert all(task.done() for task in first_tasks)
    assert not engine._retiring_candidate_l2_watchers
    assert sum(sum(adapter.unwatch_calls.values()) for adapter in adapters.values()) == 2
    assert all(max(adapter.peak_books.values(), default=0) <= 1 for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_delayed_unsubscribe_ack_does_not_create_false_venue_outage(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: DelayedUnwatchCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1, debounce_ms=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise(1)
    snapshot = engine._universe.snapshot
    assert snapshot is not None
    first, second = (
        snapshot.routes[0],
        next(
            route
            for route in snapshot.routes
            if route.long_instrument.base != snapshot.routes[0].long_instrument.base
        ),
    )

    def plan(route: UniverseRoute) -> CandidateL2Plan:
        return CandidateL2Plan(
            (),
            (route,),
            tuple(
                CandidateL2BookPlan(instrument, L2WorkPriority.CANDIDATE_ROUTE)
                for instrument in (route.long_instrument, route.short_instrument)
            ),
        )

    version = engine._request_candidate_l2_plan(plan(first))
    await engine._wait_for_candidate_l2_plan(version, 2)
    version = engine._request_candidate_l2_plan(plan(second))
    await engine._wait_for_candidate_l2_plan(version, 2)

    assert not engine._quarantined
    assert sum(sum(adapter.unwatch_calls.values()) for adapter in adapters.values()) == 2
    await engine.close()


@pytest.mark.asyncio
async def test_new_active_watchers_start_before_unrelated_candidate_unsubscribe_ack(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: DelayedUnwatchCandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1, debounce_ms=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    first = frozenset({("A000", Venue.BYBIT.value, Venue.OKX.value)})
    second = frozenset({("A001", Venue.BYBIT.value, Venue.OKX.value)})
    await engine.scan_candidate_l2(2, active_route_keys=first, candidates_admitted=False)

    transition = asyncio.create_task(
        engine.scan_candidate_l2(
            2,
            active_route_keys=second,
            candidates_admitted=False,
        )
    )
    await asyncio.wait_for(adapters[Venue.BYBIT].unwatch_started.wait(), timeout=1)
    for _ in range(100):
        if adapters[Venue.BYBIT].book_calls.get("A001/USDT:USDT", 0) and adapters[
            Venue.OKX
        ].book_calls.get("A001/USDT:USDT", 0):
            break
        await asyncio.sleep(0.001)

    assert adapters[Venue.BYBIT].book_calls.get("A001/USDT:USDT", 0) > 0
    assert adapters[Venue.OKX].book_calls.get("A001/USDT:USDT", 0) > 0
    assert adapters[Venue.BYBIT].unwatch_finished.is_set() is False
    result = await transition
    assert result.routes[0].reason == ReasonCode.QUOTE_READY
    assert result.stats.decision_latency_p95_ms is not None
    assert result.stats.decision_latency_p95_ms <= Decimal(250)
    await engine.close()


@pytest.mark.asyncio
async def test_unsubscribe_failure_recycles_adapter_before_resubscribe(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    created: dict[Venue, list[CandidateAdapter]] = {venue: [] for venue in Venue}

    def factory(venue: Venue) -> CandidateAdapter:
        adapter: CandidateAdapter
        if venue == Venue.OKX and not created[venue]:
            adapter = FailingUnwatchCandidateAdapter(venue, clock)
        else:
            adapter = CandidateAdapter(venue, clock)
        created[venue].append(adapter)
        return adapter

    engine = PublicMarketEngine(
        settings(tmp_path, debounce_ms=1),
        adapter_factory=factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    first = frozenset({("A000", Venue.OKX.value, Venue.BYBIT.value)})
    second = frozenset({("A001", Venue.OKX.value, Venue.BYBIT.value)})
    await engine.scan_candidate_l2(2, active_route_keys=first, candidates_admitted=False)
    failed = await engine.scan_candidate_l2(
        2,
        active_route_keys=second,
        candidates_admitted=False,
    )
    assert Venue.OKX in engine._quarantined
    assert failed.routes[0].reason == ReasonCode.VENUE_OUTAGE

    clock[0] += 2_000_000_000
    await engine.refresh_universe(2, reconnected=(Venue.OKX,))
    recovered = await engine.scan_candidate_l2(
        2,
        active_route_keys=second,
        candidates_admitted=False,
    )

    assert len(created[Venue.OKX]) == 2
    assert created[Venue.OKX][0].close_calls == 1
    assert Venue.OKX not in engine._candidate_l2_unsubscribe_failures
    assert recovered.routes[0].reason == ReasonCode.QUOTE_READY
    await engine.close()


@pytest.mark.asyncio
async def test_replacement_publication_invalidates_generation_before_probe(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    created: dict[Venue, list[CandidateAdapter]] = {venue: [] for venue in Venue}

    def factory(venue: Venue) -> CandidateAdapter:
        if venue == Venue.OKX and not created[venue]:
            adapter: CandidateAdapter = FailingUnwatchCandidateAdapter(venue, clock)
        elif venue == Venue.OKX:
            adapter = BlockingProbeCandidateAdapter(venue, clock)
        else:
            adapter = CandidateAdapter(venue, clock)
        created[venue].append(adapter)
        return adapter

    engine = PublicMarketEngine(
        settings(tmp_path, debounce_ms=1),
        adapter_factory=factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    first = frozenset({("A000", Venue.OKX.value, Venue.BYBIT.value)})
    second = frozenset({("A001", Venue.OKX.value, Venue.BYBIT.value)})
    await engine.scan_candidate_l2(2, active_route_keys=first, candidates_admitted=False)
    await engine.scan_candidate_l2(2, active_route_keys=second, candidates_admitted=False)
    generation_before_recycle = engine._venue_refresh_generations[Venue.OKX]
    clock[0] += 2_000_000_000

    refresh = asyncio.create_task(engine.refresh_universe(2, reconnected=(Venue.OKX,)))
    for _ in range(100):
        if len(created[Venue.OKX]) == 2:
            break
        await asyncio.sleep(0.001)
    assert len(created[Venue.OKX]) == 2
    replacement = created[Venue.OKX][1]
    assert isinstance(replacement, BlockingProbeCandidateAdapter)
    await asyncio.wait_for(replacement.probe_started.wait(), timeout=1)

    assert engine._venue_refresh_generations[Venue.OKX] > generation_before_recycle
    assert Venue.OKX in engine._quarantined
    replacement.allow_probe.set()
    await refresh
    await engine.close()


@pytest.mark.asyncio
async def test_replacement_is_unavailable_before_probe_without_prior_quarantine(
    tmp_path: Path,
) -> None:
    clock = [time.monotonic_ns()]
    created: dict[Venue, list[CandidateAdapter]] = {venue: [] for venue in Venue}

    def factory(venue: Venue) -> CandidateAdapter:
        adapter: CandidateAdapter
        if venue == Venue.OKX and not created[venue]:
            adapter = HeldFundingAndUnwatchCandidateAdapter(venue, clock)
        elif venue == Venue.OKX:
            adapter = BlockingProbeFundingCandidateAdapter(venue, clock)
        else:
            adapter = FundingCandidateAdapter(venue, clock)
        created[venue].append(adapter)
        return adapter

    engine = PublicMarketEngine(
        settings(tmp_path, debounce_ms=1),
        adapter_factory=factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    first = frozenset({("A000", Venue.OKX.value, Venue.BYBIT.value)})
    second = frozenset({("A001", Venue.OKX.value, Venue.BYBIT.value)})
    await engine.scan_candidate_l2(
        2,
        active_route_keys=first,
        candidates_admitted=False,
    )
    old = created[Venue.OKX][0]
    assert isinstance(old, HeldFundingAndUnwatchCandidateAdapter)
    assert Venue.OKX not in engine._quarantined

    selected = asyncio.create_task(engine.scan_once("A039", Decimal("0.1"), 2))
    await asyncio.wait_for(old.funding_started.wait(), timeout=1)
    transition = asyncio.create_task(
        engine.scan_candidate_l2(
            2,
            active_route_keys=second,
            candidates_admitted=False,
        )
    )
    await asyncio.wait_for(old.unwatch_started.wait(), timeout=1)
    generation_before_recycle = engine._venue_refresh_generations[Venue.OKX]

    refresh = asyncio.create_task(engine.refresh_universe(2, reconnected=(Venue.OKX,)))
    for _ in range(100):
        if len(created[Venue.OKX]) == 2:
            break
        await asyncio.sleep(0.001)
    assert len(created[Venue.OKX]) == 2
    replacement = created[Venue.OKX][1]
    assert isinstance(replacement, BlockingProbeFundingCandidateAdapter)
    await asyncio.wait_for(replacement.probe_started.wait(), timeout=1)

    old.allow_funding.set()
    selected_result = await asyncio.wait_for(selected, timeout=1)
    transition_result = await asyncio.wait_for(transition, timeout=1)
    assert engine._venue_refresh_generations[Venue.OKX] > generation_before_recycle
    assert Venue.OKX in engine._quarantined
    assert engine._quarantined[Venue.OKX].reason == "replacement capability validation pending"
    assert selected_result.quotes == ()
    assert selected_result.funding == ()
    assert selected_result.data_quality == ()
    assert transition_result.routes[0].reason == ReasonCode.VENUE_OUTAGE
    assert replacement.book_calls == {}

    replacement.allow_probe.set()
    await refresh
    await engine.close()


def test_overload_admission_sheds_p6_then_p5_before_disabling_p4() -> None:
    controller = OverloadController(10)
    controller.update_pending_by_class(
        {
            WorkClass.CLOSE: 2,
            WorkClass.NEW_ENTRY: 6,
            WorkClass.CANDIDATE_L2: 2,
            WorkClass.BROAD_BBO: 2,
        }
    )

    assert controller.shed_plan() == (WorkClass.BROAD_BBO, WorkClass.BROAD_BBO)
    assert controller.admit(WorkClass.BROAD_BBO).reason == ReasonCode.OVERLOAD_BROAD_SHED
    assert controller.admit(WorkClass.CANDIDATE_L2).reason == ReasonCode.OVERLOAD_CANDIDATE_SHED
    assert controller.admit(WorkClass.NEW_ENTRY).accepted is True
    assert controller.admit(WorkClass.CLOSE).accepted is True

    controller.update_pending_by_class(
        {
            WorkClass.CLOSE: 4,
            WorkClass.NEW_ENTRY: 7,
            WorkClass.CANDIDATE_L2: 1,
            WorkClass.BROAD_BBO: 1,
        }
    )
    assert controller.shed_plan() == (WorkClass.BROAD_BBO, WorkClass.CANDIDATE_L2)
    assert controller.admit(WorkClass.CANDIDATE_L2).reason == ReasonCode.OVERLOAD_CANDIDATE_SHED
    assert controller.admit(WorkClass.NEW_ENTRY).reason == ReasonCode.OVERLOAD_ENTRY_DISABLED
    assert controller.admit(WorkClass.HEDGE).accepted is True

    controller.update_pending_by_class(
        {
            WorkClass.CLOSE: 10,
            WorkClass.BROAD_BBO: 100,
        }
    )
    assert controller.admit(WorkClass.CANDIDATE_L2).reason == ReasonCode.OVERLOAD_CANDIDATE_SHED
    assert controller.admit(WorkClass.NEW_ENTRY).reason == ReasonCode.OVERLOAD_ENTRY_DISABLED


@pytest.mark.asyncio
async def test_l2_transport_lock_registry_prunes_unknown_unowned_keys(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    engine = PublicMarketEngine(
        settings(tmp_path),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    await engine.initialise()
    removed_key = (Venue.OKX, "REMOVED/USDT:USDT")
    held_lock = await engine._acquire_l2_transport_lock(removed_key)
    waiter = asyncio.create_task(engine._acquire_l2_transport_lock(removed_key))
    await asyncio.sleep(0)

    engine._release_l2_transport_lock(removed_key, held_lock)
    engine._prune_l2_transport_locks()
    assert engine._l2_transport_locks[removed_key] is held_lock
    waiter_lock = await waiter
    assert waiter_lock is held_lock
    engine._release_l2_transport_lock(removed_key, waiter_lock)

    assert removed_key not in engine._l2_transport_locks
    await engine.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "reason"),
    (
        ("crossed", ReasonCode.BOOK_CROSSED),
        ("unknown_sequence", ReasonCode.BOOK_SEQUENCE_UNKNOWN),
        ("sequence_gap", ReasonCode.BOOK_SEQUENCE_GAP),
        ("stale", ReasonCode.BOOK_STALE),
        ("clock_unknown", ReasonCode.CLOCK_SKEW_UNKNOWN),
    ),
)
async def test_candidate_l2_quality_failure_is_reason_coded_and_never_executable(
    tmp_path: Path,
    mode: str,
    reason: ReasonCode,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    for adapter in adapters.values():
        if mode == "crossed":
            adapter.crossed_l2 = True
        else:
            adapter.quality_mode = mode
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    result = await engine.scan_candidate_l2(2)
    await engine.close()

    assert len(result.routes) == 1
    assert result.routes[0].reason == reason
    assert result.routes[0].execution_authorized is False


@pytest.mark.asyncio
async def test_candidate_l2_venue_failure_isolated_then_explicit_reconnect_recovers(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
    adapters[Venue.OKX].fail_l2 = True
    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=6),
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    failed = await engine.scan_candidate_l2(2)
    assert Venue.OKX in engine._quarantined
    assert engine._reconnect_attempts[Venue.OKX] == 1
    assert any(route.reason == ReasonCode.VENUE_OUTAGE for route in failed.routes)

    adapters[Venue.OKX].fail_l2 = False
    clock[0] += 2_000_000_000
    await engine.refresh_universe(2, reconnected=(Venue.OKX,))
    recovered = await engine.scan_candidate_l2(2)

    assert Venue.OKX not in engine._quarantined
    assert any(
        route.reason == ReasonCode.QUOTE_READY
        and Venue.OKX in {route.long_venue, route.short_venue}
        for route in recovered.routes
    )
    await engine.close()


@pytest.mark.asyncio
async def test_cancellation_resistant_l2_recycles_one_adapter_before_recovery(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    created: dict[Venue, list[CandidateAdapter]] = {venue: [] for venue in Venue}

    def factory(venue: Venue) -> CandidateAdapter:
        adapter: CandidateAdapter
        if venue == Venue.OKX and not created[venue]:
            adapter = CancellationResistantCandidateAdapter(venue, clock)
        else:
            adapter = CandidateAdapter(venue, clock)
        created[venue].append(adapter)
        return adapter

    engine = PublicMarketEngine(
        settings(tmp_path, maximum_candidates=1),
        adapter_factory=factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )
    active = frozenset({("A039", Venue.OKX.value, Venue.BYBIT.value)})

    failed = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )
    assert failed.routes[0].reason == ReasonCode.VENUE_OUTAGE
    assert Venue.OKX in engine._quarantined

    clock[0] += 2_000_000_000
    recovered = await engine.scan_candidate_l2(
        2,
        active_route_keys=active,
        candidates_admitted=False,
    )

    assert len(created[Venue.OKX]) == 2
    assert created[Venue.OKX][0].close_calls == 1
    assert recovered.routes[0].reason == ReasonCode.QUOTE_READY
    assert Venue.OKX not in engine._quarantined
    await engine.close()


@pytest.mark.asyncio
async def test_candidate_l2_restart_is_identical_and_leaves_no_tasks(tmp_path: Path) -> None:
    clock = [1_000_000_000]

    async def run_once() -> tuple[tuple[tuple[str, str, str], ...], int, int]:
        adapters = {venue: CandidateAdapter(venue, clock) for venue in Venue}
        engine = PublicMarketEngine(
            settings(tmp_path, maximum_candidates=5),
            adapter_factory=adapters.__getitem__,
            recorder=ParquetMarketRecorder(tmp_path),
            monotonic_ns=lambda: clock[0],
        )
        result = await engine.scan_candidate_l2(2)
        identity = (
            tuple(route.stable_key for route in result.routes),
            result.stats.known_books,
            result.stats.active_watchers,
        )
        await engine.close()
        await asyncio.sleep(0)
        assert not tuple(
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith("candidate-l2-")
            and not task.done()
        )
        return identity

    assert await run_once() == await run_once()


def test_plan_selection_is_stable_and_active_priority_overrides_candidate() -> None:
    bybit = instruments(Venue.BYBIT, 1)[0]
    okx = instruments(Venue.OKX, 1)[0]
    route = UniverseRoute(bybit, okx)
    reverse = UniverseRoute(okx, bybit)
    from interexchange_perp_grid.bbo_prefilter import BboPrefilterObservation

    observations = (
        BboPrefilterObservation(
            "A000",
            Venue.BYBIT,
            Venue.OKX,
            bybit.symbol,
            okx.symbol,
            Decimal(1),
            Decimal(1),
            Decimal(0),
            1,
            ReasonCode.QUOTE_READY,
        ),
        BboPrefilterObservation(
            "A000",
            Venue.OKX,
            Venue.BYBIT,
            okx.symbol,
            bybit.symbol,
            Decimal(1),
            Decimal(1),
            Decimal(0),
            1,
            ReasonCode.QUOTE_READY,
        ),
        BboPrefilterObservation(
            "A000",
            Venue.OKX,
            Venue.BYBIT,
            okx.symbol,
            bybit.symbol,
            Decimal(1),
            Decimal(1),
            Decimal(0),
            1,
            ReasonCode.QUOTE_READY,
        ),
    )

    plan = build_candidate_l2_plan(
        (route, reverse),
        observations,
        active_route_keys=frozenset({route.stable_key}),
        maximum_candidates=1,
        candidates_admitted=True,
    )

    assert plan.active_routes == (route,)
    assert plan.candidate_routes == (reverse,)
    assert len(plan.books) == 2
    assert all(book.priority == L2WorkPriority.ACTIVE_ROUTE for book in plan.books)


def test_ready_candidate_book_is_rejected_after_freshness_expires() -> None:
    bybit = instruments(Venue.BYBIT, 1)[0]
    okx = instruments(Venue.OKX, 1)[0]
    route = UniverseRoute(bybit, okx)
    plan = CandidateL2Plan(
        (),
        (route,),
        (
            CandidateL2BookPlan(bybit, L2WorkPriority.CANDIDATE_ROUTE),
            CandidateL2BookPlan(okx, L2WorkPriority.CANDIDATE_ROUTE),
        ),
    )

    def state(instrument: Instrument) -> CandidateL2BookState:
        book = OrderBookSnapshot(
            instrument.venue,
            instrument.symbol,
            (BookLevel(Decimal(100), Decimal(1)),),
            (BookLevel(Decimal(101), Decimal(1)),),
            1_700_000_000_000,
            datetime.now(UTC),
            1_000_000_000,
            1,
            1,
            True,
            True,
            0,
        )
        return CandidateL2BookState(
            book,
            DataQualityAssessment(True, ReasonCode.QUOTE_READY, 0),
            L2WorkPriority.CANDIDATE_ROUTE,
        )

    observations = evaluate_candidate_l2_routes(
        plan,
        {
            (bybit.venue, bybit.symbol): state(bybit),
            (okx.venue, okx.symbol): state(okx),
        },
        decision_monotonic_ns=2_000_000_001,
        maximum_age_ms=1_000,
    )

    assert observations[0].reason == ReasonCode.BOOK_STALE
    assert observations[0].execution_authorized is False


def test_missing_active_route_is_reported_fail_closed() -> None:
    missing = ("MISSING", Venue.BYBIT.value, Venue.OKX.value)
    plan = build_candidate_l2_plan(
        (),
        (),
        active_route_keys=frozenset({missing}),
        maximum_candidates=30,
        candidates_admitted=True,
    )

    observations = evaluate_candidate_l2_routes(
        plan,
        {},
        decision_monotonic_ns=1,
        maximum_age_ms=1_000,
    )

    assert plan.missing_active_routes == (missing,)
    assert observations[0].stable_key == missing
    assert observations[0].reason == ReasonCode.CONTRACT_METADATA_UNKNOWN
    assert observations[0].execution_authorized is False
