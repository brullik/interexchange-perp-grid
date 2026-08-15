from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from interexchange_perp_grid.adapters import CcxtProAdapter, ExchangeAdapter
from interexchange_perp_grid.bbo_prefilter import (
    BboCacheStats,
    BboPrefilterObservation,
    LatestBboCache,
    rank_bbo_prefilter,
)
from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import (
    BboQuote,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    QuarantineRecord,
    Venue,
)
from interexchange_perp_grid.history import ParquetMarketRecorder
from interexchange_perp_grid.market_data import BookRegistry, DataQualityAssessment
from interexchange_perp_grid.market_universe import (
    InstrumentRegistry,
    UniverseService,
    UniverseSnapshot,
)
from interexchange_perp_grid.routes import (
    DirectedRouteQuote,
    directed_pairs,
    evaluate_directed_route,
)

AdapterFactory = Callable[[Venue], ExchangeAdapter]


def reconnect_backoff_seconds(attempt: int) -> int:
    if attempt <= 0:
        raise ValueError("reconnect attempt must be positive")
    return 30 if attempt >= 6 else 1 << (attempt - 1)


@dataclass(frozen=True, slots=True)
class ScanResult:
    base: str
    common_instrument_count: int
    bbo: tuple[BboQuote, ...]
    funding: tuple[FundingSnapshot, ...]
    data_quality: tuple[VenueDataQuality, ...]
    quotes: tuple[DirectedRouteQuote, ...]
    capabilities: tuple[CapabilityReport, ...]
    quarantined: tuple[QuarantineRecord, ...]
    directed_route_count: int = 0
    prefilter: tuple[BboPrefilterObservation, ...] = ()
    bbo_cache: BboCacheStats | None = None
    prefilter_latency_ms: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BroadBboResult:
    universe_generation: int
    common_instrument_count: int
    discovered_route_count: int
    directed_route_count: int
    bbo: tuple[BboQuote, ...]
    prefilter: tuple[BboPrefilterObservation, ...]
    cache: BboCacheStats
    prefilter_latency_ms: Decimal
    quarantined: tuple[QuarantineRecord, ...]


@dataclass(frozen=True, slots=True)
class VenueDataQuality:
    venue: Venue
    symbol: str
    assessment: DataQualityAssessment


class PublicMarketEngine:
    def __init__(
        self,
        settings: Settings,
        adapter_factory: AdapterFactory | None = None,
        recorder: ParquetMarketRecorder | None = None,
        *,
        now_factory: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self.settings = settings
        self._adapter_factory = adapter_factory or CcxtProAdapter
        self._recorder = recorder or ParquetMarketRecorder(Path(settings.storage.parquet_dir))
        self._now_factory = now_factory or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns
        self._adapters: dict[Venue, ExchangeAdapter] = {}
        self._capabilities: dict[Venue, CapabilityReport] = {}
        self._instruments: dict[Venue, tuple[Instrument, ...]] = {}
        self._quarantined: dict[Venue, QuarantineRecord] = {}
        self._reconnect_attempts: dict[Venue, int] = {}
        self._reconnect_after_ns: dict[Venue, int] = {}
        self._books = BookRegistry()
        self._universe = UniverseService(
            InstrumentRegistry(
                minimum_listing_age_days=settings.universe.live_min_listing_age_days,
                enforce_listing_age=True,
            ),
            refresh_seconds=settings.universe.instrument_refresh_seconds,
        )
        self._bbo_cache = LatestBboCache(
            maximum_age_ms=settings.market_data.max_bbo_age_ms,
            maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        )
        self._bbo_watchers: dict[Venue, asyncio.Task[None]] = {}
        self._bbo_changed = asyncio.Event()
        self._closed = False

    async def initialise(self, timeout_seconds: int = 30) -> None:
        configured = tuple(Venue(value) for value in self.settings.venues.wave1_public)
        self._adapters = {venue: self._adapter_factory(venue) for venue in configured}
        await asyncio.gather(
            *(self._initialise_venue_with_timeout(venue, timeout_seconds) for venue in configured)
        )
        self._refresh_universe_snapshot(force=True)

    async def _initialise_venue_with_timeout(self, venue: Venue, timeout_seconds: int) -> None:
        try:
            await asyncio.wait_for(self._initialise_venue(venue), timeout=timeout_seconds)
        except TimeoutError:
            self._quarantine(venue, f"capability probe timed out after {timeout_seconds}s")

    async def _initialise_venue(self, venue: Venue) -> None:
        adapter = self._adapters[venue]
        try:
            report = await adapter.probe_public_capabilities()
            self._capabilities[venue] = report
            if not report.public_ready:
                self._quarantine(venue, f"missing capabilities: {', '.join(report.missing)}")
                return
            if report.clock_skew_ms is None:
                self._quarantine(venue, "clock skew is unknown")
                return
            if abs(report.clock_skew_ms) > self.settings.market_data.max_clock_skew_ms:
                self._quarantine(
                    venue,
                    f"clock skew {report.clock_skew_ms}ms exceeds policy",
                )
                return
            instruments = await adapter.discover_instruments()
            if not instruments:
                self._quarantine(venue, "no qualified linear USDT perpetual instruments")
                return
            self._instruments[venue] = instruments
            self._quarantined.pop(venue, None)
            self._reconnect_attempts.pop(venue, None)
            self._reconnect_after_ns.pop(venue, None)
        except Exception as error:
            self._quarantine(venue, f"capability probe failed: {type(error).__name__}: {error}")

    def _quarantine(self, venue: Venue, reason: str) -> None:
        self._quarantined[venue] = QuarantineRecord(venue, reason, self._now_factory())
        self._instruments.pop(venue, None)
        attempt = self._reconnect_attempts.get(venue, 0) + 1
        self._reconnect_attempts[venue] = attempt
        delay_seconds = reconnect_backoff_seconds(attempt)
        self._reconnect_after_ns[venue] = self._monotonic_ns() + delay_seconds * 1_000_000_000
        self._set_available_bbo_keys()
        self._bbo_changed.set()

    def _set_available_bbo_keys(self) -> None:
        snapshot = self._universe.snapshot
        if snapshot is None:
            return
        self._bbo_cache.set_known_keys(
            frozenset(key for key in snapshot.known_bbo_keys if key[0] not in self._quarantined)
        )

    def _symbols_for_venue(self, venue: Venue) -> tuple[str, ...]:
        snapshot = self._universe.snapshot
        if snapshot is None:
            return ()
        return tuple(
            sorted(
                {
                    instrument.symbol
                    for common in snapshot.common
                    for instrument in common.instruments
                    if instrument.venue == venue
                }
            )
        )

    def _sync_bbo_watchers(self) -> None:
        desired = {
            venue
            for venue in self._adapters
            if venue not in self._quarantined and self._symbols_for_venue(venue)
        }
        for venue, task in tuple(self._bbo_watchers.items()):
            if task.done() or venue not in desired:
                if not task.done():
                    task.cancel()
                self._bbo_watchers.pop(venue, None)
        for venue in sorted(desired, key=str):
            if venue not in self._bbo_watchers:
                self._bbo_watchers[venue] = asyncio.create_task(
                    self._run_bbo_watcher(venue),
                    name=f"broad-bbo-{venue.value}",
                )

    async def _run_bbo_watcher(self, venue: Venue) -> None:
        try:
            while not self._closed and venue not in self._quarantined:
                symbols = self._symbols_for_venue(venue)
                if not symbols:
                    return
                started_ns = time.monotonic_ns()
                quotes = await self._adapters[venue].watch_bbo(symbols)
                if not quotes:
                    raise RuntimeError("batch BBO stream returned no updates")
                self._bbo_cache.ingest(
                    quotes,
                    now_monotonic_ns=self._monotonic_ns(),
                )
                self._bbo_changed.set()
                if time.monotonic_ns() - started_ns < 1_000_000:
                    await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._quarantine(
                venue,
                f"BBO stream failed: {type(error).__name__}: {error}",
            )

    def _refresh_universe_snapshot(self, *, force: bool) -> UniverseSnapshot:
        snapshot = self._universe.refresh(
            self._instruments,
            now=self._now_factory(),
            monotonic_ns=self._monotonic_ns(),
            force=force,
        )
        self._set_available_bbo_keys()
        self._sync_bbo_watchers()
        return snapshot

    async def refresh_universe(
        self,
        timeout_seconds: int,
        *,
        force: bool = False,
        reconnected: tuple[Venue, ...] = (),
    ) -> UniverseSnapshot:
        now_ns = self._monotonic_ns()
        due = self._universe.refresh_due(now_ns)
        automatic_reconnects = tuple(
            venue
            for venue, retry_at_ns in self._reconnect_after_ns.items()
            if now_ns >= retry_at_ns
        )
        reconnect_targets = tuple(
            sorted(
                {
                    venue
                    for venue in (*reconnected, *automatic_reconnects)
                    if venue in self._adapters
                },
                key=str,
            )
        )
        if not force and not due and not reconnect_targets:
            current = self._universe.snapshot
            assert current is not None
            return current
        targets = tuple(self._adapters) if force or due else reconnect_targets
        await asyncio.gather(
            *(self._initialise_venue_with_timeout(venue, timeout_seconds) for venue in targets)
        )
        return self._refresh_universe_snapshot(force=True)

    async def _wait_for_bbo_coverage(self, timeout_seconds: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while True:
            self._bbo_changed.clear()
            now_ns = self._monotonic_ns()
            if (
                len(self._bbo_cache.fresh(now_monotonic_ns=now_ns))
                >= self._bbo_cache.stats.known_keys
            ):
                return
            active_watchers = tuple(task for task in self._bbo_watchers.values() if not task.done())
            remaining = deadline - loop.time()
            if not active_watchers or remaining <= 0:
                return
            try:
                await asyncio.wait_for(self._bbo_changed.wait(), timeout=remaining)
            except TimeoutError:
                return

    async def scan_broad_bbo(self, timeout_seconds: int) -> BroadBboResult:
        if not self._adapters:
            await self.initialise(timeout_seconds)
        universe = await self.refresh_universe(timeout_seconds)
        self._sync_bbo_watchers()
        await self._wait_for_bbo_coverage(timeout_seconds)
        now_ns = self._monotonic_ns()
        available_routes = tuple(
            route
            for route in universe.routes
            if route.long_instrument.venue not in self._quarantined
            and route.short_instrument.venue not in self._quarantined
        )
        fresh = self._bbo_cache.fresh(now_monotonic_ns=now_ns)
        started_ns = time.perf_counter_ns()
        prefilter = rank_bbo_prefilter(available_routes, fresh)
        latency_ms = Decimal(time.perf_counter_ns() - started_ns) / Decimal(1_000_000)
        return BroadBboResult(
            universe.generation,
            len(universe.common),
            len(universe.routes),
            len(available_routes),
            fresh,
            prefilter,
            self._bbo_cache.stats,
            latency_ms,
            tuple(self._quarantined[venue] for venue in sorted(self._quarantined, key=str)),
        )

    async def scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
    ) -> ScanResult:
        if not self._adapters:
            await self.initialise(timeout_seconds)
        broad = await self.scan_broad_bbo(timeout_seconds)
        universe = self._universe.snapshot
        assert universe is not None
        common = universe.common
        selected = next(
            (
                item
                for item in common
                if item.key.base == base.upper() and item.key.settle == "USDT"
            ),
            None,
        )
        if selected is None:
            return self._result(base, len(common), (), (), (), (), broad)
        selected_keys = {
            (instrument.venue, instrument.symbol) for instrument in selected.instruments
        }
        bbo = tuple(quote for quote in broad.bbo if (quote.venue, quote.symbol) in selected_keys)
        funding_samples = await asyncio.gather(
            *(
                self._sample_funding(instrument, timeout_seconds)
                for instrument in selected.instruments
                if instrument.venue not in self._quarantined
            )
        )
        funding_by_venue = {
            instrument.venue: (instrument, funding)
            for instrument, funding in funding_samples
            if funding is not None and instrument.venue not in self._quarantined
        }
        warmup_samples = await asyncio.gather(
            *(
                self._sample_book(instrument, timeout_seconds)
                for instrument, _ in funding_by_venue.values()
            )
        )
        warmed_instruments = tuple(
            instrument
            for instrument, book in warmup_samples
            if book is not None and instrument.venue not in self._quarantined
        )
        book_samples = await asyncio.gather(
            *(self._sample_book(instrument, timeout_seconds) for instrument in warmed_instruments)
        )
        complete = {
            instrument.venue: (instrument, book, funding_by_venue[instrument.venue][1])
            for instrument, book in book_samples
            if book is not None and instrument.venue in funding_by_venue
        }
        books = tuple(sample[1] for sample in complete.values())
        quality: dict[Venue, DataQualityAssessment] = {}
        for venue, (_, book, _) in complete.items():
            quality[venue] = self._books.accept(
                book,
                max_age_ms=self.settings.market_data.max_l2_age_ms,
                max_clock_skew_ms=self.settings.market_data.max_clock_skew_ms,
            )
            if not quality[venue].accepted:
                self._quarantine(venue, f"L2 quality failed: {quality[venue].reason.value}")
        data_quality = tuple(
            VenueDataQuality(venue, complete[venue][0].symbol, quality[venue])
            for venue in sorted(quality, key=str)
        )
        quotes = tuple(
            evaluate_directed_route(
                long_instrument,
                short_instrument,
                complete[long_instrument.venue][1],
                complete[short_instrument.venue][1],
                complete[long_instrument.venue][2],
                complete[short_instrument.venue][2],
                quality[long_instrument.venue],
                quality[short_instrument.venue],
                requested_base_quantity,
            )
            for long_instrument, short_instrument in directed_pairs(selected)
            if long_instrument.venue in complete and short_instrument.venue in complete
        )
        if books:
            await self._recorder.append_books(books)
        return self._result(
            base,
            len(common),
            bbo,
            tuple(item[2] for item in complete.values()),
            data_quality,
            quotes,
            broad,
        )

    async def _sample_funding(
        self,
        instrument: Instrument,
        timeout_seconds: int,
    ) -> tuple[Instrument, FundingSnapshot | None]:
        adapter = self._adapters[instrument.venue]
        try:
            funding = await asyncio.wait_for(
                adapter.fetch_funding(instrument),
                timeout=timeout_seconds,
            )
            return instrument, funding
        except Exception as error:
            self._quarantine(
                instrument.venue,
                f"funding data failed: {type(error).__name__}: {error}",
            )
            return instrument, None

    async def _sample_book(
        self,
        instrument: Instrument,
        timeout_seconds: int,
    ) -> tuple[Instrument, OrderBookSnapshot | None]:
        adapter = self._adapters[instrument.venue]
        try:
            book = await asyncio.wait_for(
                adapter.watch_order_book(instrument),
                timeout=timeout_seconds,
            )
            return instrument, book
        except Exception as error:
            self._quarantine(
                instrument.venue,
                f"L2 stream failed: {type(error).__name__}: {error}",
            )
            return instrument, None

    def _result(
        self,
        base: str,
        common_count: int,
        bbo: tuple[BboQuote, ...],
        funding: tuple[FundingSnapshot, ...],
        data_quality: tuple[VenueDataQuality, ...],
        quotes: tuple[DirectedRouteQuote, ...],
        broad: BroadBboResult | None = None,
    ) -> ScanResult:
        return ScanResult(
            base=base.upper(),
            common_instrument_count=common_count,
            bbo=bbo,
            funding=funding,
            data_quality=data_quality,
            quotes=quotes,
            capabilities=tuple(
                self._capabilities[venue] for venue in sorted(self._capabilities, key=str)
            ),
            quarantined=tuple(
                self._quarantined[venue] for venue in sorted(self._quarantined, key=str)
            ),
            directed_route_count=broad.directed_route_count if broad is not None else 0,
            prefilter=broad.prefilter if broad is not None else (),
            bbo_cache=broad.cache if broad is not None else None,
            prefilter_latency_ms=broad.prefilter_latency_ms if broad is not None else None,
        )

    async def close(self) -> None:
        self._closed = True
        watchers = tuple(self._bbo_watchers.values())
        self._bbo_watchers.clear()
        for task in watchers:
            task.cancel()
        if watchers:
            done, pending = await asyncio.wait(watchers, timeout=1)
            for task in done:
                self._consume_watcher(task)
            for task in pending:
                task.add_done_callback(self._consume_watcher)
        await asyncio.gather(
            *(adapter.close() for adapter in self._adapters.values()),
            return_exceptions=True,
        )

    @staticmethod
    def _consume_watcher(task: asyncio.Task[None]) -> None:
        with suppress(asyncio.CancelledError, Exception):
            task.result()
