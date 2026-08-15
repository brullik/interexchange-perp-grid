from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from random import SystemRandom

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
ReconnectJitter = Callable[[Venue, int], Decimal]
_RECONNECT_RANDOM = SystemRandom()


def reconnect_backoff_seconds(attempt: int) -> int:
    if attempt <= 0:
        raise ValueError("reconnect attempt must be positive")
    return 30 if attempt >= 6 else 1 << (attempt - 1)


def _default_reconnect_jitter(venue: Venue, attempt: int) -> Decimal:
    del venue, attempt
    return Decimal(str(_RECONNECT_RANDOM.uniform(0.8, 1.2)))


def reconnect_delay_seconds(
    venue: Venue,
    attempt: int,
    jitter: ReconnectJitter = _default_reconnect_jitter,
) -> Decimal:
    factor = jitter(venue, attempt)
    if not factor.is_finite() or not Decimal("0.8") <= factor <= Decimal("1.2"):
        raise ValueError("reconnect jitter must be finite and within [0.8, 1.2]")
    delay = Decimal(reconnect_backoff_seconds(attempt)) * factor
    return max(Decimal(1), min(Decimal(30), delay))


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
        reconnect_jitter: ReconnectJitter = _default_reconnect_jitter,
    ) -> None:
        self.settings = settings
        self._adapter_factory = adapter_factory or CcxtProAdapter
        self._recorder = recorder or ParquetMarketRecorder(Path(settings.storage.parquet_dir))
        self._now_factory = now_factory or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns
        self._reconnect_jitter = reconnect_jitter
        self._adapters: dict[Venue, ExchangeAdapter] = {}
        self._capabilities: dict[Venue, CapabilityReport] = {}
        self._instruments: dict[Venue, tuple[Instrument, ...]] = {}
        self._quarantined: dict[Venue, QuarantineRecord] = {}
        self._reconnect_attempts: dict[Venue, int] = {}
        self._reconnect_after_ns: dict[Venue, int] = {}
        self._recycle_failure_generations: dict[Venue, int] = {}
        self._venue_refresh_generations: dict[Venue, int] = {}
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
        self._bbo_watcher_symbols: dict[Venue, tuple[str, ...]] = {}
        self._retiring_bbo_watchers: dict[Venue, asyncio.Task[None]] = {}
        self._retiring_bbo_transports: dict[Venue, asyncio.Task[tuple[BboQuote, ...]]] = {}
        self._retiring_adapter_closers: dict[Venue, asyncio.Task[None]] = {}
        self._adapter_recycle_locks: dict[Venue, asyncio.Lock] = {}
        self._lifecycle_lock = asyncio.Lock()
        self._active_public_scans: set[asyncio.Task[object]] = set()
        self._public_scans_idle = asyncio.Event()
        self._public_scans_idle.set()
        self._bbo_changed = asyncio.Event()
        self._bbo_watch_timeout_seconds = settings.market_data.max_bbo_age_ms / 1000
        self._bbo_retirement_grace_seconds = min(
            0.1,
            max(0.01, self._bbo_watch_timeout_seconds),
        )
        self._initialised = False
        self._closed = False

    async def initialise(self, timeout_seconds: int = 30) -> None:
        async with self._lifecycle_lock:
            self._require_open()
            if self._initialised:
                return
            if not self._adapters:
                staged_adapters: dict[Venue, ExchangeAdapter] = {}
                try:
                    for value in self.settings.venues.wave1_public:
                        venue = Venue(value)
                        staged_adapters[venue] = self._adapter_factory(venue)
                except Exception:
                    await self._close_unpublished_adapters(staged_adapters)
                    raise
                self._adapters = staged_adapters
            configured = tuple(self._adapters)
            await asyncio.gather(
                *(
                    self._initialise_venue_with_timeout(venue, timeout_seconds)
                    for venue in configured
                )
            )
            self._require_open()
            await self._refresh_universe_snapshot(force=True)
            self._require_open()
            self._initialised = True

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("public market engine is closed")

    def _begin_public_scan(self) -> asyncio.Task[object]:
        self._require_open()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("public scan requires an asyncio task")
        self._active_public_scans.add(task)
        self._public_scans_idle.clear()
        return task

    def _finish_public_scan(self, task: asyncio.Task[object]) -> None:
        self._active_public_scans.discard(task)
        if not self._active_public_scans:
            self._public_scans_idle.set()

    async def _close_unpublished_adapters(
        self,
        adapters: dict[Venue, ExchangeAdapter],
    ) -> None:
        if not adapters:
            return
        self._adapters = dict(adapters)
        closers = {
            venue: asyncio.create_task(adapter.close(), name=f"rollback-close-{venue.value}")
            for venue, adapter in adapters.items()
        }
        self._retiring_adapter_closers.update(closers)
        try:
            done, _pending = await asyncio.shield(asyncio.wait(tuple(closers.values()), timeout=1))
        except asyncio.CancelledError:
            self._closed = True
            raise
        failures: list[str] = []
        for venue, closer in closers.items():
            if closer in done:
                self._retiring_adapter_closers.pop(venue, None)
                try:
                    closer.result()
                except (asyncio.CancelledError, Exception) as error:
                    failures.append(f"{venue.value}: {type(error).__name__}: {error}")
                continue
            closer.cancel()
            closer.add_done_callback(self._consume_watcher)
            failures.append(f"{venue.value}: rollback deadline exceeded")
        if failures:
            self._closed = True
            raise RuntimeError(f"public adapter factory rollback failed: {'; '.join(failures)}")
        self._adapters.clear()

    async def _initialise_venue_with_timeout(self, venue: Venue, timeout_seconds: int) -> None:
        try:
            await asyncio.wait_for(self._initialise_venue(venue), timeout=timeout_seconds)
        except TimeoutError:
            if self._closed:
                return
            self._quarantine(venue, f"capability probe timed out after {timeout_seconds}s")
            self._record_venue_refresh(venue)

    async def _initialise_venue(self, venue: Venue) -> None:
        adapter = self._adapters[venue]
        report: CapabilityReport | None = None
        instruments: tuple[Instrument, ...] = ()
        failure_reason: str | None = None
        try:
            report = await adapter.probe_public_capabilities()
            if self._closed:
                return
            if not report.public_ready:
                failure_reason = f"missing capabilities: {', '.join(report.missing)}"
            elif report.clock_skew_ms is None:
                failure_reason = "clock skew is unknown"
            elif abs(report.clock_skew_ms) > self.settings.market_data.max_clock_skew_ms:
                failure_reason = f"clock skew {report.clock_skew_ms}ms exceeds policy"
            else:
                instruments = await adapter.discover_instruments()
                if self._closed:
                    return
                if not instruments:
                    failure_reason = "no qualified linear USDT perpetual instruments"
        except Exception as error:
            if self._closed:
                return
            failure_reason = f"capability probe failed: {type(error).__name__}: {error}"

        if self._closed:
            return
        if report is not None:
            self._capabilities[venue] = report
        if failure_reason is None:
            assert report is not None
            self._instruments[venue] = instruments
            self._quarantined.pop(venue, None)
            self._record_venue_refresh(venue)
            return
        self._quarantine(venue, failure_reason)
        self._record_venue_refresh(venue)

    def _record_venue_refresh(self, venue: Venue) -> None:
        self._venue_refresh_generations[venue] = self._venue_refresh_generations.get(venue, 0) + 1

    def _quarantine(self, venue: Venue, reason: str) -> None:
        self._quarantined[venue] = QuarantineRecord(venue, reason, self._now_factory())
        self._instruments.pop(venue, None)
        attempt = self._reconnect_attempts.get(venue, 0) + 1
        self._reconnect_attempts[venue] = attempt
        delay_seconds = reconnect_delay_seconds(venue, attempt, self._reconnect_jitter)
        self._reconnect_after_ns[venue] = self._monotonic_ns() + int(delay_seconds * 1_000_000_000)
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

    def _cleanup_retired_bbo_tasks(self) -> None:
        for venue, watcher in tuple(self._retiring_bbo_watchers.items()):
            if watcher.done():
                self._retiring_bbo_watchers.pop(venue, None)
                self._consume_watcher(watcher)
        for venue, transport in tuple(self._retiring_bbo_transports.items()):
            if transport.done():
                self._retiring_bbo_transports.pop(venue, None)
                self._consume_transport(transport)

    def _defer_retired_venue_reconnect(self, venue: Venue, reason: str) -> None:
        attempt = max(1, self._reconnect_attempts.get(venue, 1))
        delay_seconds = reconnect_delay_seconds(venue, attempt, self._reconnect_jitter)
        self._reconnect_after_ns[venue] = self._monotonic_ns() + int(delay_seconds * 1_000_000_000)
        self._recycle_failure_generations[venue] = (
            self._recycle_failure_generations.get(venue, 0) + 1
        )
        self._quarantined[venue] = QuarantineRecord(venue, reason, self._now_factory())

    async def _recycle_retired_venue_adapter(
        self,
        venue: Venue,
        timeout_seconds: int,
        *,
        ignore_backoff: bool,
    ) -> bool:
        lock = self._adapter_recycle_locks.setdefault(venue, asyncio.Lock())
        observed_failure_generation = self._recycle_failure_generations.get(venue, 0)
        try:
            await asyncio.wait_for(lock.acquire(), timeout=timeout_seconds)
        except TimeoutError:
            return False
        try:
            if self._closed:
                return False
            retry_at_ns = self._reconnect_after_ns.get(venue)
            failure_advanced_while_waiting = (
                self._recycle_failure_generations.get(venue, 0) != observed_failure_generation
            )
            if (
                (not ignore_backoff or failure_advanced_while_waiting)
                and retry_at_ns is not None
                and self._monotonic_ns() < retry_at_ns
            ):
                return False
            if (
                venue not in self._retiring_bbo_watchers
                and venue not in self._retiring_bbo_transports
                and venue not in self._retiring_adapter_closers
            ):
                return True
            recycled = await self._recycle_retired_venue_adapter_locked(venue, timeout_seconds)
            if not recycled:
                return False
            await self._initialise_venue_with_timeout(venue, timeout_seconds)
            if self._closed:
                self._capabilities.pop(venue, None)
                self._instruments.pop(venue, None)
                self._quarantined[venue] = QuarantineRecord(
                    venue,
                    "engine shutdown interrupted venue recovery",
                    self._now_factory(),
                )
                return False
            return True
        finally:
            lock.release()

    async def _recycle_retired_venue_adapter_locked(
        self,
        venue: Venue,
        timeout_seconds: int,
    ) -> bool:
        watcher = self._bbo_watchers.pop(venue, None)
        self._bbo_watcher_symbols.pop(venue, None)
        if watcher is not None:
            if watcher.done():
                self._consume_watcher(watcher)
            else:
                watcher.cancel()
                self._retiring_bbo_watchers[venue] = watcher
        closer = self._retiring_adapter_closers.get(venue)
        if closer is None:
            closer = asyncio.create_task(
                self._adapters[venue].close(),
                name=f"recycle-{venue.value}",
            )
            self._retiring_adapter_closers[venue] = closer
        done, _ = await asyncio.wait((closer,), timeout=min(1, timeout_seconds))
        if not done:
            closer.cancel()
            closer.add_done_callback(self._consume_watcher)
            self._defer_retired_venue_reconnect(
                venue,
                "retired BBO adapter close exceeded deadline",
            )
            return False
        self._retiring_adapter_closers.pop(venue, None)
        try:
            closer.result()
        except (asyncio.CancelledError, Exception) as error:
            self._defer_retired_venue_reconnect(
                venue,
                f"retired BBO adapter close failed: {type(error).__name__}: {error}",
            )
            return False
        await asyncio.sleep(0)
        retiring = tuple(
            task
            for task in (
                self._retiring_bbo_watchers.get(venue),
                self._retiring_bbo_transports.get(venue),
            )
            if task is not None and not task.done()
        )
        if retiring:
            await asyncio.wait(retiring, timeout=self._bbo_retirement_grace_seconds)
        self._cleanup_retired_bbo_tasks()
        if venue in self._retiring_bbo_watchers or venue in self._retiring_bbo_transports:
            self._defer_retired_venue_reconnect(
                venue,
                "retired BBO transport remained active after adapter close",
            )
            return False
        if self._closed:
            return False
        try:
            replacement = self._adapter_factory(venue)
        except Exception as error:
            self._defer_retired_venue_reconnect(
                venue,
                f"replacement BBO adapter creation failed: {type(error).__name__}: {error}",
            )
            return False
        self._adapters[venue] = replacement
        self._capabilities.pop(venue, None)
        self._instruments.pop(venue, None)
        return True

    async def _sync_bbo_watchers(self) -> None:
        self._require_open()
        self._cleanup_retired_bbo_tasks()
        desired = {
            venue: self._symbols_for_venue(venue)
            for venue in self._adapters
            if venue not in self._quarantined and self._symbols_for_venue(venue)
        }
        newly_retiring: list[asyncio.Task[None]] = []
        for venue, task in tuple(self._bbo_watchers.items()):
            symbols_changed = self._bbo_watcher_symbols.get(venue) != desired.get(venue)
            if task.done():
                self._bbo_watchers.pop(venue, None)
                self._bbo_watcher_symbols.pop(venue, None)
                self._consume_watcher(task)
            elif venue not in desired or symbols_changed:
                task.cancel()
                self._bbo_watchers.pop(venue, None)
                self._bbo_watcher_symbols.pop(venue, None)
                self._retiring_bbo_watchers[venue] = task
                newly_retiring.append(task)
        if newly_retiring:
            await asyncio.wait(newly_retiring, timeout=self._bbo_retirement_grace_seconds)
            await asyncio.sleep(0)
        self._cleanup_retired_bbo_tasks()
        retiring = tuple(
            task
            for task in (
                *self._retiring_bbo_watchers.values(),
                *self._retiring_bbo_transports.values(),
            )
            if not task.done()
        )
        if retiring:
            await asyncio.wait(retiring, timeout=self._bbo_retirement_grace_seconds)
            self._cleanup_retired_bbo_tasks()
        self._require_open()
        retiring_venues = {
            *self._retiring_bbo_watchers,
            *self._retiring_bbo_transports,
        }
        for venue in retiring_venues:
            if venue not in self._quarantined:
                self._quarantine(venue, "prior BBO watcher did not terminate")
        for venue, symbols in sorted(desired.items(), key=lambda item: str(item[0])):
            if (
                venue not in self._bbo_watchers
                and venue not in self._quarantined
                and venue not in retiring_venues
            ):
                self._bbo_watcher_symbols[venue] = symbols
                self._bbo_watchers[venue] = asyncio.create_task(
                    self._run_bbo_watcher(venue),
                    name=f"broad-bbo-{venue.value}",
                )

    def _retire_bbo_transport(
        self,
        venue: Venue,
        task: asyncio.Task[tuple[BboQuote, ...]],
    ) -> None:
        if task.done():
            self._consume_transport(task)
            return
        task.cancel()
        existing = self._retiring_bbo_transports.get(venue)
        if existing is not None and existing is not task and not existing.done():
            raise RuntimeError("multiple retiring BBO transports for one venue")
        self._retiring_bbo_transports[venue] = task

    async def _next_bbo_update(
        self,
        venue: Venue,
        symbols: tuple[str, ...],
    ) -> tuple[BboQuote, ...]:
        transport = asyncio.create_task(
            self._adapters[venue].watch_bbo(symbols),
            name=f"broad-bbo-transport-{venue.value}",
        )
        try:
            done, _ = await asyncio.wait((transport,), timeout=self._bbo_watch_timeout_seconds)
            if not done:
                self._retire_bbo_transport(venue, transport)
                raise TimeoutError("batch BBO stream made no progress before staleness deadline")
            return transport.result()
        except asyncio.CancelledError:
            self._retire_bbo_transport(venue, transport)
            raise

    async def _run_bbo_watcher(self, venue: Venue) -> None:
        loop = asyncio.get_running_loop()
        qualified_deadline = loop.time() + self._bbo_watch_timeout_seconds
        try:
            while not self._closed and venue not in self._quarantined:
                symbols = self._symbols_for_venue(venue)
                if not symbols:
                    return
                started_ns = time.monotonic_ns()
                quotes = await self._next_bbo_update(venue, symbols)
                if self._closed:
                    return
                if not quotes:
                    raise RuntimeError("batch BBO stream returned no updates")
                accepted = self._bbo_cache.ingest(
                    quotes,
                    now_monotonic_ns=self._monotonic_ns(),
                )
                if accepted == 0:
                    remaining = qualified_deadline - loop.time()
                    if remaining <= 0:
                        raise RuntimeError(
                            "batch BBO stream made no qualified progress before staleness deadline"
                        )
                    await asyncio.sleep(min(0.01, remaining))
                    continue
                qualified_deadline = loop.time() + self._bbo_watch_timeout_seconds
                self._reconnect_attempts.pop(venue, None)
                self._reconnect_after_ns.pop(venue, None)
                self._bbo_changed.set()
                if time.monotonic_ns() - started_ns < 1_000_000:
                    await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._closed:
                return
            self._quarantine(
                venue,
                f"BBO stream failed: {type(error).__name__}: {error}",
            )

    async def _refresh_universe_snapshot(self, *, force: bool) -> UniverseSnapshot:
        snapshot = self._universe.refresh(
            self._instruments,
            now=self._now_factory(),
            monotonic_ns=self._monotonic_ns(),
            force=force,
        )
        self._set_available_bbo_keys()
        await self._sync_bbo_watchers()
        return snapshot

    async def refresh_universe(
        self,
        timeout_seconds: int,
        *,
        force: bool = False,
        reconnected: tuple[Venue, ...] = (),
    ) -> UniverseSnapshot:
        self._require_open()
        if not self._initialised:
            await self.initialise(timeout_seconds)
        now_ns = self._monotonic_ns()
        due_reconnects = {
            venue
            for venue, retry_at_ns in self._reconnect_after_ns.items()
            if now_ns >= retry_at_ns
        }
        requested_venues = (
            set(self._adapters)
            if force
            else {venue for venue in (*reconnected, *due_reconnects) if venue in self._adapters}
        )
        observed_refresh_generations = {
            venue: self._venue_refresh_generations.get(venue, 0) for venue in requested_venues
        }
        observed_failure_generations = {
            venue: self._recycle_failure_generations.get(venue, 0) for venue in requested_venues
        }
        async with self._lifecycle_lock:
            self._require_open()
            if requested_venues and all(
                self._venue_refresh_generations.get(venue, 0) != observed_refresh_generations[venue]
                or self._recycle_failure_generations.get(venue, 0)
                != observed_failure_generations[venue]
                for venue in requested_venues
            ):
                current = self._universe.snapshot
                assert current is not None
                return current
            return await self._refresh_universe_locked(
                timeout_seconds,
                force=force,
                reconnected=reconnected,
            )

    async def _refresh_universe_locked(
        self,
        timeout_seconds: int,
        *,
        force: bool,
        reconnected: tuple[Venue, ...],
    ) -> UniverseSnapshot:
        now_ns = self._monotonic_ns()
        due = self._universe.refresh_due(now_ns)
        due_reconnects = {
            venue
            for venue, retry_at_ns in self._reconnect_after_ns.items()
            if now_ns >= retry_at_ns
        }
        requested_reconnects = {
            venue for venue in (*reconnected, *due_reconnects) if venue in self._adapters
        }
        explicit_reconnects = set(reconnected)
        retired_targets = tuple(
            sorted(
                {
                    venue
                    for venue in requested_reconnects
                    if venue in self._retiring_bbo_watchers
                    or venue in self._retiring_bbo_transports
                    or venue in self._retiring_adapter_closers
                },
                key=str,
            )
        )
        recycled_targets: set[Venue] = set()
        if retired_targets:
            recycle_results = await asyncio.gather(
                *(
                    self._recycle_retired_venue_adapter(
                        venue,
                        timeout_seconds,
                        ignore_backoff=venue in explicit_reconnects,
                    )
                    for venue in retired_targets
                )
            )
            recycled_targets = {
                venue
                for venue, recycled in zip(retired_targets, recycle_results, strict=True)
                if recycled
            }
        reconnect_targets = tuple(
            sorted(
                {venue for venue in requested_reconnects if venue not in retired_targets}
                | {venue for venue in recycled_targets},
                key=str,
            )
        )
        if not force and not due and not reconnect_targets:
            self._require_open()
            current = self._universe.snapshot
            assert current is not None
            return current
        targets = (
            tuple(
                venue
                for venue in self._adapters
                if venue not in recycled_targets
                if venue not in self._retiring_bbo_watchers
                and venue not in self._retiring_bbo_transports
                and venue not in self._retiring_adapter_closers
            )
            if force or due
            else tuple(venue for venue in reconnect_targets if venue not in recycled_targets)
        )
        await asyncio.gather(
            *(self._initialise_venue_with_timeout(venue, timeout_seconds) for venue in targets)
        )
        self._require_open()
        snapshot = await self._refresh_universe_snapshot(force=True)
        self._require_open()
        return snapshot

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
        task = self._begin_public_scan()
        try:
            return await self._scan_broad_bbo(timeout_seconds)
        finally:
            self._finish_public_scan(task)

    async def _scan_broad_bbo(self, timeout_seconds: int) -> BroadBboResult:
        self._require_open()
        if not self._initialised:
            await self.initialise(timeout_seconds)
        universe = await self.refresh_universe(timeout_seconds)
        async with self._lifecycle_lock:
            self._require_open()
            await self._sync_bbo_watchers()
            self._require_open()
        started_ns = time.perf_counter_ns()
        await self._wait_for_bbo_coverage(timeout_seconds)
        self._require_open()
        now_ns = self._monotonic_ns()
        available_routes = tuple(
            route
            for route in universe.routes
            if route.long_instrument.venue not in self._quarantined
            and route.short_instrument.venue not in self._quarantined
        )
        fresh = self._bbo_cache.fresh(now_monotonic_ns=now_ns)
        prefilter = rank_bbo_prefilter(
            available_routes,
            fresh,
            stale_keys=self._bbo_cache.stale_keys(now_monotonic_ns=now_ns),
        )
        compute_latency_ms = Decimal(time.perf_counter_ns() - started_ns) / Decimal(1_000_000)
        ranked_at_ns = self._monotonic_ns()
        oldest_quote_latency_ms = max(
            (
                Decimal(ranked_at_ns - quote.received_monotonic_ns) / Decimal(1_000_000)
                for quote in fresh
            ),
            default=Decimal(0),
        )
        latency_ms = max(compute_latency_ms, oldest_quote_latency_ms)
        return BroadBboResult(
            universe.generation,
            len(universe.common),
            len(universe.routes),
            len(available_routes),
            fresh,
            prefilter,
            self._bbo_cache.stats_at(now_monotonic_ns=now_ns),
            latency_ms,
            tuple(self._quarantined[venue] for venue in sorted(self._quarantined, key=str)),
        )

    async def scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
    ) -> ScanResult:
        task = self._begin_public_scan()
        try:
            return await self._scan_once(base, requested_base_quantity, timeout_seconds)
        finally:
            self._finish_public_scan(task)

    async def _scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
    ) -> ScanResult:
        self._require_open()
        if not self._initialised:
            await self.initialise(timeout_seconds)
        broad = await self._scan_broad_bbo(timeout_seconds)
        self._require_open()
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
        self._require_open()
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
        self._require_open()
        warmed_instruments = tuple(
            instrument
            for instrument, book in warmup_samples
            if book is not None and instrument.venue not in self._quarantined
        )
        book_samples = await asyncio.gather(
            *(self._sample_book(instrument, timeout_seconds) for instrument in warmed_instruments)
        )
        self._require_open()
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
            self._require_open()
        self._require_open()
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
            if not self._closed:
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
            if not self._closed:
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
        self._bbo_changed.set()
        loop = asyncio.get_running_loop()
        shutdown_deadline = loop.time() + 1
        public_scans_blocked = False
        if self._active_public_scans:
            try:
                await asyncio.wait_for(
                    self._public_scans_idle.wait(),
                    timeout=max(0, shutdown_deadline - loop.time()),
                )
            except TimeoutError:
                public_scans_blocked = True
                for task in tuple(self._active_public_scans):
                    task.cancel()
                await asyncio.sleep(0)
        lifecycle_lock_acquired = False
        lifecycle_lock_blocked = False
        try:
            await asyncio.wait_for(
                self._lifecycle_lock.acquire(),
                timeout=max(0, shutdown_deadline - loop.time()),
            )
            lifecycle_lock_acquired = True
        except TimeoutError:
            lifecycle_lock_blocked = True
        recycle_locks = tuple(
            (venue, self._adapter_recycle_locks.setdefault(venue, asyncio.Lock()))
            for venue in sorted(self._adapters, key=str)
        )
        acquired_locks: list[asyncio.Lock] = []
        blocked_recycle_venues: set[Venue] = set()
        for venue, lock in recycle_locks:
            remaining = shutdown_deadline - loop.time()
            if remaining <= 0:
                blocked_recycle_venues.add(venue)
                continue
            try:
                await asyncio.wait_for(lock.acquire(), timeout=remaining)
                acquired_locks.append(lock)
            except TimeoutError:
                blocked_recycle_venues.add(venue)
        try:
            await self._close_with_recycles_blocked(
                shutdown_deadline,
                blocked_recycle_venues,
                lifecycle_lock_blocked=lifecycle_lock_blocked,
                public_scans_blocked=public_scans_blocked,
            )
        finally:
            for lock in reversed(acquired_locks):
                lock.release()
            if lifecycle_lock_acquired:
                self._lifecycle_lock.release()

    async def _close_with_recycles_blocked(
        self,
        shutdown_deadline: float,
        blocked_recycle_venues: set[Venue],
        *,
        lifecycle_lock_blocked: bool,
        public_scans_blocked: bool,
    ) -> None:
        close_failures: list[str] = []
        timed_out_closer_venues: set[Venue] = set()
        self._cleanup_retired_bbo_tasks()
        for venue, closer in tuple(self._retiring_adapter_closers.items()):
            if closer.done():
                self._retiring_adapter_closers.pop(venue, None)
                try:
                    closer.result()
                except (asyncio.CancelledError, Exception) as error:
                    close_failures.append(f"{venue.value}: {type(error).__name__}: {error}")
        for task in self._bbo_watchers.values():
            task.cancel()
        for venue, adapter in self._adapters.items():
            if venue not in self._retiring_adapter_closers:
                self._retiring_adapter_closers[venue] = asyncio.create_task(
                    adapter.close(),
                    name=f"close-{venue.value}",
                )
        adapter_closers = tuple(self._retiring_adapter_closers.items())
        if adapter_closers:
            done_closers, pending_closers = await asyncio.wait(
                (task for _, task in adapter_closers),
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
            for venue, task in adapter_closers:
                if task in done_closers:
                    self._retiring_adapter_closers.pop(venue, None)
                    try:
                        task.result()
                    except (asyncio.CancelledError, Exception) as error:
                        close_failures.append(f"{venue.value}: {type(error).__name__}: {error}")
                elif task in pending_closers:
                    timed_out_closer_venues.add(venue)
                    task.cancel()
                    task.add_done_callback(self._consume_watcher)
        await asyncio.sleep(0)
        for venue, task in self._bbo_watchers.items():
            self._retiring_bbo_watchers.setdefault(venue, task)
        self._bbo_watchers.clear()
        self._bbo_watcher_symbols.clear()
        retiring_watchers = tuple(set(self._retiring_bbo_watchers.values()))
        retiring_transports = tuple(set(self._retiring_bbo_transports.values()))
        for watcher in retiring_watchers:
            watcher.cancel()
        for transport in retiring_transports:
            transport.cancel()
        if retiring_watchers:
            await asyncio.wait(
                retiring_watchers,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_transports:
            await asyncio.wait(
                retiring_transports,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_watchers or retiring_transports:
            for watcher in retiring_watchers:
                if not watcher.done():
                    watcher.add_done_callback(self._consume_watcher)
            for transport in retiring_transports:
                if not transport.done():
                    transport.add_done_callback(self._consume_transport)
        self._cleanup_retired_bbo_tasks()
        pending_venues = tuple(
            sorted(
                {venue for venue, task in self._retiring_bbo_watchers.items() if not task.done()}
                | {
                    venue
                    for venue, task in self._retiring_bbo_transports.items()
                    if not task.done()
                }
                | {
                    venue
                    for venue, task in self._retiring_adapter_closers.items()
                    if not task.done()
                }
                | timed_out_closer_venues
                | blocked_recycle_venues,
                key=str,
            )
        )
        shutdown_failures: list[str] = []
        if public_scans_blocked:
            shutdown_failures.append("shutdown deadline exceeded for: public scans")
        if lifecycle_lock_blocked:
            shutdown_failures.append("shutdown deadline exceeded for: public lifecycle")
        if pending_venues:
            shutdown_failures.append(
                f"shutdown deadline exceeded for: "
                f"{', '.join(venue.value for venue in pending_venues)}"
            )
        if close_failures:
            shutdown_failures.append(f"adapter shutdown failed for: {'; '.join(close_failures)}")
        if shutdown_failures:
            raise RuntimeError(f"BBO {'; '.join(shutdown_failures)}")

    @staticmethod
    def _consume_watcher(task: asyncio.Task[None]) -> None:
        with suppress(asyncio.CancelledError, Exception):
            task.result()

    @staticmethod
    def _consume_transport(task: asyncio.Task[tuple[BboQuote, ...]]) -> None:
        with suppress(asyncio.CancelledError, Exception):
            task.result()
