from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
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
from interexchange_perp_grid.candidate_l2 import (
    BookKey,
    CandidateL2BookPlan,
    CandidateL2BookState,
    CandidateL2Plan,
    CandidateL2Result,
    CandidateL2RouteObservation,
    CandidateL2Stats,
    L2WorkPriority,
    RouteStableKey,
    build_candidate_l2_plan,
    evaluate_candidate_l2_routes,
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
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.route_calibration import (
    RouteCalibrationAssessment,
    RouteCalibrationObservation,
    RouteCalibrationSamplingPolicy,
    build_route_calibration_observations,
)
from interexchange_perp_grid.routes import (
    DirectedRouteQuote,
    directed_pairs,
    evaluate_directed_route,
)
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.venue_capabilities import (
    VenueCapabilityMatrix,
    build_venue_capability_matrix,
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
    candidate_l2: CandidateL2Result | None = None
    route_calibration: tuple[RouteCalibrationAssessment, ...] = ()
    venue_capability_matrix: VenueCapabilityMatrix | None = None


@dataclass(frozen=True, slots=True)
class AggressiveRouteMarketSnapshot:
    route: DirectedRouteKey
    long_instrument: Instrument
    short_instrument: Instrument
    long_book: OrderBookSnapshot | None
    short_book: OrderBookSnapshot | None
    long_quality: DataQualityAssessment
    short_quality: DataQualityAssessment
    long_funding: FundingSnapshot | None
    short_funding: FundingSnapshot | None
    observed_monotonic_ns: int
    unavailable_venues: frozenset[Venue]
    execution_authorized: bool = field(default=False, init=False)


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
    venue_capability_matrix: VenueCapabilityMatrix


@dataclass(frozen=True, slots=True)
class PublicWorkload:
    active_l2_tasks: int
    candidate_l2_demand: int
    broad_bbo_demand: int


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
        public_venues: tuple[Venue, ...] | None = None,
        now_factory: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        reconnect_jitter: ReconnectJitter = _default_reconnect_jitter,
    ) -> None:
        self.settings = settings
        configured_public_venues = tuple(Venue(value) for value in settings.venues.public_runtime)
        selected_public_venues = (
            tuple(Venue(value) for value in settings.venues.wave1_public)
            if public_venues is None
            else public_venues
        )
        if len(selected_public_venues) != len(set(selected_public_venues)):
            raise ValueError("public runtime venues must be unique")
        if not set(selected_public_venues) <= set(configured_public_venues):
            raise ValueError("public runtime venues must be configured in a venue wave")
        self._wave1_public_venues = frozenset(
            Venue(value) for value in settings.venues.wave1_public
        )
        if not self._wave1_public_venues <= set(selected_public_venues):
            raise ValueError("public runtime must preserve every Wave 1 venue")
        self._configured_public_venues = selected_public_venues
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
                maximum_common_instruments=settings.universe.max_broad_bbo_instruments,
                preferred_bases=(settings.shadow.base,),
            ),
            refresh_seconds=settings.universe.instrument_refresh_seconds,
        )
        self._bbo_cache = LatestBboCache(
            maximum_age_ms=settings.market_data.max_bbo_age_ms,
            maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        )
        self._bbo_watchers: dict[Venue, asyncio.Task[None]] = {}
        self._bbo_watcher_symbols: dict[Venue, tuple[str, ...]] = {}
        self._bbo_subscription_started: set[Venue] = set()
        self._bbo_qualified_venues: set[Venue] = set()
        self._retiring_bbo_watchers: dict[Venue, asyncio.Task[None]] = {}
        self._retiring_bbo_transports: dict[Venue, asyncio.Task[tuple[BboQuote, ...]]] = {}
        self._retiring_bbo_unsubscribes: dict[Venue, asyncio.Task[None]] = {}
        self._bbo_unsubscribe_failures: set[Venue] = set()
        self._retiring_adapter_closers: dict[Venue, asyncio.Task[None]] = {}
        self._venue_initialise_attempts: dict[Venue, object] = {}
        self._retiring_venue_initialisers: dict[asyncio.Task[None], Venue] = {}
        self._adapter_recycle_locks: dict[Venue, asyncio.Lock] = {}
        self._lifecycle_lock = asyncio.Lock()
        self._active_public_scans: set[asyncio.Task[object]] = set()
        self._public_scans_idle = asyncio.Event()
        self._public_scans_idle.set()
        self._bbo_changed = asyncio.Event()
        self._candidate_l2_registry = BookRegistry()
        self._candidate_l2_scan_lock = asyncio.Lock()
        self._selected_scan_lock = asyncio.Lock()
        self._l2_transport_locks: dict[BookKey, asyncio.Lock] = {}
        self._l2_transport_lock_users: dict[BookKey, int] = {}
        self._candidate_l2_plan = CandidateL2Plan((), (), ())
        self._pending_candidate_l2_plan = self._candidate_l2_plan
        self._candidate_l2_plan_version = 0
        self._candidate_l2_applied_version = 0
        self._candidate_l2_plan_generation = 0
        self._candidate_l2_plan_changed = asyncio.Event()
        self._candidate_l2_plan_applied = asyncio.Event()
        self._candidate_l2_changed = asyncio.Event()
        self._candidate_l2_decision_input = asyncio.Event()
        self._candidate_l2_decision_ready = asyncio.Event()
        self._candidate_l2_debouncer: asyncio.Task[None] | None = None
        self._candidate_l2_decision_worker: asyncio.Task[None] | None = None
        self._candidate_l2_watchers: dict[BookKey, asyncio.Task[None]] = {}
        self._candidate_l2_watcher_instruments: dict[BookKey, Instrument] = {}
        self._candidate_l2_transports: dict[BookKey, asyncio.Task[OrderBookSnapshot]] = {}
        self._candidate_l2_subscription_started: set[BookKey] = set()
        self._retiring_candidate_l2_watchers: dict[BookKey, asyncio.Task[None]] = {}
        self._retiring_candidate_l2_transports: dict[BookKey, asyncio.Task[OrderBookSnapshot]] = {}
        self._retiring_candidate_l2_unsubscribes: dict[BookKey, asyncio.Task[None]] = {}
        self._candidate_l2_unsubscribe_failures: set[Venue] = set()
        self._candidate_l2_states: dict[BookKey, CandidateL2BookState] = {}
        self._candidate_l2_peak_books = 0
        self._candidate_l2_peak_watchers = 0
        self._candidate_l2_accepted_updates = 0
        self._candidate_l2_rejected_updates = 0
        self._candidate_l2_coalesced_plans = 0
        self._candidate_l2_state_version = 0
        self._candidate_l2_decision_version = 0
        self._candidate_l2_decision_updates = 0
        self._candidate_l2_observations: tuple[CandidateL2RouteObservation, ...] = ()
        self._candidate_l2_decision_latency_ms = Decimal(0)
        self._candidate_l2_latency_samples: deque[Decimal] = deque(maxlen=2048)
        self._route_calibration_funding: dict[BookKey, FundingSnapshot] = {}
        self._route_calibration_funding_observed_ns: dict[BookKey, int] = {}
        self._route_calibration_funding_generation: dict[BookKey, int] = {}
        self._route_calibration_funding_tasks: dict[
            BookKey, asyncio.Task[tuple[FundingSnapshot, int]]
        ] = {}
        self._route_calibration_funding_task_generation: dict[BookKey, int] = {}
        self._route_calibration_funding_task_started_ns: dict[BookKey, int] = {}
        self._retiring_route_calibration_funding_tasks: dict[
            BookKey, asyncio.Task[tuple[FundingSnapshot, int]]
        ] = {}
        self._retiring_route_calibration_funding_started_ns: dict[BookKey, int] = {}
        self._route_calibration_funding_transports: dict[
            BookKey, asyncio.Task[FundingSnapshot]
        ] = {}
        self._retiring_route_calibration_funding_transports: dict[
            BookKey, asyncio.Task[FundingSnapshot]
        ] = {}
        self._retiring_route_calibration_funding_transport_started_ns: dict[BookKey, int] = {}
        self._retiring_route_calibration_funding_watchdogs: dict[BookKey, asyncio.Task[None]] = {}
        self._route_calibration_funding_retry_after_ns: dict[BookKey, int] = {}
        self._route_calibration_funding_cursor = 0
        self._route_calibration_previous_routes: frozenset[DirectedRouteKey] = frozenset()
        self._candidate_l2_candidate_demand = settings.universe.max_dynamic_l2_candidates * 2
        self._last_candidate_l2_prefilter: tuple[BboPrefilterObservation, ...] = ()
        self._broad_bbo_admitted = True
        self._bbo_watch_timeout_seconds = settings.market_data.max_bbo_age_ms / 1000
        # A first public WebSocket subscription includes DNS/TLS/authless channel
        # setup and can legitimately take longer than the quote freshness bound.
        # Keep that startup allowance bounded; after the first accepted quote the
        # strict market-data age is again the transport progress deadline.
        self._bbo_initial_progress_timeout_seconds = max(
            0.25,
            min(10.0, self._bbo_watch_timeout_seconds * 6),
        )
        self._candidate_l2_watch_timeout_seconds = settings.market_data.max_l2_age_ms / 1000
        self._candidate_l2_unsubscribe_timeout_seconds = max(
            1.0,
            self._candidate_l2_watch_timeout_seconds,
        )
        self._route_calibration_funding_timeout_ns = 1_000_000_000
        self._bbo_unsubscribe_timeout_seconds = self._candidate_l2_unsubscribe_timeout_seconds
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
                for venue in self._configured_public_venues:
                    try:
                        staged_adapters[venue] = self._adapter_factory(venue)
                    except Exception as error:
                        if venue in self._wave1_public_venues:
                            await self._close_unpublished_adapters(staged_adapters)
                            raise
                        self._quarantine(
                            venue,
                            f"adapter creation failed: {type(error).__name__}: {error}",
                        )
                        self._record_venue_refresh(venue)
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

    async def _initialise_venue_with_timeout(
        self,
        venue: Venue,
        timeout_seconds: float,
    ) -> None:
        self._cleanup_retired_venue_initialisers()
        if venue in self._venue_initialise_attempts or self._venue_has_retiring_initialiser(venue):
            if not self._closed:
                self._quarantine(venue, "previous capability probe remains nonterminal")
            return
        attempt = object()
        self._venue_initialise_attempts[venue] = attempt
        task = asyncio.create_task(
            self._initialise_venue(venue, attempt),
            name=f"public-initialise-{venue.value}",
        )
        try:
            done, _pending = await asyncio.wait((task,), timeout=timeout_seconds)
            if task in done:
                task.result()
                return
            self._retire_venue_initialiser(venue, attempt, task)
            if self._closed:
                return
            self._quarantine(venue, f"capability probe timed out after {timeout_seconds}s")
            self._record_venue_refresh(venue)
        except asyncio.CancelledError:
            self._retire_venue_initialiser(venue, attempt, task)
            raise
        finally:
            if self._venue_initialise_attempts.get(venue) is attempt:
                self._venue_initialise_attempts.pop(venue, None)

    def _retire_venue_initialiser(
        self,
        venue: Venue,
        attempt: object,
        task: asyncio.Task[None],
    ) -> None:
        if self._venue_initialise_attempts.get(venue) is attempt:
            self._venue_initialise_attempts.pop(venue, None)
        if task.done():
            self._consume_watcher(task)
            return
        task.cancel()
        self._retiring_venue_initialisers[task] = venue
        task.add_done_callback(self._complete_retired_venue_initialiser)

    def _complete_retired_venue_initialiser(self, task: asyncio.Task[None]) -> None:
        self._retiring_venue_initialisers.pop(task, None)
        self._consume_watcher(task)

    def _cleanup_retired_venue_initialisers(self) -> None:
        for task in tuple(self._retiring_venue_initialisers):
            if not task.done():
                continue
            self._retiring_venue_initialisers.pop(task, None)
            self._consume_watcher(task)

    def _venue_has_retiring_initialiser(self, venue: Venue) -> bool:
        return any(
            task_venue == venue and not task.done()
            for task, task_venue in self._retiring_venue_initialisers.items()
        )

    def _venue_initialise_attempt_is_current(self, venue: Venue, attempt: object) -> bool:
        return not self._closed and self._venue_initialise_attempts.get(venue) is attempt

    async def _initialise_venue(self, venue: Venue, attempt: object) -> None:
        adapter = self._adapters[venue]
        report: CapabilityReport | None = None
        instruments: tuple[Instrument, ...] = ()
        failure_reason: str | None = None
        try:
            report = await adapter.probe_public_capabilities()
            if not self._venue_initialise_attempt_is_current(venue, attempt):
                return
            if not report.public_ready:
                failure_reason = f"missing capabilities: {', '.join(report.missing)}"
            elif report.clock_skew_ms is None:
                failure_reason = "clock skew is unknown"
            elif abs(report.clock_skew_ms) > self.settings.market_data.max_clock_skew_ms:
                failure_reason = f"clock skew {report.clock_skew_ms}ms exceeds policy"
            else:
                instruments = await adapter.discover_instruments()
                if not self._venue_initialise_attempt_is_current(venue, attempt):
                    return
                if not instruments:
                    failure_reason = "no qualified linear USDT perpetual instruments"
        except Exception as error:
            if not self._venue_initialise_attempt_is_current(venue, attempt):
                return
            failure_reason = f"capability probe failed: {type(error).__name__}: {error}"

        if not self._venue_initialise_attempt_is_current(venue, attempt):
            return
        if report is not None:
            self._capabilities[venue] = report
        if failure_reason is None:
            assert report is not None
            if venue in self._quarantined:
                self._reset_candidate_l2_venue(venue)
            self._instruments[venue] = instruments
            self._quarantined.pop(venue, None)
            self._record_venue_refresh(venue)
            return
        self._quarantine(venue, failure_reason)
        self._record_venue_refresh(venue)

    def _record_venue_refresh(self, venue: Venue) -> None:
        self._venue_refresh_generations[venue] = self._venue_refresh_generations.get(venue, 0) + 1

    def _quarantine(self, venue: Venue, reason: str) -> None:
        self._mark_venue_unavailable(venue, reason)
        attempt = self._reconnect_attempts.get(venue, 0) + 1
        self._reconnect_attempts[venue] = attempt
        delay_seconds = reconnect_delay_seconds(venue, attempt, self._reconnect_jitter)
        self._reconnect_after_ns[venue] = self._monotonic_ns() + int(delay_seconds * 1_000_000_000)

    def _mark_venue_unavailable(self, venue: Venue, reason: str) -> None:
        self._quarantined[venue] = QuarantineRecord(venue, reason, self._now_factory())
        self._instruments.pop(venue, None)
        candidate_invalidated = False
        for book_plan in self._candidate_l2_plan.books:
            if book_plan.instrument.venue == venue:
                self._candidate_l2_states[book_plan.key] = CandidateL2BookState(
                    None,
                    DataQualityAssessment(False, ReasonCode.VENUE_OUTAGE, 0),
                    book_plan.priority,
                )
                candidate_invalidated = True
        if candidate_invalidated:
            self._mark_candidate_l2_state_changed()
        self._set_available_bbo_keys()
        self._bbo_changed.set()

    def _set_available_bbo_keys(self) -> None:
        snapshot = self._universe.snapshot
        if snapshot is None:
            return
        self._bbo_cache.set_known_keys(
            frozenset(key for key in snapshot.known_bbo_keys if key[0] not in self._quarantined)
        )

    def _reset_candidate_l2_venue(self, venue: Venue) -> None:
        keys = frozenset(
            key
            for key in (
                *self._candidate_l2_states,
                *(
                    (book.instrument.venue, book.instrument.symbol)
                    for book in self._candidate_l2_plan.books
                ),
            )
            if key[0] == venue
        )
        self._candidate_l2_registry.discard_keys(keys)
        for key in keys:
            self._candidate_l2_states.pop(key, None)

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
        for venue, unsubscribe in tuple(self._retiring_bbo_unsubscribes.items()):
            if not unsubscribe.done():
                continue
            self._retiring_bbo_unsubscribes.pop(venue, None)
            try:
                unsubscribe.result()
            except (asyncio.CancelledError, Exception) as error:
                self._bbo_unsubscribe_failures.add(venue)
                if not self._closed and venue not in self._quarantined:
                    self._quarantine(
                        venue,
                        f"broad BBO unsubscribe failed: {type(error).__name__}: {error}",
                    )
            else:
                self._bbo_subscription_started.discard(venue)
                self._bbo_qualified_venues.discard(venue)

    def _defer_retired_venue_reconnect(self, venue: Venue, reason: str) -> None:
        attempt = max(1, self._reconnect_attempts.get(venue, 1))
        delay_seconds = reconnect_delay_seconds(venue, attempt, self._reconnect_jitter)
        self._reconnect_after_ns[venue] = self._monotonic_ns() + int(delay_seconds * 1_000_000_000)
        self._recycle_failure_generations[venue] = (
            self._recycle_failure_generations.get(venue, 0) + 1
        )
        self._mark_venue_unavailable(venue, reason)

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
                and venue not in self._retiring_bbo_unsubscribes
                and venue not in self._bbo_unsubscribe_failures
                and venue not in self._retiring_adapter_closers
                and not self._venue_has_retiring_initialiser(venue)
                and not self._venue_has_retiring_candidate_l2(venue)
                and not self._venue_has_retiring_route_calibration_funding(venue)
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
        for key, l2_watcher in tuple(self._candidate_l2_watchers.items()):
            if key[0] != venue:
                continue
            self._candidate_l2_watchers.pop(key, None)
            self._candidate_l2_watcher_instruments.pop(key, None)
            if l2_watcher.done():
                self._consume_watcher(l2_watcher)
            else:
                l2_watcher.cancel()
                self._retiring_candidate_l2_watchers[key] = l2_watcher
        for key, funding_worker in tuple(self._route_calibration_funding_tasks.items()):
            if key[0] == venue:
                self._retire_route_calibration_funding_task(key, funding_worker)
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
                self._retiring_bbo_unsubscribes.get(venue),
                *self._retiring_candidate_l2_tasks_for_venue(venue),
                *(
                    task
                    for key, task in self._retiring_route_calibration_funding_tasks.items()
                    if key[0] == venue
                ),
                *(
                    task
                    for key, task in self._retiring_route_calibration_funding_transports.items()
                    if key[0] == venue
                ),
                *(
                    task
                    for task, task_venue in self._retiring_venue_initialisers.items()
                    if task_venue == venue
                ),
            )
            if task is not None and not task.done()
        )
        if retiring:
            await asyncio.wait(retiring, timeout=self._bbo_retirement_grace_seconds)
        self._cleanup_retired_bbo_tasks()
        self._cleanup_retired_candidate_l2_tasks()
        self._cleanup_retired_route_calibration_funding_tasks()
        self._cleanup_retired_route_calibration_funding_tasks()
        self._bbo_subscription_started.discard(venue)
        self._bbo_qualified_venues.discard(venue)
        self._candidate_l2_subscription_started = {
            key for key in self._candidate_l2_subscription_started if key[0] != venue
        }
        self._bbo_unsubscribe_failures.discard(venue)
        self._candidate_l2_unsubscribe_failures.discard(venue)
        if (
            venue in self._retiring_bbo_watchers
            or venue in self._retiring_bbo_transports
            or venue in self._retiring_bbo_unsubscribes
            or self._venue_has_retiring_initialiser(venue)
            or self._venue_has_retiring_candidate_l2(venue)
            or self._venue_has_retiring_route_calibration_funding(venue)
        ):
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
        self._mark_venue_unavailable(venue, "replacement capability validation pending")
        self._record_venue_refresh(venue)
        self._adapters[venue] = replacement
        self._capabilities.pop(venue, None)
        self._instruments.pop(venue, None)
        return True

    async def _sync_bbo_watchers(self) -> None:
        self._require_open()
        self._cleanup_retired_bbo_tasks()
        desired = (
            {
                venue: self._symbols_for_venue(venue)
                for venue in self._adapters
                if venue not in self._quarantined and self._symbols_for_venue(venue)
            }
            if self._broad_bbo_admitted
            else {}
        )
        newly_retiring: list[asyncio.Task[None]] = []
        new_unsubscribes: list[asyncio.Task[None]] = []
        for venue, task in tuple(self._bbo_watchers.items()):
            symbols_changed = self._bbo_watcher_symbols.get(venue) != desired.get(venue)
            if task.done():
                self._bbo_watchers.pop(venue, None)
                self._bbo_watcher_symbols.pop(venue, None)
                self._bbo_qualified_venues.discard(venue)
                self._consume_watcher(task)
            elif venue not in desired or symbols_changed:
                previous_symbols = self._bbo_watcher_symbols.get(venue, ())
                task.cancel()
                self._bbo_watchers.pop(venue, None)
                self._bbo_watcher_symbols.pop(venue, None)
                self._retiring_bbo_watchers[venue] = task
                newly_retiring.append(task)
                if (
                    previous_symbols
                    and venue in self._bbo_subscription_started
                    and venue not in self._retiring_bbo_unsubscribes
                ):
                    unsubscribe = asyncio.create_task(
                        self._adapters[venue].unwatch_bbo(previous_symbols),
                        name=f"broad-bbo-unsubscribe-{venue.value}",
                    )
                    self._retiring_bbo_unsubscribes[venue] = unsubscribe
                    new_unsubscribes.append(unsubscribe)
        if newly_retiring:
            await asyncio.wait(newly_retiring, timeout=self._bbo_retirement_grace_seconds)
            await asyncio.sleep(0)
        if new_unsubscribes:
            await asyncio.wait(
                new_unsubscribes,
                timeout=self._bbo_unsubscribe_timeout_seconds,
            )
            await asyncio.sleep(0)
        self._cleanup_retired_bbo_tasks()
        retiring = tuple(
            task
            for task in (
                *self._retiring_bbo_watchers.values(),
                *self._retiring_bbo_transports.values(),
                *self._retiring_bbo_unsubscribes.values(),
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
            *self._retiring_bbo_unsubscribes,
            *self._bbo_unsubscribe_failures,
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
        if not self._closed and venue not in self._bbo_subscription_started:
            # The subscribe call did not return a qualified update, so matching
            # network ownership is unknowable. Recycle the adapter instead of
            # issuing a broad unsubscribe for topics that may never have acked.
            self._bbo_unsubscribe_failures.add(venue)
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
            self._watch_bbo_subscription(venue, symbols),
            name=f"broad-bbo-transport-{venue.value}",
        )
        try:
            progress_timeout = (
                self._bbo_watch_timeout_seconds
                if venue in self._bbo_qualified_venues
                else self._bbo_initial_progress_timeout_seconds
            )
            done, _ = await asyncio.wait((transport,), timeout=progress_timeout)
            if not done:
                self._retire_bbo_transport(venue, transport)
                raise TimeoutError("batch BBO stream made no progress before staleness deadline")
            return transport.result()
        except asyncio.CancelledError:
            self._retire_bbo_transport(venue, transport)
            raise

    async def _watch_bbo_subscription(
        self,
        venue: Venue,
        symbols: tuple[str, ...],
    ) -> tuple[BboQuote, ...]:
        quotes = await self._adapters[venue].watch_bbo(symbols)
        self._bbo_subscription_started.add(venue)
        return quotes

    async def _run_bbo_watcher(self, venue: Venue) -> None:
        loop = asyncio.get_running_loop()
        qualified_deadline = loop.time() + self._bbo_initial_progress_timeout_seconds
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
                    remaining = qualified_deadline - loop.time()
                    if remaining <= 0:
                        raise RuntimeError(
                            "batch BBO stream made no qualified progress before staleness deadline"
                        )
                    await asyncio.sleep(min(0.01, remaining))
                    continue
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
                self._bbo_qualified_venues.add(venue)
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
            set(self._configured_public_venues)
            if force
            else {
                venue
                for venue in (*reconnected, *due_reconnects)
                if venue in self._configured_public_venues
            }
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
        refresh_scope = (
            set(self._configured_public_venues)
            if force or self._broad_bbo_admitted
            else set(self._wave1_public_venues) | set(reconnected)
        )
        due_reconnects = {
            venue
            for venue, retry_at_ns in self._reconnect_after_ns.items()
            if now_ns >= retry_at_ns and venue in refresh_scope
        }
        missing_targets = {
            venue
            for venue in refresh_scope
            if venue not in self._adapters
            and (force or due or venue in reconnected or venue in due_reconnects)
        }
        restored_targets: set[Venue] = set()
        for venue in sorted(missing_targets, key=str):
            try:
                replacement = self._adapter_factory(venue)
            except Exception as error:
                self._quarantine(
                    venue,
                    f"adapter creation failed: {type(error).__name__}: {error}",
                )
                self._record_venue_refresh(venue)
                continue
            self._mark_venue_unavailable(venue, "capability validation pending")
            self._record_venue_refresh(venue)
            self._adapters[venue] = replacement
            restored_targets.add(venue)
        requested_reconnects = {
            venue for venue in (*reconnected, *due_reconnects) if venue in self._adapters
        } | restored_targets
        explicit_reconnects = set(reconnected)
        retired_targets = tuple(
            sorted(
                {
                    venue
                    for venue in requested_reconnects
                    if venue in self._retiring_bbo_watchers
                    or venue in self._retiring_bbo_transports
                    or venue in self._retiring_bbo_unsubscribes
                    or venue in self._bbo_unsubscribe_failures
                    or venue in self._retiring_adapter_closers
                    or self._venue_has_retiring_initialiser(venue)
                    or self._venue_has_retiring_candidate_l2(venue)
                    or self._venue_has_retiring_route_calibration_funding(venue)
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
                if venue in refresh_scope
                if venue not in recycled_targets
                if venue not in self._retiring_bbo_watchers
                and venue not in self._retiring_bbo_transports
                and venue not in self._retiring_bbo_unsubscribes
                and venue not in self._bbo_unsubscribe_failures
                and venue not in self._retiring_adapter_closers
                and not self._venue_has_retiring_initialiser(venue)
                and not self._venue_has_retiring_candidate_l2(venue)
                and not self._venue_has_retiring_route_calibration_funding(venue)
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

    def public_workload(self) -> PublicWorkload:
        snapshot = self._universe.snapshot
        broad_demand = (
            len({venue for venue, _ in snapshot.known_bbo_keys})
            if snapshot is not None
            else len(self._configured_public_venues)
        )
        active_l2 = sum(
            1
            for book in self._candidate_l2_plan.books
            if book.priority == L2WorkPriority.ACTIVE_ROUTE
        )
        return PublicWorkload(
            active_l2,
            self._candidate_l2_candidate_demand,
            broad_demand,
        )

    def venue_capability_matrix(self) -> VenueCapabilityMatrix:
        return build_venue_capability_matrix(
            self.settings,
            public_reports=self._capabilities,
            private_reports={},
            quarantined_venues=frozenset(self._quarantined),
            now=self._now_factory(),
            maximum_report_age_seconds=self.settings.universe.instrument_refresh_seconds,
            require_all_profiles=False,
            public_runtime_enabled=frozenset(self._configured_public_venues),
        )

    async def set_broad_bbo_admitted(self, admitted: bool) -> None:
        async with self._lifecycle_lock:
            self._require_open()
            if self._broad_bbo_admitted == admitted:
                return
            self._broad_bbo_admitted = admitted
            await self._sync_bbo_watchers()
            self._require_open()

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
        if self._broad_bbo_admitted:
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
            self.venue_capability_matrix(),
        )

    async def scan_candidate_l2(
        self,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey] = frozenset(),
        candidates_admitted: bool = True,
        prefilter: tuple[BboPrefilterObservation, ...] | None = None,
        preserve_existing_candidates: bool = False,
    ) -> CandidateL2Result:
        task = self._begin_public_scan()
        try:
            async with self._candidate_l2_scan_lock:
                self._require_open()
                return await self._run_candidate_l2_request(
                    timeout_seconds,
                    active_route_keys=active_route_keys,
                    candidates_admitted=candidates_admitted,
                    prefilter=prefilter,
                    preserve_existing_candidates=preserve_existing_candidates,
                )
        finally:
            self._finish_public_scan(task)

    async def scan_route_calibration_observations(
        self,
        timeout_seconds: int,
        *,
        epoch_id: str | None = None,
    ) -> tuple[RouteCalibrationObservation, ...]:
        """Sample every applied active/candidate route without authorizing execution."""
        task = self._begin_public_scan()
        try:
            async with self._candidate_l2_scan_lock:
                self._require_open()
                if not self._initialised:
                    await self.initialise(timeout_seconds)
                plan = self._candidate_l2_plan
                generation = {
                    venue: self._venue_refresh_generations.get(venue, 0)
                    for venue in {book.instrument.venue for book in plan.books}
                }
                funding = await self._refresh_route_calibration_funding(
                    plan,
                    timeout_seconds,
                )
                self._require_open()
                if plan.signature != self._candidate_l2_plan.signature or any(
                    self._venue_refresh_generations.get(venue, 0) != observed
                    for venue, observed in generation.items()
                ):
                    return ()
                unavailable = frozenset(self._quarantined)
                plan_keys = {book.key for book in plan.books}
                states = {
                    key: CandidateL2BookState(
                        state.book,
                        self._current_candidate_l2_quality(state),
                        state.priority,
                    )
                    for key, state in self._candidate_l2_states.items()
                    if key in plan_keys
                }
                policy = RouteCalibrationSamplingPolicy(
                    self.settings.strategy.calibration_size_multipliers,
                    self.settings.risk.max_hold_seconds,
                    self.settings.strategy.calibration_funding_refresh_seconds,
                    self.settings.execution.funding_stress_multiplier,
                    self.settings.execution.latency_reserve_bps,
                    self.settings.execution.partial_fill_reserve_bps,
                    self.settings.execution.emergency_hedge_reserve_bps,
                    self.settings.execution.reconciliation_forced_exit_reserve_bps,
                )
                current_routes = frozenset(
                    DirectedRouteKey(
                        route.long_instrument.base,
                        route.long_instrument.venue,
                        route.short_instrument.venue,
                    )
                    for route in plan.routes
                )
                removed_routes = tuple(
                    sorted(
                        self._route_calibration_previous_routes - current_routes,
                        key=lambda item: item.value,
                    )
                )
                observations = build_route_calibration_observations(
                    plan.routes,
                    states,
                    funding,
                    policy=policy,
                    epoch_id=epoch_id or "ephemeral-public-session",
                    observed_at=self._now_factory(),
                    unavailable_venues=unavailable,
                    missing_active_routes=plan.missing_active_routes,
                    removed_routes=removed_routes,
                )
                self._route_calibration_previous_routes = current_routes
                return observations
        finally:
            self._finish_public_scan(task)

    async def aggressive_route_market_snapshots(
        self,
        timeout_seconds: int,
    ) -> tuple[AggressiveRouteMarketSnapshot, ...]:
        """Expose immutable raw Candidate-L2 inputs to the shared aggressive core."""
        task = self._begin_public_scan()
        try:
            async with self._candidate_l2_scan_lock:
                self._require_open()
                if not self._initialised:
                    await self.initialise(timeout_seconds)
                plan = self._candidate_l2_plan
                generation = {
                    venue: self._venue_refresh_generations.get(venue, 0)
                    for venue in {book.instrument.venue for book in plan.books}
                }
                funding = await self._refresh_route_calibration_funding(
                    plan,
                    timeout_seconds,
                )
                self._require_open()
                if plan.signature != self._candidate_l2_plan.signature or any(
                    self._venue_refresh_generations.get(venue, 0) != observed
                    for venue, observed in generation.items()
                ):
                    return ()
                unavailable = frozenset(self._quarantined)
                snapshots: list[AggressiveRouteMarketSnapshot] = []
                for planned in plan.routes:
                    long_key = (
                        planned.long_instrument.venue,
                        planned.long_instrument.symbol,
                    )
                    short_key = (
                        planned.short_instrument.venue,
                        planned.short_instrument.symbol,
                    )
                    long_state = self._candidate_l2_states.get(long_key)
                    short_state = self._candidate_l2_states.get(short_key)
                    long_quality = (
                        self._current_candidate_l2_quality(long_state)
                        if long_state is not None
                        else DataQualityAssessment(False, ReasonCode.BOOK_EMPTY, 0)
                    )
                    short_quality = (
                        self._current_candidate_l2_quality(short_state)
                        if short_state is not None
                        else DataQualityAssessment(False, ReasonCode.BOOK_EMPTY, 0)
                    )
                    long_book = long_state.book if long_state is not None else None
                    short_book = short_state.book if short_state is not None else None
                    receipts = tuple(
                        book.received_monotonic_ns
                        for book in (long_book, short_book)
                        if book is not None
                    )
                    snapshots.append(
                        AggressiveRouteMarketSnapshot(
                            route=DirectedRouteKey(
                                planned.long_instrument.base,
                                planned.long_instrument.venue,
                                planned.short_instrument.venue,
                            ),
                            long_instrument=planned.long_instrument,
                            short_instrument=planned.short_instrument,
                            long_book=long_book,
                            short_book=short_book,
                            long_quality=long_quality,
                            short_quality=short_quality,
                            long_funding=funding.get(long_key),
                            short_funding=funding.get(short_key),
                            observed_monotonic_ns=max(receipts, default=self._monotonic_ns()),
                            unavailable_venues=unavailable,
                        )
                    )
                return tuple(snapshots)
        finally:
            self._finish_public_scan(task)

    async def _refresh_route_calibration_funding(
        self,
        plan: CandidateL2Plan,
        timeout_seconds: int,
    ) -> dict[BookKey, FundingSnapshot]:
        del timeout_seconds
        instruments = {book.key: book.instrument for book in plan.books}
        self._route_calibration_funding = {
            key: snapshot
            for key, snapshot in self._route_calibration_funding.items()
            if key in instruments
        }
        self._route_calibration_funding_observed_ns = {
            key: observed
            for key, observed in self._route_calibration_funding_observed_ns.items()
            if key in instruments
        }
        self._route_calibration_funding_generation = {
            key: generation
            for key, generation in self._route_calibration_funding_generation.items()
            if key in instruments
        }
        self._route_calibration_funding_retry_after_ns = {
            key: retry_at
            for key, retry_at in self._route_calibration_funding_retry_after_ns.items()
            if key in instruments
        }
        for key, worker in tuple(self._route_calibration_funding_tasks.items()):
            if key not in instruments:
                self._retire_route_calibration_funding_task(key, worker)
        self._cleanup_retired_route_calibration_funding_tasks()
        self._process_route_calibration_funding_tasks(instruments)
        now_ns = self._monotonic_ns()
        maximum_age_ns = self.settings.strategy.calibration_funding_refresh_seconds * 1_000_000_000
        keys_by_venue = {
            venue: tuple(
                sorted(
                    (key for key in instruments if key[0] == venue),
                    key=lambda item: item[1],
                )
            )
            for venue in sorted({key[0] for key in instruments}, key=lambda item: item.value)
        }
        fair_keys = tuple(
            key
            for index in range(max((len(keys) for keys in keys_by_venue.values()), default=0))
            for keys in keys_by_venue.values()
            if index < len(keys)
            for key in (keys[index],)
        )
        stale = tuple(
            key
            for key in fair_keys
            if key not in self._route_calibration_funding
            or self._route_calibration_funding_generation.get(key)
            != self._venue_refresh_generations.get(key[0], 0)
            or now_ns < self._route_calibration_funding_observed_ns.get(key, 0)
            or now_ns - self._route_calibration_funding_observed_ns.get(key, 0) > maximum_age_ns
        )
        eligible = tuple(
            key
            for key in stale
            if key not in self._route_calibration_funding_tasks
            and self._route_calibration_funding_retry_after_ns.get(key, 0) <= now_ns
            and key[0] not in self._quarantined
            and not self._venue_has_retiring_route_calibration_funding(key[0])
        )
        if eligible:
            start = self._route_calibration_funding_cursor % len(eligible)
            rotated = (*eligible[start:], *eligible[:start])
            venue_capacities = self._route_calibration_funding_capacity_by_venue(instruments)
            inflight_by_venue = {
                venue: sum(1 for key in self._route_calibration_funding_tasks if key[0] == venue)
                for venue in venue_capacities
            }
            available_slots = max(
                0,
                sum(venue_capacities.values()) - len(self._route_calibration_funding_tasks),
            )
            selected_list: list[BookKey] = []
            for key in rotated:
                if len(selected_list) >= available_slots:
                    break
                venue = key[0]
                if inflight_by_venue.get(venue, 0) >= venue_capacities.get(venue, 0):
                    continue
                selected_list.append(key)
                inflight_by_venue[venue] = inflight_by_venue.get(venue, 0) + 1
            selected = tuple(selected_list)
            self._route_calibration_funding_cursor = (start + len(selected)) % len(eligible)
            for key in selected:
                instrument = instruments[key]
                generation = self._venue_refresh_generations.get(instrument.venue, 0)
                worker = asyncio.create_task(
                    self._fetch_route_calibration_funding(key, instrument),
                    name=f"route-funding-{instrument.venue.value}-{instrument.symbol}",
                )
                self._route_calibration_funding_tasks[key] = worker
                self._route_calibration_funding_task_generation[key] = generation
                self._route_calibration_funding_task_started_ns[key] = now_ns
        # Funding is P5/P6 calibration input and must never consume the
        # Candidate-L2 receipt-to-decision budget.  Give immediately-complete
        # adapters one scheduler turn, retain all other workers under the
        # engine shutdown/lifecycle barrier, and fail this scan closed with
        # FUNDING_UNKNOWN until a later scan harvests the cache.
        for _ in range(4):
            await asyncio.sleep(0)
        self._process_route_calibration_funding_tasks(instruments)
        # The cache may still contain a snapshot from the adapter generation
        # that was current at the beginning of this scan.  A reconnect can
        # advance the venue generation while an asynchronous refresh is still
        # pending; returning that older snapshot would mix pre-reconnect
        # funding with post-reconnect L2.  Expose only cache entries that are
        # current at this exact read boundary.  Everything else remains
        # retained solely as an implementation detail until the worker
        # replaces it, while the observation builder fails closed with
        # FUNDING_UNKNOWN.
        qualified: dict[BookKey, FundingSnapshot] = {}
        read_ns = self._monotonic_ns()
        for key, snapshot in self._route_calibration_funding.items():
            observed_ns = self._route_calibration_funding_observed_ns.get(key)
            if (
                key in instruments
                and key[0] not in self._quarantined
                and self._route_calibration_funding_generation.get(key)
                == self._venue_refresh_generations.get(key[0], 0)
                and observed_ns is not None
                and observed_ns <= read_ns
                and read_ns - observed_ns <= maximum_age_ns
            ):
                qualified[key] = snapshot
        return qualified

    def _retire_route_calibration_funding_task(
        self,
        key: BookKey,
        worker: asyncio.Task[tuple[FundingSnapshot, int]],
    ) -> None:
        self._route_calibration_funding_tasks.pop(key, None)
        self._route_calibration_funding_task_generation.pop(key, None)
        started_ns = self._route_calibration_funding_task_started_ns.pop(
            key,
            self._monotonic_ns(),
        )
        worker.cancel()
        existing = self._retiring_route_calibration_funding_tasks.get(key)
        if existing is not None and existing is not worker and not existing.done():
            raise RuntimeError("multiple retiring funding transports for one book")
        self._retiring_route_calibration_funding_tasks[key] = worker
        self._retiring_route_calibration_funding_started_ns[key] = started_ns

    def _cleanup_retired_route_calibration_funding_tasks(self) -> None:
        now_ns = self._monotonic_ns()
        for key, worker in tuple(self._retiring_route_calibration_funding_tasks.items()):
            if not worker.done():
                started_ns = self._retiring_route_calibration_funding_started_ns.get(key, now_ns)
                if (
                    not self._closed
                    and now_ns - started_ns >= self._route_calibration_funding_timeout_ns
                    and key[0] not in self._quarantined
                ):
                    self._quarantine(
                        key[0],
                        f"retired funding snapshot did not terminate for {key[1]}",
                    )
                continue
            self._retiring_route_calibration_funding_tasks.pop(key, None)
            self._retiring_route_calibration_funding_started_ns.pop(key, None)
            with suppress(asyncio.CancelledError, Exception):
                worker.result()
        for key, transport in tuple(self._retiring_route_calibration_funding_transports.items()):
            if not transport.done():
                started_ns = self._retiring_route_calibration_funding_transport_started_ns.get(
                    key,
                    now_ns,
                )
                if (
                    not self._closed
                    and now_ns - started_ns >= self._route_calibration_funding_timeout_ns
                    and key[0] not in self._quarantined
                ):
                    self._quarantine(
                        key[0],
                        f"retired funding transport did not terminate for {key[1]}",
                    )
                continue
            self._retiring_route_calibration_funding_transports.pop(key, None)
            self._retiring_route_calibration_funding_transport_started_ns.pop(key, None)
            watchdog = self._retiring_route_calibration_funding_watchdogs.pop(key, None)
            if watchdog is not None and watchdog is not asyncio.current_task():
                watchdog.cancel()
                watchdog.add_done_callback(self._consume_watcher)
            self._consume_funding_transport(transport)

    def _venue_has_retiring_route_calibration_funding(self, venue: Venue) -> bool:
        return any(
            key[0] == venue and not task.done()
            for key, task in (
                *self._retiring_route_calibration_funding_tasks.items(),
                *self._retiring_route_calibration_funding_transports.items(),
            )
        )

    def _route_calibration_funding_capacity_by_venue(
        self,
        instruments: dict[BookKey, Instrument],
    ) -> dict[Venue, int]:
        counts = {
            venue: sum(1 for key in instruments if key[0] == venue)
            for venue in {key[0] for key in instruments}
        }
        refresh_seconds = self.settings.strategy.calibration_funding_refresh_seconds
        scan_seconds = self.settings.shadow.scan_interval_seconds
        cycles_before_refresh_lead = max(
            1,
            (refresh_seconds * 3 // 4) // scan_seconds,
        )
        return {
            venue: min(
                count,
                max(1, (count + cycles_before_refresh_lead - 1) // cycles_before_refresh_lead),
            )
            for venue, count in counts.items()
        }

    async def _fetch_route_calibration_funding(
        self,
        key: BookKey,
        instrument: Instrument,
    ) -> tuple[FundingSnapshot, int]:
        transport = asyncio.create_task(
            self._adapters[instrument.venue].fetch_funding(instrument),
            name=f"route-funding-transport-{instrument.venue.value}-{instrument.symbol}",
        )
        self._route_calibration_funding_transports[key] = transport
        started_ns = self._monotonic_ns()
        try:
            done, _pending = await asyncio.wait(
                (transport,),
                timeout=self._route_calibration_funding_timeout_ns / 1_000_000_000,
            )
            observed_ns = self._monotonic_ns()
            if (
                transport not in done
                or observed_ns - started_ns > self._route_calibration_funding_timeout_ns
            ):
                self._retire_route_calibration_funding_transport(key, transport)
                if not self._closed and instrument.venue not in self._quarantined:
                    self._quarantine(
                        instrument.venue,
                        f"funding snapshot timed out for {instrument.symbol}",
                    )
                raise TimeoutError("funding snapshot exceeded its one-second deadline")
            self._route_calibration_funding_transports.pop(key, None)
            return transport.result(), observed_ns
        except asyncio.CancelledError:
            self._retire_route_calibration_funding_transport(key, transport)
            raise

    def _retire_route_calibration_funding_transport(
        self,
        key: BookKey,
        transport: asyncio.Task[FundingSnapshot],
    ) -> None:
        self._route_calibration_funding_transports.pop(key, None)
        if transport.done():
            self._consume_funding_transport(transport)
            return
        transport.cancel()
        existing = self._retiring_route_calibration_funding_transports.get(key)
        if existing is not None and existing is not transport and not existing.done():
            raise RuntimeError("multiple retiring funding transports for one book")
        self._retiring_route_calibration_funding_transports[key] = transport
        self._retiring_route_calibration_funding_transport_started_ns[key] = self._monotonic_ns()
        watchdog = self._retiring_route_calibration_funding_watchdogs.get(key)
        if watchdog is None or watchdog.done():
            self._retiring_route_calibration_funding_watchdogs[key] = asyncio.create_task(
                self._watch_retired_route_calibration_funding_transport(key, transport),
                name=f"route-funding-retirement-{key[0].value}-{key[1]}",
            )

    async def _watch_retired_route_calibration_funding_transport(
        self,
        key: BookKey,
        transport: asyncio.Task[FundingSnapshot],
    ) -> None:
        try:
            await asyncio.sleep(self._route_calibration_funding_timeout_ns / 1_000_000_000)
            if self._retiring_route_calibration_funding_transports.get(key) is not transport:
                return
            if transport.done():
                self._cleanup_retired_route_calibration_funding_tasks()
                return
            if not self._closed and key[0] not in self._quarantined:
                self._quarantine(
                    key[0],
                    f"retired funding transport did not terminate for {key[1]}",
                )
        finally:
            current = self._retiring_route_calibration_funding_watchdogs.get(key)
            if current is asyncio.current_task():
                self._retiring_route_calibration_funding_watchdogs.pop(key, None)

    def _process_route_calibration_funding_tasks(
        self,
        instruments: dict[BookKey, Instrument],
    ) -> None:
        now_ns = self._monotonic_ns()
        self._cleanup_retired_route_calibration_funding_tasks()
        for key, worker in tuple(self._route_calibration_funding_tasks.items()):
            started_ns = self._route_calibration_funding_task_started_ns.get(key, now_ns)
            if worker.done() or now_ns - started_ns < self._route_calibration_funding_timeout_ns:
                continue
            self._retire_route_calibration_funding_task(key, worker)
            self._route_calibration_funding_retry_after_ns[key] = now_ns + 30_000_000_000
            self._route_calibration_funding.pop(key, None)
            self._route_calibration_funding_observed_ns.pop(key, None)
            self._route_calibration_funding_generation.pop(key, None)
            if not self._closed and key[0] not in self._quarantined:
                self._quarantine(
                    key[0],
                    f"funding snapshot timed out for {key[1]}",
                )
        for key, worker in tuple(self._route_calibration_funding_tasks.items()):
            if not worker.done():
                continue
            self._route_calibration_funding_tasks.pop(key, None)
            generation = self._route_calibration_funding_task_generation.pop(key, -1)
            self._route_calibration_funding_task_started_ns.pop(key, None)
            instrument = instruments.get(key)
            try:
                snapshot, observed_ns = worker.result()
            except (asyncio.CancelledError, Exception):
                if key in instruments:
                    self._route_calibration_funding_retry_after_ns[key] = now_ns + 30_000_000_000
                else:
                    self._route_calibration_funding_retry_after_ns.pop(key, None)
                self._route_calibration_funding.pop(key, None)
                self._route_calibration_funding_observed_ns.pop(key, None)
                self._route_calibration_funding_generation.pop(key, None)
                continue
            if (
                instrument is None
                or key[0] in self._quarantined
                or generation != self._venue_refresh_generations.get(key[0], 0)
                or snapshot.venue != key[0]
                or snapshot.symbol != key[1]
            ):
                if instrument is not None and (
                    key[0] in self._quarantined
                    or snapshot.venue != key[0]
                    or snapshot.symbol != key[1]
                ):
                    self._route_calibration_funding_retry_after_ns[key] = now_ns + 30_000_000_000
                else:
                    self._route_calibration_funding_retry_after_ns.pop(key, None)
                self._route_calibration_funding.pop(key, None)
                self._route_calibration_funding_observed_ns.pop(key, None)
                self._route_calibration_funding_generation.pop(key, None)
                continue
            self._route_calibration_funding[key] = snapshot
            self._route_calibration_funding_observed_ns[key] = observed_ns
            self._route_calibration_funding_generation[key] = generation
            self._route_calibration_funding_retry_after_ns.pop(key, None)

    async def _run_candidate_l2_request(
        self,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey],
        candidates_admitted: bool,
        prefilter: tuple[BboPrefilterObservation, ...] | None,
        preserve_existing_candidates: bool,
    ) -> CandidateL2Result:
        if candidates_admitted and preserve_existing_candidates:
            self._require_open()
            if not self._initialised:
                await self.initialise(timeout_seconds)
            await self.refresh_universe(timeout_seconds)
            self._require_open()
            ranked_prefilter = self._last_candidate_l2_prefilter
        elif candidates_admitted and prefilter is None:
            broad = await self._scan_broad_bbo(timeout_seconds)
            ranked_prefilter = broad.prefilter
        else:
            self._require_open()
            if not self._initialised:
                await self.initialise(timeout_seconds)
            await self.refresh_universe(timeout_seconds)
            self._require_open()
            ranked_prefilter = prefilter if prefilter is not None else ()
        if candidates_admitted and not preserve_existing_candidates:
            self._last_candidate_l2_prefilter = ranked_prefilter
        return await self._scan_candidate_l2(
            ranked_prefilter,
            timeout_seconds,
            active_route_keys=active_route_keys,
            candidates_admitted=candidates_admitted,
        )

    async def _scan_candidate_l2(
        self,
        prefilter: tuple[BboPrefilterObservation, ...],
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey],
        candidates_admitted: bool,
    ) -> CandidateL2Result:
        self._require_open()
        universe = self._universe.snapshot
        assert universe is not None
        plan = build_candidate_l2_plan(
            universe.routes,
            prefilter,
            active_route_keys=active_route_keys,
            maximum_candidates=self.settings.universe.max_dynamic_l2_candidates,
            candidates_admitted=candidates_admitted,
        )
        demand_plan = (
            plan
            if candidates_admitted
            else build_candidate_l2_plan(
                universe.routes,
                prefilter,
                active_route_keys=active_route_keys,
                maximum_candidates=self.settings.universe.max_dynamic_l2_candidates,
                candidates_admitted=True,
            )
        )
        observed_candidate_demand = sum(
            1 for book in demand_plan.books if book.priority == L2WorkPriority.CANDIDATE_ROUTE
        )
        if candidates_admitted:
            self._candidate_l2_candidate_demand = observed_candidate_demand
        requested_version = self._request_candidate_l2_plan(plan)
        await self._wait_for_candidate_l2_plan(requested_version, timeout_seconds)
        self._require_open()
        await self._wait_for_candidate_l2_coverage(timeout_seconds)
        self._require_open()
        target_decision_version = self._candidate_l2_state_version
        await self._wait_for_candidate_l2_decision(target_decision_version, timeout_seconds)
        self._require_open()
        applied_plan = self._candidate_l2_plan
        evaluation_started_ns = time.perf_counter_ns()
        current_observations = evaluate_candidate_l2_routes(
            applied_plan,
            dict(self._candidate_l2_states),
            decision_monotonic_ns=self._monotonic_ns(),
            maximum_age_ms=self.settings.market_data.max_l2_age_ms,
            unavailable_venues=frozenset(self._quarantined),
        )
        current_receipt_latencies = tuple(
            observation.decision_latency_ms
            for observation in current_observations
            if observation.decision_latency_ms is not None
        )
        current_decision_latency_ms = max(
            max(current_receipt_latencies, default=Decimal(0)),
            Decimal(time.perf_counter_ns() - evaluation_started_ns) / Decimal(1_000_000),
        )
        if current_receipt_latencies:
            self._candidate_l2_latency_samples.append(current_decision_latency_ms)
        stats = CandidateL2Stats(
            self._candidate_l2_plan_generation,
            len(applied_plan.active_routes) + len(applied_plan.missing_active_routes),
            len(applied_plan.candidate_routes),
            len(applied_plan.routes) + len(applied_plan.missing_active_routes),
            len(applied_plan.books),
            len(self._candidate_l2_states),
            len(self._candidate_l2_watchers),
            len(self._retiring_candidate_l2_watchers)
            + len(self._retiring_candidate_l2_transports)
            + len(self._retiring_candidate_l2_unsubscribes),
            self._candidate_l2_peak_books,
            self._candidate_l2_peak_watchers,
            self._candidate_l2_accepted_updates,
            self._candidate_l2_rejected_updates,
            self._candidate_l2_coalesced_plans,
            self._candidate_l2_decision_updates,
            self._candidate_l2_latency_p95_ms(),
        )
        return CandidateL2Result(
            current_observations,
            stats,
            current_decision_latency_ms,
        )

    def _request_candidate_l2_plan(self, plan: CandidateL2Plan) -> int:
        if plan.signature == self._pending_candidate_l2_plan.signature:
            expected_watchers = {
                book.key for book in plan.books if book.instrument.venue not in self._quarantined
            }
            current_watchers = {
                key for key, task in self._candidate_l2_watchers.items() if not task.done()
            }
            if (
                self._candidate_l2_plan_version > self._candidate_l2_applied_version
                or current_watchers == expected_watchers
            ):
                return self._candidate_l2_plan_version
        if self._candidate_l2_plan_version > self._candidate_l2_applied_version:
            self._candidate_l2_coalesced_plans += 1
        self._pending_candidate_l2_plan = plan
        self._candidate_l2_plan_version += 1
        self._candidate_l2_plan_changed.set()
        if self._candidate_l2_debouncer is None or self._candidate_l2_debouncer.done():
            if self._candidate_l2_debouncer is not None:
                self._consume_watcher(self._candidate_l2_debouncer)
            self._candidate_l2_debouncer = asyncio.create_task(
                self._run_candidate_l2_debouncer(),
                name="candidate-l2-debouncer",
            )
        return self._candidate_l2_plan_version

    async def _run_candidate_l2_debouncer(self) -> None:
        debounce_seconds = self.settings.universe.decision_debounce_ms / 1000
        try:
            while not self._closed:
                await self._candidate_l2_plan_changed.wait()
                while not self._closed:
                    self._candidate_l2_plan_changed.clear()
                    version = self._candidate_l2_plan_version
                    try:
                        await asyncio.wait_for(
                            self._candidate_l2_plan_changed.wait(),
                            timeout=debounce_seconds,
                        )
                    except TimeoutError:
                        break
                if self._closed:
                    return
                await self._apply_candidate_l2_plan(self._pending_candidate_l2_plan)
                self._candidate_l2_applied_version = version
                self._candidate_l2_plan_applied.set()
        except asyncio.CancelledError:
            raise

    async def _apply_candidate_l2_plan(self, plan: CandidateL2Plan) -> None:
        self._require_open()
        self._candidate_l2_plan = plan
        self._candidate_l2_plan_generation += 1
        desired = {book.key: book for book in plan.books}
        reusable_state_keys = {
            key
            for key in desired
            if (
                (
                    (task := self._candidate_l2_watchers.get(key)) is not None
                    and not task.done()
                    and self._candidate_l2_watcher_instruments.get(key) == desired[key].instrument
                )
                or (
                    key[0] in self._quarantined
                    and (state := self._candidate_l2_states.get(key)) is not None
                    and state.quality.reason == ReasonCode.VENUE_OUTAGE
                )
            )
        }
        self._candidate_l2_registry.retain_keys(frozenset(desired))
        self._candidate_l2_states = {
            key: CandidateL2BookState(state.book, state.quality, desired[key].priority)
            for key, state in self._candidate_l2_states.items()
            if key in reusable_state_keys
        }
        for key, book_plan in desired.items():
            if key[0] in self._quarantined:
                self._candidate_l2_states[key] = CandidateL2BookState(
                    None,
                    DataQualityAssessment(False, ReasonCode.VENUE_OUTAGE, 0),
                    book_plan.priority,
                )
        self._mark_candidate_l2_state_changed()
        await self._sync_candidate_l2_watchers(desired)
        self._candidate_l2_peak_books = max(
            self._candidate_l2_peak_books,
            len(self._candidate_l2_states),
        )

    async def _wait_for_candidate_l2_plan(self, version: int, timeout_seconds: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while self._candidate_l2_applied_version < version:
            self._candidate_l2_plan_applied.clear()
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("candidate L2 plan debounce exceeded scan deadline")
            await asyncio.wait_for(self._candidate_l2_plan_applied.wait(), timeout=remaining)

    async def _wait_for_candidate_l2_coverage(self, timeout_seconds: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while True:
            desired = {book.key for book in self._candidate_l2_plan.books}
            if desired.issubset(self._candidate_l2_states):
                return
            active = tuple(task for task in self._candidate_l2_watchers.values() if not task.done())
            remaining = deadline - loop.time()
            if not active or remaining <= 0:
                return
            self._candidate_l2_changed.clear()
            try:
                await asyncio.wait_for(self._candidate_l2_changed.wait(), timeout=remaining)
            except TimeoutError:
                return

    async def _wait_for_candidate_l2_decision(
        self,
        version: int,
        timeout_seconds: int,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while self._candidate_l2_decision_version < version:
            self._candidate_l2_decision_ready.clear()
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("candidate L2 decision exceeded scan deadline")
            await asyncio.wait_for(self._candidate_l2_decision_ready.wait(), timeout=remaining)

    def _mark_candidate_l2_state_changed(self) -> None:
        self._candidate_l2_state_version += 1
        self._candidate_l2_changed.set()
        self._candidate_l2_decision_input.set()
        if self._candidate_l2_decision_worker is None or self._candidate_l2_decision_worker.done():
            if self._candidate_l2_decision_worker is not None:
                self._consume_watcher(self._candidate_l2_decision_worker)
            self._candidate_l2_decision_worker = asyncio.create_task(
                self._run_candidate_l2_decision_worker(),
                name="candidate-l2-decision",
            )

    async def _run_candidate_l2_decision_worker(self) -> None:
        try:
            while not self._closed:
                await self._candidate_l2_decision_input.wait()
                if self._closed:
                    return
                self._candidate_l2_decision_input.clear()
                await asyncio.sleep(0)
                if self._closed:
                    return
                version = self._candidate_l2_state_version
                started_ns = time.perf_counter_ns()
                observations = evaluate_candidate_l2_routes(
                    self._candidate_l2_plan,
                    dict(self._candidate_l2_states),
                    decision_monotonic_ns=self._monotonic_ns(),
                    maximum_age_ms=self.settings.market_data.max_l2_age_ms,
                    unavailable_venues=frozenset(self._quarantined),
                )
                receipt_latencies = tuple(
                    observation.decision_latency_ms
                    for observation in observations
                    if observation.decision_latency_ms is not None
                )
                receipt_latency = max(receipt_latencies, default=Decimal(0))
                compute_latency = Decimal(time.perf_counter_ns() - started_ns) / Decimal(1_000_000)
                self._candidate_l2_observations = observations
                self._candidate_l2_decision_latency_ms = max(
                    receipt_latency,
                    compute_latency,
                )
                if receipt_latencies:
                    self._candidate_l2_latency_samples.append(
                        self._candidate_l2_decision_latency_ms
                    )
                self._candidate_l2_decision_version = version
                self._candidate_l2_decision_updates += 1
                self._candidate_l2_decision_ready.set()
        except asyncio.CancelledError:
            raise

    def _candidate_l2_latency_p95_ms(self) -> Decimal | None:
        if not self._candidate_l2_latency_samples:
            return None
        ordered = sorted(self._candidate_l2_latency_samples)
        index = max(0, (len(ordered) * 95 + 99) // 100 - 1)
        return ordered[index]

    async def _sync_candidate_l2_watchers(
        self,
        desired: dict[BookKey, CandidateL2BookPlan],
    ) -> None:
        self._cleanup_retired_candidate_l2_tasks()
        newly_retiring: list[asyncio.Task[None]] = []
        new_unsubscribes: list[asyncio.Task[None]] = []
        for key, task in tuple(self._candidate_l2_watchers.items()):
            planned = desired.get(key)
            current_instrument = self._candidate_l2_watcher_instruments.get(key)
            instrument_changed = planned is not None and current_instrument != planned.instrument
            if task.done():
                self._candidate_l2_watchers.pop(key, None)
                self._candidate_l2_watcher_instruments.pop(key, None)
                self._consume_watcher(task)
            elif planned is None or instrument_changed:
                task.cancel()
                self._candidate_l2_watchers.pop(key, None)
                self._candidate_l2_watcher_instruments.pop(key, None)
                self._retiring_candidate_l2_watchers[key] = task
                newly_retiring.append(task)
                if (
                    current_instrument is not None
                    and key in self._candidate_l2_subscription_started
                    and key not in self._retiring_candidate_l2_unsubscribes
                ):
                    unsubscribe = asyncio.create_task(
                        self._adapters[current_instrument.venue].unwatch_order_book(
                            current_instrument
                        ),
                        name=f"candidate-l2-unsubscribe-{key[0].value}-{key[1]}",
                    )
                    self._retiring_candidate_l2_unsubscribes[key] = unsubscribe
                    new_unsubscribes.append(unsubscribe)
        self._start_candidate_l2_watchers(
            desired,
            maximum_priority=L2WorkPriority.ACTIVE_ROUTE,
        )
        if newly_retiring:
            await asyncio.wait(newly_retiring, timeout=self._bbo_retirement_grace_seconds)
            await asyncio.sleep(0)
        if new_unsubscribes:
            await asyncio.wait(
                new_unsubscribes,
                timeout=self._candidate_l2_unsubscribe_timeout_seconds,
            )
            await asyncio.sleep(0)
        self._cleanup_retired_candidate_l2_tasks()
        self._require_open()
        stuck_venues = {
            key[0]
            for key, task in (
                *self._retiring_candidate_l2_watchers.items(),
                *self._retiring_candidate_l2_transports.items(),
                *self._retiring_candidate_l2_unsubscribes.items(),
            )
            if not task.done()
        }
        for venue in stuck_venues:
            if venue not in self._quarantined:
                self._quarantine(venue, "prior candidate L2 watcher did not terminate")
        self._require_open()
        self._start_candidate_l2_watchers(desired)
        self._candidate_l2_peak_watchers = max(
            self._candidate_l2_peak_watchers,
            len(self._candidate_l2_watchers),
        )

    def _start_candidate_l2_watchers(
        self,
        desired: dict[BookKey, CandidateL2BookPlan],
        *,
        maximum_priority: L2WorkPriority | None = None,
    ) -> None:
        for key, book_plan in desired.items():
            if maximum_priority is not None and book_plan.priority > maximum_priority:
                continue
            if (
                key not in self._candidate_l2_watchers
                and key not in self._retiring_candidate_l2_watchers
                and key not in self._retiring_candidate_l2_transports
                and key not in self._retiring_candidate_l2_unsubscribes
                and key[0] not in self._quarantined
            ):
                self._candidate_l2_watcher_instruments[key] = book_plan.instrument
                self._candidate_l2_watchers[key] = asyncio.create_task(
                    self._run_candidate_l2_watcher(key),
                    name=f"candidate-l2-{key[0].value}-{key[1]}",
                )

    async def _run_candidate_l2_watcher(self, key: BookKey) -> None:
        try:
            while (
                not self._closed
                and key in self._candidate_l2_watcher_instruments
                and key[0] not in self._quarantined
            ):
                instrument = self._candidate_l2_watcher_instruments[key]
                book = await self._next_candidate_l2_update(key, instrument)
                if (
                    self._closed
                    or key[0] in self._quarantined
                    or self._candidate_l2_watcher_instruments.get(key) != instrument
                ):
                    return
                quality = self._candidate_l2_registry.accept(
                    book,
                    max_age_ms=self.settings.market_data.max_l2_age_ms,
                    max_clock_skew_ms=self.settings.market_data.max_clock_skew_ms,
                    now_monotonic_ns=self._monotonic_ns(),
                )
                book_plan = next(
                    (plan for plan in self._candidate_l2_plan.books if plan.key == key),
                    None,
                )
                if book_plan is None:
                    return
                self._candidate_l2_states[key] = CandidateL2BookState(
                    book,
                    quality,
                    book_plan.priority,
                )
                if quality.accepted:
                    self._candidate_l2_accepted_updates += 1
                else:
                    self._candidate_l2_rejected_updates += 1
                self._candidate_l2_peak_books = max(
                    self._candidate_l2_peak_books,
                    len(self._candidate_l2_states),
                )
                self._mark_candidate_l2_state_changed()
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._closed:
                return
            failed_priority = next(
                (plan.priority for plan in self._candidate_l2_plan.books if plan.key == key),
                None,
            )
            if failed_priority is not None and key[0] not in self._quarantined:
                self._quarantine(
                    key[0],
                    f"candidate L2 stream failed: {type(error).__name__}: {error}",
                )

    async def _next_candidate_l2_update(
        self,
        key: BookKey,
        instrument: Instrument,
    ) -> OrderBookSnapshot:
        transport = asyncio.create_task(
            self._watch_candidate_l2_subscription(key, instrument),
            name=f"candidate-l2-transport-{instrument.venue.value}-{instrument.symbol}",
        )
        self._candidate_l2_transports[key] = transport
        try:
            done, _ = await asyncio.wait(
                (transport,),
                timeout=self._candidate_l2_watch_timeout_seconds,
            )
            if not done:
                self._retire_candidate_l2_transport(key, transport)
                raise TimeoutError("candidate L2 stream made no progress before staleness deadline")
            self._candidate_l2_transports.pop(key, None)
            return transport.result()
        except asyncio.CancelledError:
            self._retire_candidate_l2_transport(key, transport)
            raise

    async def _watch_candidate_l2_subscription(
        self,
        key: BookKey,
        instrument: Instrument,
    ) -> OrderBookSnapshot:
        lock = await self._acquire_l2_transport_lock(key)
        try:
            self._candidate_l2_subscription_started.add(key)
            return await self._adapters[instrument.venue].watch_order_book(instrument)
        finally:
            self._release_l2_transport_lock(key, lock)

    def _retire_candidate_l2_transport(
        self,
        key: BookKey,
        task: asyncio.Task[OrderBookSnapshot],
    ) -> None:
        self._candidate_l2_transports.pop(key, None)
        if task.done():
            self._consume_book_transport(task)
            return
        task.cancel()
        existing = self._retiring_candidate_l2_transports.get(key)
        if existing is not None and existing is not task and not existing.done():
            raise RuntimeError("multiple retiring candidate L2 transports for one book")
        self._retiring_candidate_l2_transports[key] = task

    def _cleanup_retired_candidate_l2_tasks(self) -> None:
        for key, watcher in tuple(self._retiring_candidate_l2_watchers.items()):
            if watcher.done():
                self._retiring_candidate_l2_watchers.pop(key, None)
                self._consume_watcher(watcher)
        for key, transport in tuple(self._retiring_candidate_l2_transports.items()):
            if transport.done():
                self._retiring_candidate_l2_transports.pop(key, None)
                self._consume_book_transport(transport)
        for key, unsubscribe in tuple(self._retiring_candidate_l2_unsubscribes.items()):
            if not unsubscribe.done():
                continue
            self._retiring_candidate_l2_unsubscribes.pop(key, None)
            try:
                unsubscribe.result()
            except (asyncio.CancelledError, Exception) as error:
                self._candidate_l2_unsubscribe_failures.add(key[0])
                if not self._closed and key[0] not in self._quarantined:
                    self._quarantine(
                        key[0],
                        f"candidate L2 unsubscribe failed: {type(error).__name__}: {error}",
                    )
            else:
                self._candidate_l2_subscription_started.discard(key)
        self._prune_l2_transport_locks()

    def _prune_l2_transport_locks(self) -> None:
        snapshot = self._universe.snapshot
        if snapshot is None:
            return
        owned_keys = {
            *self._candidate_l2_watchers,
            *self._candidate_l2_transports,
            *self._retiring_candidate_l2_watchers,
            *self._retiring_candidate_l2_transports,
            *self._retiring_candidate_l2_unsubscribes,
        }
        known_keys = snapshot.known_bbo_keys
        self._l2_transport_locks = {
            key: lock
            for key, lock in self._l2_transport_locks.items()
            if key in known_keys
            or key in owned_keys
            or lock.locked()
            or self._l2_transport_lock_users.get(key, 0) > 0
        }

    async def _acquire_l2_transport_lock(self, key: BookKey) -> asyncio.Lock:
        lock = self._l2_transport_locks.setdefault(key, asyncio.Lock())
        self._l2_transport_lock_users[key] = self._l2_transport_lock_users.get(key, 0) + 1
        try:
            await lock.acquire()
        except BaseException:
            self._decrement_l2_transport_lock_users(key)
            self._prune_l2_transport_locks()
            raise
        return lock

    def _release_l2_transport_lock(self, key: BookKey, lock: asyncio.Lock) -> None:
        lock.release()
        self._decrement_l2_transport_lock_users(key)
        self._prune_l2_transport_locks()

    def _decrement_l2_transport_lock_users(self, key: BookKey) -> None:
        remaining = self._l2_transport_lock_users.get(key, 0) - 1
        if remaining > 0:
            self._l2_transport_lock_users[key] = remaining
        else:
            self._l2_transport_lock_users.pop(key, None)

    def _venue_has_retiring_candidate_l2(self, venue: Venue) -> bool:
        return (
            any(
                key[0] == venue
                for key in (
                    *self._retiring_candidate_l2_watchers,
                    *self._retiring_candidate_l2_transports,
                    *self._retiring_candidate_l2_unsubscribes,
                )
            )
            or venue in self._candidate_l2_unsubscribe_failures
        )

    def _retiring_candidate_l2_tasks_for_venue(
        self,
        venue: Venue,
    ) -> tuple[asyncio.Task[None] | asyncio.Task[OrderBookSnapshot], ...]:
        tasks: list[asyncio.Task[None] | asyncio.Task[OrderBookSnapshot]] = []
        tasks.extend(
            task for key, task in self._retiring_candidate_l2_watchers.items() if key[0] == venue
        )
        tasks.extend(
            task for key, task in self._retiring_candidate_l2_transports.items() if key[0] == venue
        )
        tasks.extend(
            task
            for key, task in self._retiring_candidate_l2_unsubscribes.items()
            if key[0] == venue
        )
        return tuple(tasks)

    async def scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey] = frozenset(),
        entry_work_admitted: bool = True,
    ) -> ScanResult:
        task = self._begin_public_scan()
        try:
            async with self._selected_scan_lock:
                self._require_open()
                return await self._scan_once(
                    base,
                    requested_base_quantity,
                    timeout_seconds,
                    active_route_keys=active_route_keys,
                    entry_work_admitted=entry_work_admitted,
                )
        finally:
            self._finish_public_scan(task)

    async def _scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey],
        entry_work_admitted: bool,
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
        active_selected_route_keys = {key for key in active_route_keys if key[0] == base.upper()}
        selected_base_is_active = bool(active_selected_route_keys)
        if not entry_work_admitted and not selected_base_is_active:
            return self._result(base, len(common), (), (), (), (), broad)
        selected_venues = {
            Venue(venue) for key in active_selected_route_keys for venue in (key[1], key[2])
        }
        sampled_instruments = tuple(
            instrument
            for instrument in selected.instruments
            if entry_work_admitted or instrument.venue in selected_venues
        )
        sampling_generations = {
            instrument.venue: self._venue_refresh_generations.get(instrument.venue, 0)
            for instrument in sampled_instruments
        }
        selected_keys = {
            (instrument.venue, instrument.symbol) for instrument in sampled_instruments
        }
        bbo = tuple(quote for quote in broad.bbo if (quote.venue, quote.symbol) in selected_keys)
        funding_samples = await asyncio.gather(
            *(
                self._sample_funding(instrument, timeout_seconds)
                for instrument in sampled_instruments
                if instrument.venue not in self._quarantined
            )
        )
        self._require_open()
        if self._sampling_generations_changed(sampling_generations):
            return self._result(base, len(common), (), (), (), (), broad)
        funding_by_venue = {
            instrument.venue: (instrument, funding)
            for instrument, funding in funding_samples
            if funding is not None and instrument.venue not in self._quarantined
        }
        warmup_samples = await asyncio.gather(
            *(
                self._sample_book(instrument, timeout_seconds, require_new=False)
                for instrument, _ in funding_by_venue.values()
            )
        )
        self._require_open()
        if self._sampling_generations_changed(sampling_generations):
            return self._result(base, len(common), (), (), (), (), broad)
        warmed_instruments = tuple(
            instrument
            for instrument, book, _ in warmup_samples
            if book is not None and instrument.venue not in self._quarantined
        )
        book_samples = await asyncio.gather(
            *(
                self._sample_book(instrument, timeout_seconds, require_new=True)
                for instrument in warmed_instruments
            )
        )
        self._require_open()
        if self._sampling_generations_changed(sampling_generations):
            return self._result(base, len(common), (), (), (), (), broad)
        complete = {
            instrument.venue: (
                instrument,
                book,
                funding_by_venue[instrument.venue][1],
                candidate_quality,
            )
            for instrument, book, candidate_quality in book_samples
            if book is not None and instrument.venue in funding_by_venue
        }
        books = tuple(sample[1] for sample in complete.values())
        quality: dict[Venue, DataQualityAssessment] = {}
        for venue, (_, book, _, candidate_quality) in complete.items():
            quality[venue] = (
                candidate_quality
                if candidate_quality is not None
                else self._books.accept(
                    book,
                    max_age_ms=self.settings.market_data.max_l2_age_ms,
                    max_clock_skew_ms=self.settings.market_data.max_clock_skew_ms,
                    now_monotonic_ns=self._monotonic_ns(),
                )
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
            if entry_work_admitted
            or (
                long_instrument.base,
                long_instrument.venue.value,
                short_instrument.venue.value,
            )
            in active_selected_route_keys
        )
        if books and self._broad_bbo_admitted:
            await self._recorder.append_books(books)
            self._require_open()
            if self._sampling_generations_changed(sampling_generations):
                return self._result(base, len(common), (), (), (), (), broad)
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

    def _sampling_generations_changed(self, expected: dict[Venue, int]) -> bool:
        return any(
            self._venue_refresh_generations.get(venue, 0) != generation
            for venue, generation in expected.items()
        )

    def _current_candidate_l2_quality(
        self,
        state: CandidateL2BookState,
    ) -> DataQualityAssessment:
        if not state.quality.accepted or state.book is None:
            return state.quality
        age_ns = self._monotonic_ns() - state.book.received_monotonic_ns
        if age_ns < 0:
            return DataQualityAssessment(False, ReasonCode.BOOK_UNSYNCHRONISED, 0)
        age_ms = age_ns // 1_000_000
        if age_ms > self.settings.market_data.max_l2_age_ms:
            return DataQualityAssessment(False, ReasonCode.BOOK_STALE, age_ms)
        return DataQualityAssessment(True, ReasonCode.QUOTE_READY, age_ms)

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
        *,
        require_new: bool = False,
    ) -> tuple[Instrument, OrderBookSnapshot | None, DataQualityAssessment | None]:
        key = (instrument.venue, instrument.symbol)
        watcher = self._candidate_l2_watchers.get(key)
        if (
            watcher is not None
            and not watcher.done()
            and self._candidate_l2_watcher_instruments.get(key) == instrument
        ):
            state = await self._sample_candidate_l2_book(
                key,
                timeout_seconds,
                require_new=require_new,
            )
            return (
                instrument,
                state.book if state is not None else None,
                self._current_candidate_l2_quality(state) if state is not None else None,
            )
        candidate_owns_subscription = False
        try:
            async with asyncio.timeout(timeout_seconds):
                lock = await self._acquire_l2_transport_lock(key)
                try:
                    watcher = self._candidate_l2_watchers.get(key)
                    candidate_owns_subscription = (
                        watcher is not None
                        and not watcher.done()
                        and self._candidate_l2_watcher_instruments.get(key) == instrument
                    )
                    if candidate_owns_subscription:
                        book = None
                    else:
                        book = await self._adapters[instrument.venue].watch_order_book(instrument)
                finally:
                    self._release_l2_transport_lock(key, lock)
            if candidate_owns_subscription:
                state = await self._sample_candidate_l2_book(
                    key,
                    timeout_seconds,
                    require_new=require_new,
                )
                return (
                    instrument,
                    state.book if state is not None else None,
                    self._current_candidate_l2_quality(state) if state is not None else None,
                )
            return instrument, book, None
        except Exception as error:
            if not self._closed:
                self._quarantine(
                    instrument.venue,
                    f"L2 stream failed: {type(error).__name__}: {error}",
                )
            return instrument, None, None

    async def _sample_candidate_l2_book(
        self,
        key: BookKey,
        timeout_seconds: int,
        *,
        require_new: bool,
    ) -> CandidateL2BookState | None:
        initial = self._candidate_l2_states.get(key)
        baseline = (
            (
                initial.book.sequence_end,
                initial.book.received_monotonic_ns,
            )
            if require_new and initial is not None and initial.book is not None
            else None
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while True:
            self._require_open()
            state = self._candidate_l2_states.get(key)
            if state is not None and state.book is not None:
                version = (state.book.sequence_end, state.book.received_monotonic_ns)
                if baseline is None or version != baseline:
                    return state
            watcher = self._candidate_l2_watchers.get(key)
            remaining = deadline - loop.time()
            if watcher is None or watcher.done() or remaining <= 0:
                return None
            self._candidate_l2_changed.clear()
            try:
                await asyncio.wait_for(self._candidate_l2_changed.wait(), timeout=remaining)
            except TimeoutError:
                return None

    def _result(
        self,
        base: str,
        common_count: int,
        bbo: tuple[BboQuote, ...],
        funding: tuple[FundingSnapshot, ...],
        data_quality: tuple[VenueDataQuality, ...],
        quotes: tuple[DirectedRouteQuote, ...],
        broad: BroadBboResult | None = None,
        candidate_l2: CandidateL2Result | None = None,
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
            candidate_l2=candidate_l2,
            venue_capability_matrix=self.venue_capability_matrix(),
        )

    async def close(self) -> None:
        self._closed = True
        self._venue_initialise_attempts.clear()
        for initialiser in self._retiring_venue_initialisers:
            initialiser.cancel()
        self._bbo_changed.set()
        for key, funding_worker in tuple(self._route_calibration_funding_tasks.items()):
            self._retire_route_calibration_funding_task(key, funding_worker)
        for funding_worker in self._retiring_route_calibration_funding_tasks.values():
            funding_worker.cancel()
        for watchdog in self._retiring_route_calibration_funding_watchdogs.values():
            watchdog.cancel()
            watchdog.add_done_callback(self._consume_watcher)
        self._retiring_route_calibration_funding_watchdogs.clear()
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
        self._cleanup_retired_venue_initialisers()
        self._cleanup_retired_bbo_tasks()
        self._cleanup_retired_candidate_l2_tasks()
        for venue, closer in tuple(self._retiring_adapter_closers.items()):
            if closer.done():
                self._retiring_adapter_closers.pop(venue, None)
                try:
                    closer.result()
                except (asyncio.CancelledError, Exception) as error:
                    close_failures.append(f"{venue.value}: {type(error).__name__}: {error}")
        for bbo_task in self._bbo_watchers.values():
            bbo_task.cancel()
        for bbo_unsubscribe in self._retiring_bbo_unsubscribes.values():
            bbo_unsubscribe.cancel()
        if self._candidate_l2_debouncer is not None:
            self._candidate_l2_debouncer.cancel()
            self._candidate_l2_debouncer.add_done_callback(self._consume_watcher)
        if self._candidate_l2_decision_worker is not None:
            self._candidate_l2_decision_worker.cancel()
            self._candidate_l2_decision_worker.add_done_callback(self._consume_watcher)
        for l2_watcher_task in self._candidate_l2_watchers.values():
            l2_watcher_task.cancel()
        for l2_transport_task in self._candidate_l2_transports.values():
            l2_transport_task.cancel()
        for unsubscribe_task in self._retiring_candidate_l2_unsubscribes.values():
            unsubscribe_task.cancel()
        for key, funding_worker in tuple(self._route_calibration_funding_tasks.items()):
            self._retire_route_calibration_funding_task(key, funding_worker)
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
            for venue, closer_task in adapter_closers:
                if closer_task in done_closers:
                    self._retiring_adapter_closers.pop(venue, None)
                    try:
                        closer_task.result()
                    except (asyncio.CancelledError, Exception) as error:
                        close_failures.append(f"{venue.value}: {type(error).__name__}: {error}")
                elif closer_task in pending_closers:
                    timed_out_closer_venues.add(venue)
                    closer_task.cancel()
                    closer_task.add_done_callback(self._consume_watcher)
        await asyncio.sleep(0)
        for venue, bbo_task in self._bbo_watchers.items():
            self._retiring_bbo_watchers.setdefault(venue, bbo_task)
        self._bbo_watchers.clear()
        self._bbo_watcher_symbols.clear()
        for key, l2_watcher_task in self._candidate_l2_watchers.items():
            self._retiring_candidate_l2_watchers.setdefault(key, l2_watcher_task)
        self._candidate_l2_watchers.clear()
        self._candidate_l2_watcher_instruments.clear()
        for key, l2_transport_task in self._candidate_l2_transports.items():
            self._retiring_candidate_l2_transports.setdefault(key, l2_transport_task)
        self._candidate_l2_transports.clear()
        retiring_watchers = tuple(set(self._retiring_bbo_watchers.values()))
        retiring_transports = tuple(set(self._retiring_bbo_transports.values()))
        retiring_bbo_unsubscribes = tuple(set(self._retiring_bbo_unsubscribes.values()))
        retiring_l2_watchers = tuple(set(self._retiring_candidate_l2_watchers.values()))
        retiring_l2_transports = tuple(set(self._retiring_candidate_l2_transports.values()))
        retiring_l2_unsubscribes = tuple(set(self._retiring_candidate_l2_unsubscribes.values()))
        retiring_funding = tuple(set(self._retiring_route_calibration_funding_tasks.values()))
        retiring_funding_transports = tuple(
            set(self._retiring_route_calibration_funding_transports.values())
        )
        retiring_initialisers = tuple(self._retiring_venue_initialisers)
        for watcher in retiring_watchers:
            watcher.cancel()
        for bbo_transport in retiring_transports:
            bbo_transport.cancel()
        for bbo_unsubscribe in retiring_bbo_unsubscribes:
            bbo_unsubscribe.cancel()
        for l2_watcher in retiring_l2_watchers:
            l2_watcher.cancel()
        for l2_transport in retiring_l2_transports:
            l2_transport.cancel()
        for l2_unsubscribe in retiring_l2_unsubscribes:
            l2_unsubscribe.cancel()
        for funding_worker in retiring_funding:
            funding_worker.cancel()
        for funding_transport in retiring_funding_transports:
            funding_transport.cancel()
        for initialiser in retiring_initialisers:
            initialiser.cancel()
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
        if retiring_bbo_unsubscribes:
            await asyncio.wait(
                retiring_bbo_unsubscribes,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_l2_watchers:
            await asyncio.wait(
                retiring_l2_watchers,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_l2_transports:
            await asyncio.wait(
                retiring_l2_transports,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_l2_unsubscribes:
            await asyncio.wait(
                retiring_l2_unsubscribes,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_funding:
            await asyncio.wait(
                retiring_funding,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_funding_transports:
            await asyncio.wait(
                retiring_funding_transports,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_initialisers:
            await asyncio.wait(
                retiring_initialisers,
                timeout=max(0, shutdown_deadline - asyncio.get_running_loop().time()),
            )
        if retiring_watchers or retiring_transports:
            for watcher in retiring_watchers:
                if not watcher.done():
                    watcher.add_done_callback(self._consume_watcher)
            for bbo_transport in retiring_transports:
                if not bbo_transport.done():
                    bbo_transport.add_done_callback(self._consume_transport)
        for bbo_unsubscribe in retiring_bbo_unsubscribes:
            if not bbo_unsubscribe.done():
                bbo_unsubscribe.add_done_callback(self._consume_watcher)
        for l2_watcher in retiring_l2_watchers:
            if not l2_watcher.done():
                l2_watcher.add_done_callback(self._consume_watcher)
        for l2_transport in retiring_l2_transports:
            if not l2_transport.done():
                l2_transport.add_done_callback(self._consume_book_transport)
        for l2_unsubscribe in retiring_l2_unsubscribes:
            if not l2_unsubscribe.done():
                l2_unsubscribe.add_done_callback(self._consume_watcher)
        for funding_worker in retiring_funding:
            if not funding_worker.done():
                funding_worker.add_done_callback(self._consume_funding_worker)
        for funding_transport in retiring_funding_transports:
            if not funding_transport.done():
                funding_transport.add_done_callback(self._consume_funding_transport)
        for initialiser in retiring_initialisers:
            if not initialiser.done():
                initialiser.add_done_callback(self._consume_watcher)
        self._cleanup_retired_venue_initialisers()
        self._cleanup_retired_bbo_tasks()
        self._cleanup_retired_candidate_l2_tasks()
        self._cleanup_retired_route_calibration_funding_tasks()
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
                    for venue, task in self._retiring_bbo_unsubscribes.items()
                    if not task.done()
                }
                | {
                    venue
                    for venue, task in self._retiring_adapter_closers.items()
                    if not task.done()
                }
                | {
                    key[0]
                    for key, task in self._retiring_candidate_l2_watchers.items()
                    if not task.done()
                }
                | {
                    key[0]
                    for key, task in self._retiring_candidate_l2_transports.items()
                    if not task.done()
                }
                | {
                    key[0]
                    for key, task in self._retiring_candidate_l2_unsubscribes.items()
                    if not task.done()
                }
                | {
                    key[0]
                    for key, task in self._retiring_route_calibration_funding_tasks.items()
                    if not task.done()
                }
                | {
                    key[0]
                    for key, task in self._retiring_route_calibration_funding_transports.items()
                    if not task.done()
                }
                | {
                    venue
                    for task, venue in self._retiring_venue_initialisers.items()
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

    @staticmethod
    def _consume_funding_worker(
        task: asyncio.Task[tuple[FundingSnapshot, int]],
    ) -> None:
        with suppress(asyncio.CancelledError, Exception):
            task.result()

    @staticmethod
    def _consume_funding_transport(task: asyncio.Task[FundingSnapshot]) -> None:
        with suppress(asyncio.CancelledError, Exception):
            task.result()

    @staticmethod
    def _consume_book_transport(task: asyncio.Task[OrderBookSnapshot]) -> None:
        with suppress(asyncio.CancelledError, Exception):
            task.result()
