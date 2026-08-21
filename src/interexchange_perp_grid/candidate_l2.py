from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import IntEnum

from interexchange_perp_grid.bbo_prefilter import BboPrefilterObservation
from interexchange_perp_grid.domain import Instrument, OrderBookSnapshot, Venue
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.market_universe import UniverseRoute
from interexchange_perp_grid.reason_codes import ReasonCode

RouteStableKey = tuple[str, str, str]
BookKey = tuple[Venue, str]


class L2WorkPriority(IntEnum):
    ACTIVE_ROUTE = 2
    CANDIDATE_ROUTE = 5


@dataclass(frozen=True, slots=True)
class CandidateL2BookPlan:
    instrument: Instrument
    priority: L2WorkPriority

    @property
    def key(self) -> BookKey:
        return self.instrument.venue, self.instrument.symbol


@dataclass(frozen=True, slots=True)
class CandidateL2Plan:
    active_routes: tuple[UniverseRoute, ...]
    candidate_routes: tuple[UniverseRoute, ...]
    books: tuple[CandidateL2BookPlan, ...]
    missing_active_routes: tuple[RouteStableKey, ...] = ()

    @property
    def routes(self) -> tuple[UniverseRoute, ...]:
        return (*self.active_routes, *self.candidate_routes)

    @property
    def signature(self) -> tuple[object, ...]:
        return (
            tuple(route.stable_key for route in self.active_routes),
            tuple(route.stable_key for route in self.candidate_routes),
            tuple((book.instrument, int(book.priority)) for book in self.books),
            self.missing_active_routes,
        )


@dataclass(frozen=True, slots=True)
class CandidateL2BookState:
    book: OrderBookSnapshot | None
    quality: DataQualityAssessment
    priority: L2WorkPriority


@dataclass(frozen=True, slots=True)
class CandidateL2RouteObservation:
    base: str
    long_venue: Venue
    short_venue: Venue
    long_symbol: str
    short_symbol: str
    priority: L2WorkPriority
    reason: ReasonCode
    observed_monotonic_ns: int
    decision_latency_ms: Decimal | None
    execution_authorized: bool = field(default=False, init=False)

    @property
    def stable_key(self) -> RouteStableKey:
        return self.base, self.long_venue.value, self.short_venue.value


@dataclass(frozen=True, slots=True)
class CandidateL2Stats:
    plan_generation: int
    active_routes: int
    candidate_routes: int
    selected_routes: int
    known_books: int
    cached_books: int
    active_watchers: int
    retiring_tasks: int
    peak_books: int
    peak_watchers: int
    accepted_updates: int
    rejected_updates: int
    coalesced_plan_updates: int
    decision_updates: int = 0
    decision_latency_p95_ms: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CandidateL2Result:
    routes: tuple[CandidateL2RouteObservation, ...]
    stats: CandidateL2Stats
    decision_latency_ms: Decimal
    execution_authorized: bool = field(default=False, init=False)


def build_candidate_l2_plan(
    routes: tuple[UniverseRoute, ...],
    prefilter: tuple[BboPrefilterObservation, ...],
    *,
    active_route_keys: frozenset[RouteStableKey],
    maximum_candidates: int,
    candidates_admitted: bool,
) -> CandidateL2Plan:
    if maximum_candidates <= 0:
        raise ValueError("candidate L2 route limit must be positive")
    by_key = {route.stable_key: route for route in routes}
    active = tuple(by_key[key] for key in sorted(active_route_keys) if key in by_key)
    missing_active = tuple(key for key in sorted(active_route_keys) if key not in by_key)
    active_keys = {route.stable_key for route in active}
    ranked_keys = (
        tuple(
            dict.fromkeys(
                observation.stable_key
                for observation in prefilter
                if observation.reason == ReasonCode.QUOTE_READY
                and observation.stable_key in by_key
                and observation.stable_key not in active_keys
            )
        )
        if candidates_admitted
        else ()
    )
    candidate = tuple(by_key[key] for key in ranked_keys[:maximum_candidates])
    priorities: dict[BookKey, CandidateL2BookPlan] = {}
    for route, priority in (
        *((route, L2WorkPriority.ACTIVE_ROUTE) for route in active),
        *((route, L2WorkPriority.CANDIDATE_ROUTE) for route in candidate),
    ):
        for instrument in (route.long_instrument, route.short_instrument):
            key = (instrument.venue, instrument.symbol)
            current = priorities.get(key)
            if current is None or priority < current.priority:
                priorities[key] = CandidateL2BookPlan(instrument, priority)
    books = tuple(
        sorted(
            priorities.values(),
            key=lambda book: (
                int(book.priority),
                book.instrument.venue.value,
                book.instrument.symbol,
            ),
        )
    )
    return CandidateL2Plan(active, candidate, books, missing_active)


def evaluate_candidate_l2_routes(
    plan: CandidateL2Plan,
    states: dict[BookKey, CandidateL2BookState],
    *,
    decision_monotonic_ns: int,
    maximum_age_ms: int,
    unavailable_venues: frozenset[Venue] = frozenset(),
) -> tuple[CandidateL2RouteObservation, ...]:
    active_keys = {route.stable_key for route in plan.active_routes}
    observations: list[CandidateL2RouteObservation] = []
    for route in plan.routes:
        long = route.long_instrument
        short = route.short_instrument
        long_state = states.get((long.venue, long.symbol))
        short_state = states.get((short.venue, short.symbol))
        reason = _route_reason(
            long_state,
            short_state,
            decision_monotonic_ns=decision_monotonic_ns,
            maximum_age_ms=maximum_age_ms,
        )
        received = tuple(
            state.book.received_monotonic_ns
            for state in (long_state, short_state)
            if state is not None and state.book is not None
        )
        observed_ns = max(received, default=0)
        latency_ms = (
            Decimal(decision_monotonic_ns - min(received)) / Decimal(1_000_000)
            if reason == ReasonCode.QUOTE_READY and len(received) == 2
            else None
        )
        observations.append(
            CandidateL2RouteObservation(
                long.base,
                long.venue,
                short.venue,
                long.symbol,
                short.symbol,
                L2WorkPriority.ACTIVE_ROUTE
                if route.stable_key in active_keys
                else L2WorkPriority.CANDIDATE_ROUTE,
                reason,
                observed_ns,
                latency_ms,
            )
        )
    observations.extend(
        CandidateL2RouteObservation(
            base,
            Venue(long_venue),
            Venue(short_venue),
            "",
            "",
            L2WorkPriority.ACTIVE_ROUTE,
            ReasonCode.VENUE_OUTAGE
            if Venue(long_venue) in unavailable_venues or Venue(short_venue) in unavailable_venues
            else ReasonCode.CONTRACT_METADATA_UNKNOWN,
            0,
            None,
        )
        for base, long_venue, short_venue in plan.missing_active_routes
    )
    return tuple(observations)


def _route_reason(
    long: CandidateL2BookState | None,
    short: CandidateL2BookState | None,
    *,
    decision_monotonic_ns: int,
    maximum_age_ms: int,
) -> ReasonCode:
    for state in (long, short):
        if state is not None and not state.quality.accepted:
            return state.quality.reason
    for state in (long, short):
        if state is None or state.book is None:
            return ReasonCode.BOOK_EMPTY
        age_ns = decision_monotonic_ns - state.book.received_monotonic_ns
        if age_ns < 0:
            return ReasonCode.BOOK_UNSYNCHRONISED
        if age_ns > maximum_age_ms * 1_000_000:
            return ReasonCode.BOOK_STALE
    return ReasonCode.QUOTE_READY
