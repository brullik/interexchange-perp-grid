from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from interexchange_perp_grid.client_ids import is_bot_client_order_id
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import LiveJournalAction
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateActiveSnapshot,
    PrivateOrder,
    SnapshotCompleteness,
    UnknownActiveRecord,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import (
    RiskBook,
    RiskDecision,
    RiskLimits,
    RiskRequest,
    VenueProjection,
)
from interexchange_perp_grid.strategy import DirectedRouteKey


class ReconciliationStatus(StrEnum):
    CONSISTENT = "CONSISTENT"
    INCONSISTENT = "INCONSISTENT"
    UNKNOWN = "UNKNOWN"


class PrivateStateAdapter(Protocol):
    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot: ...

    async def fetch_all_open_orders(self) -> tuple[PrivateOrder, ...]: ...

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]: ...

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]: ...

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot: ...

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal | None: ...


@dataclass(frozen=True, slots=True)
class VenuePrivateState:
    venue: Venue
    account: AccountSnapshot | None
    open_orders: tuple[PrivateOrder, ...]
    recent_orders: tuple[PrivateOrder, ...]
    positions: tuple[PositionSnapshot, ...]
    taker_fee_rate: Decimal | None
    error: str | None = None
    raw_open_order_count: int = 0
    raw_nonzero_position_count: int = 0
    unknown_active_records: tuple[UnknownActiveRecord, ...] = ()
    completeness: SnapshotCompleteness = SnapshotCompleteness.COMPLETE


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    status: ReconciliationStatus
    states: dict[Venue, VenuePrivateState]
    discrepancies: tuple[str, ...]
    unknown_client_order_ids: tuple[str, ...]
    open_bot_order_count: int
    open_position_count: int
    actual_signed_positions: dict[Venue, Decimal]
    expected_signed_positions: dict[Venue, Decimal]
    residual_delta: Decimal
    flat_verified: bool
    raw_open_order_count: int
    raw_nonzero_position_count: int
    unknown_active_record_count: int
    snapshots_complete: bool

    @property
    def consistent(self) -> bool:
        return self.status == ReconciliationStatus.CONSISTENT


@dataclass(frozen=True, slots=True)
class FlatBarrierPolicy:
    consecutive_snapshots: int = 2
    quiet_period_seconds: float = 0.05
    poll_interval_seconds: float = 0.01
    timeout_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.consecutive_snapshots < 2:
            raise ValueError("flat barrier requires at least two snapshots")
        if self.quiet_period_seconds < 0 or self.poll_interval_seconds <= 0:
            raise ValueError("invalid flat barrier timing")
        if self.timeout_seconds <= 0 or self.timeout_seconds < self.quiet_period_seconds:
            raise ValueError("flat barrier timeout must cover the quiet period")


@dataclass(frozen=True, slots=True)
class FlatBarrierResult:
    verified: bool
    report: ReconciliationReport
    consecutive_snapshots: int
    event_watermark: int
    timed_out: bool


def _flat_signature(report: ReconciliationReport, watermark: int) -> tuple[object, ...]:
    return (
        watermark,
        report.status.value,
        report.raw_open_order_count,
        report.raw_nonzero_position_count,
        report.unknown_active_record_count,
        report.open_bot_order_count,
        report.open_position_count,
        tuple(
            sorted(
                (venue.value, str(value)) for venue, value in report.actual_signed_positions.items()
            )
        ),
        report.discrepancies,
        report.unknown_client_order_ids,
    )


async def wait_for_stable_flat(
    report_factory: Callable[[], Awaitable[ReconciliationReport]],
    watermark_factory: Callable[[], Awaitable[int]],
    policy: FlatBarrierPolicy,
) -> FlatBarrierResult:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + policy.timeout_seconds
    stable_since: float | None = None
    previous_signature: tuple[object, ...] | None = None
    consecutive = 0
    last_report: ReconciliationReport | None = None
    last_watermark = -1
    while True:
        before = await watermark_factory()
        report = await report_factory()
        after = await watermark_factory()
        last_report = report
        last_watermark = after
        now = loop.time()
        signature = _flat_signature(report, after)
        if report.flat_verified and before == after:
            if signature == previous_signature:
                consecutive += 1
            else:
                previous_signature = signature
                consecutive = 1
                stable_since = now
            if (
                consecutive >= policy.consecutive_snapshots
                and stable_since is not None
                and now - stable_since >= policy.quiet_period_seconds
                and await watermark_factory() == after
            ):
                return FlatBarrierResult(True, report, consecutive, after, False)
        else:
            previous_signature = None
            stable_since = None
            consecutive = 0
        if now >= deadline:
            assert last_report is not None
            return FlatBarrierResult(False, last_report, consecutive, last_watermark, True)
        await asyncio.sleep(min(policy.poll_interval_seconds, max(0, deadline - now)))


async def collect_private_states(
    adapters: Mapping[Venue, PrivateStateAdapter],
    instruments: Mapping[Venue, Instrument],
) -> dict[Venue, VenuePrivateState]:
    async def collect(venue: Venue) -> VenuePrivateState:
        adapter = adapters[venue]
        instrument = instruments[venue]
        try:
            account, active, recent_orders, fee = await asyncio.gather(
                adapter.fetch_account(instrument),
                adapter.fetch_active_snapshot(),
                adapter.fetch_closed_orders(instrument),
                adapter.fetch_trading_fee(instrument),
            )
            return VenuePrivateState(
                venue,
                account,
                active.open_orders,
                recent_orders,
                active.positions,
                fee,
                None,
                active.raw_open_order_count,
                active.raw_nonzero_position_count,
                active.unknown_active_records,
                active.completeness,
            )
        except Exception as error:
            return VenuePrivateState(
                venue,
                None,
                (),
                (),
                (),
                None,
                f"{type(error).__name__}:{error}",
                0,
                0,
                (),
                SnapshotCompleteness.UNKNOWN,
            )

    results = await asyncio.gather(*(collect(venue) for venue in adapters))
    return {state.venue: state for state in results}


def reconcile_private_states(
    action: LiveJournalAction | None,
    states: dict[Venue, VenuePrivateState],
    known_client_order_ids: set[str],
    required_venues: set[Venue],
) -> ReconciliationReport:
    discrepancies: list[str] = []
    unknown: list[str] = []
    missing = required_venues - set(states)
    discrepancies.extend(f"{venue.value}:STATE_MISSING" for venue in sorted(missing))
    if any(state.error is not None for state in states.values()):
        unknown.extend(
            f"{venue.value}:PRIVATE_STATE_ERROR"
            for venue, state in states.items()
            if state.error is not None
        )
    for venue in required_venues & set(states):
        state = states[venue]
        if state.account is None:
            unknown.append(f"{venue.value}:ACCOUNT_UNKNOWN")
        if state.taker_fee_rate is None:
            unknown.append(f"{venue.value}:FEE_UNKNOWN")
        if (
            state.completeness != SnapshotCompleteness.COMPLETE
            or state.raw_open_order_count != len(state.open_orders)
            or state.raw_nonzero_position_count != len(state.positions)
        ):
            unknown.append(f"{venue.value}:PRIVATE_SNAPSHOT_INCOMPLETE")
        unknown.extend(
            f"{venue.value}:UNKNOWN_ACTIVE_RECORD:{record.kind}:{record.reason}"
            for record in state.unknown_active_records
        )

    all_open = tuple(order for state in states.values() for order in state.open_orders)
    all_recent = tuple(order for state in states.values() for order in state.recent_orders)
    for order in all_open:
        if not is_bot_client_order_id(order.client_order_id):
            discrepancies.append(f"{order.venue.value}:NON_BOT_OPEN_ORDER")
        elif order.client_order_id not in known_client_order_ids:
            unknown.append(order.client_order_id)
    recent_by_client: dict[str, list[PrivateOrder]] = {}
    for order in (*all_open, *all_recent):
        recent_by_client.setdefault(order.client_order_id, []).append(order)

    expected_positions = {venue: Decimal(0) for venue in required_venues}
    if action is not None:
        initial_positions = action.risk_reservation.get("initial_signed_positions", {})
        if isinstance(initial_positions, dict):
            for venue, quantity in initial_positions.items():
                parsed_venue = Venue(str(venue))
                expected_positions[parsed_venue] = Decimal(str(quantity))
        for leg in action.legs:
            signed = leg.filled_base_quantity if leg.side == Side.BUY else -leg.filled_base_quantity
            expected_positions[leg.venue] = expected_positions.get(leg.venue, Decimal(0)) + signed
            if not leg.submit_attempted:
                continue
            observed = recent_by_client.get(leg.client_order_id, [])
            if not observed:
                unknown.append(leg.client_order_id)
                continue
            if len(observed) > 1 and len({order.order_id for order in observed}) > 1:
                discrepancies.append(f"{leg.client_order_id}:MULTIPLE_EXCHANGE_ORDERS")
            latest = max(observed, key=lambda order: order.observed_at)
            if latest.venue != leg.venue or latest.symbol != leg.symbol or latest.side != leg.side:
                discrepancies.append(f"{leg.client_order_id}:IDENTITY_MISMATCH")
            if latest.status.value == "UNKNOWN":
                unknown.append(leg.client_order_id)
            if latest.filled_base_quantity != leg.filled_base_quantity:
                discrepancies.append(f"{leg.client_order_id}:FILL_MISMATCH")
    elif any(is_bot_client_order_id(order.client_order_id) for order in all_open):
        unknown.append("OPEN_BOT_ORDER_WITHOUT_ACTIVE_JOURNAL")

    actual_positions = {venue: Decimal(0) for venue in required_venues}
    raw_open_order_count = sum(state.raw_open_order_count for state in states.values())
    open_position_count = sum(state.raw_nonzero_position_count for state in states.values())
    unknown_active_count = sum(len(state.unknown_active_records) for state in states.values())
    snapshots_complete = all(
        state.completeness == SnapshotCompleteness.COMPLETE
        and state.raw_open_order_count == len(state.open_orders)
        and state.raw_nonzero_position_count == len(state.positions)
        for state in states.values()
    )
    allowed_symbols: dict[Venue, set[str]] = {}
    if action is not None:
        for leg in action.legs:
            allowed_symbols.setdefault(leg.venue, set()).add(leg.symbol)
    for state in states.values():
        for position in state.positions:
            if action is None:
                discrepancies.append(f"{position.venue.value}:UNEXPECTED_OPEN_POSITION")
            elif position.symbol not in allowed_symbols.get(position.venue, set()):
                discrepancies.append(f"{position.venue.value}:NON_ROUTE_POSITION")
            signed = (
                position.base_quantity if position.side == Side.BUY else -position.base_quantity
            )
            actual_positions[position.venue] = (
                actual_positions.get(position.venue, Decimal(0)) + signed
            )
    for venue in required_venues:
        if actual_positions.get(venue, Decimal(0)) != expected_positions.get(venue, Decimal(0)):
            discrepancies.append(f"{venue.value}:POSITION_MISMATCH")

    residual = abs(sum(actual_positions.values(), Decimal(0)))
    open_bot_count = sum(is_bot_client_order_id(order.client_order_id) for order in all_open)
    if unknown:
        status = ReconciliationStatus.UNKNOWN
    elif discrepancies:
        status = ReconciliationStatus.INCONSISTENT
    else:
        status = ReconciliationStatus.CONSISTENT
    flat = (
        status == ReconciliationStatus.CONSISTENT
        and open_bot_count == 0
        and raw_open_order_count == 0
        and open_position_count == 0
        and unknown_active_count == 0
        and snapshots_complete
        and not unknown
        and all(value == 0 for value in actual_positions.values())
        and residual == 0
    )
    return ReconciliationReport(
        status=status,
        states=states,
        discrepancies=tuple(sorted(set(discrepancies))),
        unknown_client_order_ids=tuple(sorted(set(unknown))),
        open_bot_order_count=open_bot_count,
        open_position_count=open_position_count,
        actual_signed_positions=actual_positions,
        expected_signed_positions=expected_positions,
        residual_delta=residual,
        flat_verified=flat,
        raw_open_order_count=raw_open_order_count,
        raw_nonzero_position_count=open_position_count,
        unknown_active_record_count=unknown_active_count,
        snapshots_complete=snapshots_complete,
    )


def evaluate_canary_risk_from_private_state(
    route: DirectedRouteKey,
    states: dict[Venue, VenuePrivateState],
    proposed_notional_usdt: Decimal,
    projected_stress_usdt: Decimal,
    *,
    pair_stress_limit_usdt: Decimal,
    portfolio_stress_limit_usdt: Decimal,
    free_margin_floor_ratio: Decimal,
    effective_leverage_cap: Decimal,
    exit_depth_sufficient: bool,
) -> RiskDecision:
    if projected_stress_usdt > pair_stress_limit_usdt:
        return RiskDecision(
            False,
            ReasonCode.PAIR_STRESS_LIMIT,
            {
                "projected_route_stress_usdt": projected_stress_usdt,
                "pair_limit_usdt": pair_stress_limit_usdt,
            },
        )
    projections: list[VenueProjection] = []
    existing_exposure = False
    for venue in (route.long_venue, route.short_venue):
        state = states.get(venue)
        if state is None or state.account is None:
            return RiskDecision(False, ReasonCode.RISK_PREFLIGHT_FAILED, {})
        position_notional = sum(
            (
                position.base_quantity * (position.mark_price or position.entry_price or Decimal(0))
                for position in state.positions
            ),
            Decimal(0),
        )
        order_notional = sum(
            (
                order.requested_base_quantity
                * (order.limit_price or order.average_price or Decimal(0))
                for order in state.open_orders
            ),
            Decimal(0),
        )
        existing_notional = position_notional + order_notional
        existing_exposure = existing_exposure or existing_notional > 0
        projections.append(
            VenueProjection(
                venue=venue,
                equity_usdt=state.account.equity_usdt,
                projected_notional_usdt=existing_notional + proposed_notional_usdt,
                projected_margin_used_usdt=(
                    state.account.equity_usdt
                    - state.account.free_margin_usdt
                    + proposed_notional_usdt / effective_leverage_cap
                ),
                venue_stress_usdt=projected_stress_usdt / Decimal(2),
            )
        )
    book = RiskBook(
        RiskLimits(
            pair_stress_limit_usdt,
            portfolio_stress_limit_usdt,
            1,
            1,
            1,
            free_margin_floor_ratio,
            effective_leverage_cap,
        )
    )
    book.set_execution_block(
        unmatched_exposure=existing_exposure,
        unknown_order_state=any(state.error is not None for state in states.values()),
    )
    return book.reserve(
        RiskRequest(
            reservation_id="live-canary-reservation",
            route_id=route.value,
            base=route.base,
            tranche_id="live-canary-tranche",
            projected_stress_usdt=projected_stress_usdt,
            venues=tuple(projections),
            exit_depth_sufficient=exit_depth_sufficient,
        )
    )
