from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, cast, runtime_checkable

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

MAX_PRIVATE_RECONCILIATION_SYMBOLS_PER_VENUE = 10
_PrivateRequestKey = tuple[Venue, str, str]
_PRIVATE_REQUEST_TASKS: dict[_PrivateRequestKey, tuple[int, asyncio.Task[object]]] = {}


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


@runtime_checkable
class ReconcilingPrivateStateAdapter(Protocol):
    async def reconcile_active_snapshot(self, trigger: str) -> PrivateActiveSnapshot: ...


@runtime_checkable
class PrivateEventWatermarkAdapter(Protocol):
    def current_private_event_watermark(self) -> int: ...


def _consume_background_task[T](task: asyncio.Task[T]) -> None:
    with suppress(asyncio.CancelledError, Exception):
        task.exception()


def _owned_private_request[T](
    key: _PrivateRequestKey,
    adapter: PrivateStateAdapter,
    factory: Callable[[], Coroutine[Any, Any, T]],
) -> asyncio.Task[T]:
    """Return the one process-owned in-flight request for a private endpoint key."""
    loop = asyncio.get_running_loop()
    existing = _PRIVATE_REQUEST_TASKS.get(key)
    if existing is not None:
        owner, task = existing
        if task.done():
            _consume_background_task(task)
            _PRIVATE_REQUEST_TASKS.pop(key, None)
        elif owner != id(adapter) or task.get_loop() is not loop:
            raise RuntimeError(f"prior private request remains active: {key}")
        else:
            return cast(asyncio.Task[T], task)

    if key[1] == "recent":
        outstanding_recent = sum(
            1
            for (venue, operation, _), (_, task) in _PRIVATE_REQUEST_TASKS.items()
            if venue == key[0] and operation == "recent" and not task.done()
        )
        if outstanding_recent >= MAX_PRIVATE_RECONCILIATION_SYMBOLS_PER_VENUE:
            raise RuntimeError(f"private reconciliation request budget exhausted: {key[0].value}")

    created: asyncio.Task[T] = asyncio.create_task(factory())
    owned = cast(asyncio.Task[object], created)
    _PRIVATE_REQUEST_TASKS[key] = (id(adapter), owned)

    def release(completed: asyncio.Task[T]) -> None:
        current = _PRIVATE_REQUEST_TASKS.get(key)
        if current is not None and current[1] is completed:
            _PRIVATE_REQUEST_TASKS.pop(key, None)
        _consume_background_task(completed)

    created.add_done_callback(release)
    return created


async def shutdown_private_requests(
    adapters: Mapping[Venue, PrivateStateAdapter],
    *,
    timeout_seconds: float = 1.0,
) -> None:
    """Cancel and boundedly drain every process-owned request for these adapters."""
    if timeout_seconds <= 0:
        raise ValueError("private request shutdown timeout must be positive")
    owner_ids = {id(adapter) for adapter in adapters.values()}
    tasks = tuple(
        task
        for owner, task in _PRIVATE_REQUEST_TASKS.values()
        if owner in owner_ids and not task.done()
    )
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    done, pending = await asyncio.wait(
        tasks,
        timeout=timeout_seconds,
        return_when=asyncio.ALL_COMPLETED,
    )
    for task in done:
        _consume_background_task(task)
    if pending:
        venues = sorted(
            {
                venue.value
                for (venue, _, _), (owner, task) in _PRIVATE_REQUEST_TASKS.items()
                if owner in owner_ids and task in pending
            }
        )
        raise RuntimeError("private request shutdown deadline exceeded: " + ",".join(venues))


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
    account_wide: bool = False
    event_watermark: int = 0


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
    failure_reason: ReasonCode | None = None


def flat_barrier_failure_reason(result: FlatBarrierResult) -> ReasonCode:
    """Return the stable fail-closed reason for an unverified terminal barrier."""
    if result.verified:
        raise ValueError("a verified flat barrier has no failure reason")
    if result.failure_reason is not None:
        return result.failure_reason
    if result.report.status == ReconciliationStatus.UNKNOWN or not result.report.snapshots_complete:
        return ReasonCode.FLAT_BARRIER_PRIVATE_STATE_UNKNOWN
    if result.report.flat_verified and result.consecutive_snapshots == 0:
        return ReasonCode.FLAT_BARRIER_EVENT_RACE
    return ReasonCode.FLAT_BARRIER_TIMEOUT


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
        tuple(
            sorted((venue.value, state.event_watermark) for venue, state in report.states.items())
        ),
    )


async def combined_event_watermark(
    adapters: Mapping[Venue, PrivateStateAdapter],
    journal_watermark_factory: Callable[[], Awaitable[int]],
) -> int:
    watermark = await journal_watermark_factory()
    for adapter in adapters.values():
        if isinstance(adapter, PrivateEventWatermarkAdapter):
            private_watermark = adapter.current_private_event_watermark()
            if private_watermark < 0:
                raise ValueError("private event watermark cannot be negative")
            watermark += private_watermark
    return watermark


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
    last_watermark = -1
    last_report: ReconciliationReport | None = None
    private_unknown_report: ReconciliationReport | None = None
    event_race_observed = False

    async def before_deadline[T](factory: Callable[[], Awaitable[T]]) -> T:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError
        return await asyncio.wait_for(factory(), timeout=remaining)

    def timed_out_result() -> FlatBarrierResult:
        report = private_unknown_report or last_report or _unavailable_private_report()
        if private_unknown_report is not None or last_report is None:
            reason = ReasonCode.FLAT_BARRIER_PRIVATE_STATE_UNKNOWN
        elif event_race_observed:
            reason = ReasonCode.FLAT_BARRIER_EVENT_RACE
        else:
            reason = ReasonCode.FLAT_BARRIER_TIMEOUT
        return FlatBarrierResult(False, report, consecutive, last_watermark, True, reason)

    while True:
        try:
            before = await before_deadline(watermark_factory)
            report = await before_deadline(report_factory)
            last_report = report
            after = await before_deadline(watermark_factory)
        except TimeoutError:
            return timed_out_result()
        last_watermark = after
        now = loop.time()
        signature = _flat_signature(report, after)
        if now >= deadline:
            return timed_out_result()
        private_unknown = (
            report.status == ReconciliationStatus.UNKNOWN or not report.snapshots_complete
        )
        if private_unknown and private_unknown_report is None:
            private_unknown_report = report
        if before != after:
            event_race_observed = True
        if private_unknown_report is None and report.flat_verified and before == after:
            if signature == previous_signature:
                consecutive += 1
            else:
                previous_signature = signature
                consecutive = 1
                stable_since = now
            ready = (
                consecutive >= policy.consecutive_snapshots
                and stable_since is not None
                and now - stable_since >= policy.quiet_period_seconds
            )
            if ready:
                try:
                    final_watermark = await before_deadline(watermark_factory)
                except TimeoutError:
                    return timed_out_result()
                if loop.time() >= deadline:
                    last_watermark = final_watermark
                    return timed_out_result()
                if final_watermark == after:
                    return FlatBarrierResult(True, report, consecutive, after, False)
                event_race_observed = True
                previous_signature = None
                stable_since = None
                consecutive = 0
                last_watermark = final_watermark
        else:
            previous_signature = None
            stable_since = None
            consecutive = 0
        await asyncio.sleep(min(policy.poll_interval_seconds, max(0, deadline - now)))


def _unavailable_private_report() -> ReconciliationReport:
    return ReconciliationReport(
        status=ReconciliationStatus.UNKNOWN,
        states={},
        discrepancies=("FLAT_BARRIER_PRIVATE_STATE_UNAVAILABLE",),
        unknown_client_order_ids=(),
        open_bot_order_count=0,
        open_position_count=0,
        actual_signed_positions={},
        expected_signed_positions={},
        residual_delta=Decimal(0),
        flat_verified=False,
        raw_open_order_count=0,
        raw_nonzero_position_count=0,
        unknown_active_record_count=1,
        snapshots_complete=False,
    )


async def collect_private_states(
    adapters: Mapping[Venue, PrivateStateAdapter],
    instruments: Mapping[Venue, Instrument],
    *,
    timeout_seconds: float = 2.0,
    reconciliation_trigger: str | None = None,
    recent_instruments: Mapping[Venue, Sequence[Instrument]] | None = None,
) -> dict[Venue, VenuePrivateState]:
    if timeout_seconds <= 0:
        raise ValueError("private state collection timeout must be positive")

    async def collect(venue: Venue) -> VenuePrivateState:
        adapter = adapters[venue]
        instrument = instruments[venue]

        async def fetch_active() -> PrivateActiveSnapshot:
            if reconciliation_trigger is not None and isinstance(
                adapter, ReconcilingPrivateStateAdapter
            ):
                return await adapter.reconcile_active_snapshot(
                    f"{reconciliation_trigger}:{venue.value}"
                )
            return await adapter.fetch_active_snapshot()

        def recent_factory(
            recent_instrument: Instrument,
        ) -> Callable[[], Coroutine[Any, Any, tuple[PrivateOrder, ...]]]:
            async def fetch_recent() -> tuple[PrivateOrder, ...]:
                return await adapter.fetch_closed_orders(recent_instrument)

            return fetch_recent

        try:
            venue_recent_instruments = tuple(
                {
                    item.symbol: item
                    for item in (
                        recent_instruments.get(venue, (instrument,))
                        if recent_instruments is not None
                        else (instrument,)
                    )
                }.values()
            ) or (instrument,)
            if len(venue_recent_instruments) > MAX_PRIVATE_RECONCILIATION_SYMBOLS_PER_VENUE:
                raise ValueError("private reconciliation symbol budget exceeded")
            if any(item.venue != venue for item in venue_recent_instruments):
                raise ValueError("private reconciliation instrument venue mismatch")
            account_task = _owned_private_request(
                (venue, "account", instrument.symbol),
                adapter,
                lambda: adapter.fetch_account(instrument),
            )
            active_task = _owned_private_request(
                (
                    venue,
                    "active-reconcile" if reconciliation_trigger is not None else "active",
                    "account-wide",
                ),
                adapter,
                fetch_active,
            )
            recent_orders_tasks = tuple(
                _owned_private_request(
                    (venue, "recent", recent_instrument.symbol),
                    adapter,
                    recent_factory(recent_instrument),
                )
                for recent_instrument in venue_recent_instruments
            )
            fee_task = _owned_private_request(
                (venue, "fee", instrument.symbol),
                adapter,
                lambda: adapter.fetch_trading_fee(instrument),
            )
            tasks = (account_task, active_task, *recent_orders_tasks, fee_task)
            _, pending = await asyncio.wait(
                tasks,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )
            if pending:
                raise TimeoutError
            task_errors = tuple(error for task in tasks if (error := task.exception()) is not None)
            if task_errors:
                raise task_errors[0]
            account = account_task.result()
            active = active_task.result()
            recent_orders_by_key = {
                (
                    order.client_order_id,
                    order.order_id,
                    order.status,
                    order.filled_base_quantity,
                    order.observed_at,
                ): order
                for task in recent_orders_tasks
                for order in task.result()
            }
            recent_orders = tuple(recent_orders_by_key.values())
            fee = fee_task.result()
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
                active.account_wide,
                active.event_watermark,
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
                False,
            )

    results = await asyncio.gather(*(collect(venue) for venue in adapters))
    return {state.venue: state for state in results}


def reconcile_private_states(
    action: LiveJournalAction | Sequence[LiveJournalAction] | None,
    states: dict[Venue, VenuePrivateState],
    known_client_order_ids: set[str],
    required_venues: set[Venue],
) -> ReconciliationReport:
    actions = (
        ()
        if action is None
        else (action,)
        if isinstance(action, LiveJournalAction)
        else tuple(action)
    )
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
            not state.account_wide
            or state.completeness != SnapshotCompleteness.COMPLETE
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
    if actions:
        for current_action in actions:
            initial_positions = current_action.risk_reservation.get("initial_signed_positions", {})
            if isinstance(initial_positions, dict):
                for venue, quantity in initial_positions.items():
                    parsed_venue = Venue(str(venue))
                    expected_positions[parsed_venue] = expected_positions.get(
                        parsed_venue, Decimal(0)
                    ) + Decimal(str(quantity))
        for leg in (leg for current_action in actions for leg in current_action.legs):
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
        state.account_wide
        and state.completeness == SnapshotCompleteness.COMPLETE
        and state.raw_open_order_count == len(state.open_orders)
        and state.raw_nonzero_position_count == len(state.positions)
        for state in states.values()
    )
    allowed_symbols: dict[Venue, set[str]] = {}
    if actions:
        for leg in (leg for current_action in actions for leg in current_action.legs):
            allowed_symbols.setdefault(leg.venue, set()).add(leg.symbol)
    for state in states.values():
        for position in state.positions:
            if not actions:
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
