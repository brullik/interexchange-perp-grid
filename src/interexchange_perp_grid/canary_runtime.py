from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter, PrivateCredentials
from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import (
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_control import (
    LiveControlResult,
    LiveControlService,
    emergency_unlock_valid,
)
from interexchange_perp_grid.live_coordinator import (
    CanaryCloseSignals,
    CanaryCycleResult,
    CanaryExecutionPlan,
    CanaryMonitor,
    CanaryVenueAdapter,
    CloseReason,
    LiveCanaryCoordinator,
    ProtectionProvider,
    first_close_reason,
)
from interexchange_perp_grid.live_economics import (
    EmergencyVenueAssessment,
    LiveEconomicDecision,
    LiveEconomicPolicy,
    evaluate_emergency_venue,
    evaluate_live_entry,
)
from interexchange_perp_grid.live_journal import (
    JournalLeg,
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
    request_payload_hash,
)
from interexchange_perp_grid.live_reconciliation import (
    FlatBarrierPolicy,
    ReconciliationReport,
    VenuePrivateState,
    collect_private_states,
    evaluate_canary_risk_from_private_state,
    reconcile_private_states,
    shutdown_private_requests,
)
from interexchange_perp_grid.market_data import BookRegistry, DataQualityAssessment
from interexchange_perp_grid.private_cache import Wave1PrivateStateSupervisor
from interexchange_perp_grid.private_domain import PrivateCapabilityReport, VenueOrderRequest
from interexchange_perp_grid.private_execution import (
    CanaryAction,
    CanaryPolicy,
    PrivatePreflightInput,
    PrivatePreflightReport,
    protected_ioc_price,
    run_private_preflight,
    translate_protected_order,
)
from interexchange_perp_grid.qualification import (
    load_qualification,
    qualification_is_current,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import RiskDecision
from interexchange_perp_grid.risk_stages import load_locked_risk_stage_table
from interexchange_perp_grid.routes import (
    evaluate_directed_route,
    executable_vwap,
    minimum_common_base_quantity,
)
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.state import (
    RiskStage,
    RuntimeControls,
    initialise_state,
    live_confirmation_valid,
    read_risk_stage,
    read_runtime_controls,
    read_runtime_controls_bounded,
)
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.venue_capabilities import (
    CapabilityState,
    build_venue_capability_matrix,
)

OWNER_CONFIRMATION = "I_ACCEPT_LIVE_CANARY_RISK"
_OPENING_GATE_TASKS: set[asyncio.Task[object]] = set()


def _consume_opening_gate_task(task: asyncio.Task[object]) -> None:
    _OPENING_GATE_TASKS.discard(task)
    if task.cancelled():
        return
    with contextlib.suppress(Exception):
        task.exception()


def _cancel_opening_gate_tasks() -> None:
    for task in tuple(_OPENING_GATE_TASKS):
        if not task.done():
            task.cancel()


async def _shutdown_opening_gate_tasks(timeout_seconds: float = 1.0) -> None:
    pending = tuple(task for task in _OPENING_GATE_TASKS if not task.done())
    for task in pending:
        task.cancel()
    if pending:
        _, still_pending = await asyncio.wait(pending, timeout=timeout_seconds)
        if still_pending:
            raise RuntimeError("opening capability gate transport did not terminate")
    for task in tuple(_OPENING_GATE_TASKS):
        if task.done():
            _consume_opening_gate_task(task)


async def _await_owned_opening_operation[OpeningResult](
    operation: Coroutine[Any, Any, OpeningResult],
    *,
    name: str,
    timeout_seconds: float = 1.0,
) -> OpeningResult:
    task = asyncio.create_task(operation, name=name)
    owned_task = cast(asyncio.Task[object], task)
    _OPENING_GATE_TASKS.add(owned_task)
    owned_task.add_done_callback(_consume_opening_gate_task)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_seconds)
    except TimeoutError:
        task.cancel()
        raise


async def _start_private_state_supervisor(
    adapters: dict[Venue, CcxtPrivateAdapter],
    state_path: Path,
) -> tuple[
    Wave1PrivateStateSupervisor,
    asyncio.Event,
    asyncio.Task[None],
    dict[Venue, CanaryVenueAdapter],
]:
    supervisor = Wave1PrivateStateSupervisor(adapters, state_path=state_path)
    await supervisor.startup()
    stop_event = asyncio.Event()
    task = asyncio.create_task(supervisor.run(stop_event))
    cached = cast(dict[Venue, CanaryVenueAdapter], supervisor.cached_adapters())
    return supervisor, stop_event, task, cached


async def _stop_private_state_supervisor(
    stop_event: asyncio.Event | None,
    task: asyncio.Task[None] | None,
) -> None:
    if stop_event is None or task is None:
        return
    stop_event.set()
    await asyncio.gather(task, return_exceptions=True)


def _flat_barrier_policy(settings: Settings) -> FlatBarrierPolicy:
    return FlatBarrierPolicy(
        consecutive_snapshots=settings.live.flat_barrier_consecutive_snapshots,
        quiet_period_seconds=float(settings.live.flat_barrier_quiet_period_seconds),
        poll_interval_seconds=float(settings.live.flat_barrier_poll_interval_seconds),
        timeout_seconds=float(settings.live.flat_barrier_timeout_seconds),
    )


@dataclass(frozen=True, slots=True)
class CanaryRunEvidence:
    submitted: bool
    success: bool
    reason: ReasonCode | None
    route: str | None
    quantity: Decimal | None
    orders_sent: int
    hedged: bool
    residual_delta: Decimal
    recovery_action: str | None
    terminal_state: LiveActionState | None
    economic_decision: LiveEconomicDecision | None
    preflights: tuple[PrivatePreflightReport, ...]
    reconciliation: ReconciliationReport | None
    owner_instruction: str | None
    emergency_venue: EmergencyVenueAssessment | None = None


@dataclass(frozen=True, slots=True)
class _OpeningGateSnapshot:
    public_reports: tuple[CapabilityReport, ...]
    private_reports: tuple[PrivateCapabilityReport, ...]
    private_states: dict[Venue, VenuePrivateState]
    books: dict[Venue, OrderBookSnapshot]
    quality: dict[Venue, DataQualityAssessment]
    funding: tuple[FundingSnapshot, ...]
    controls: RuntimeControls
    action: LiveJournalAction | None
    known_client_ids: set[str]


class PublicProtectionProvider(ProtectionProvider):
    def __init__(
        self,
        settings: Settings,
        adapters: dict[Venue, CcxtProAdapter],
        instruments: dict[Venue, Instrument],
    ) -> None:
        self._settings = settings
        self._adapters = adapters
        self._instruments = instruments
        self._registry = BookRegistry()

    async def price(
        self,
        venue: Venue,
        side: Side,
        quantity: Decimal,
        purpose: OrderPurpose,
    ) -> Decimal:
        instrument = self._instruments[venue]
        book = await self._adapters[venue].watch_order_book(instrument)
        quality = self._registry.accept(
            book,
            max_age_ms=self._settings.market_data.max_l2_age_ms,
            max_clock_skew_ms=self._settings.market_data.max_clock_skew_ms,
        )
        if not quality.accepted:
            raise RuntimeError(quality.reason.value)
        consumed = executable_vwap(book.asks if side == Side.BUY else book.bids, quantity)
        if consumed is None:
            raise RuntimeError(ReasonCode.DEPTH_INSUFFICIENT.value)
        slippage = (
            self._settings.live.canary_entry_slippage_cap_bps
            if purpose == OrderPurpose.NORMAL_OPEN
            else self._settings.live.canary_close_slippage_cap_bps
        )
        return protected_ioc_price(
            side,
            consumed.marginal_price,
            instrument.price_tick,
            slippage,
        )


class RuntimeCanaryMonitor(CanaryMonitor):
    def __init__(
        self,
        settings: Settings,
        route: DirectedRouteKey,
        quantity: Decimal,
        target_exit_spread_bps: Decimal,
        public_adapters: dict[Venue, CcxtProAdapter],
        private_adapters: dict[Venue, CanaryVenueAdapter],
        instruments: dict[Venue, Instrument],
        initial_funding: dict[Venue, FundingSnapshot],
        state_path: Path,
    ) -> None:
        self._settings = settings
        self._route = route
        self._quantity = quantity
        self._target_exit_spread_bps = target_exit_spread_bps
        self._public = public_adapters
        self._private = private_adapters
        self._instruments = instruments
        self._initial_funding = initial_funding
        self._state_path = state_path
        self._registry = BookRegistry()

    async def wait_for_close(self, timeout_seconds: int) -> CloseReason:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            controls = await read_runtime_controls(self._state_path)
            if controls.killed:
                return CloseReason.EMERGENCY
            if controls.paused:
                return CloseReason.OPERATOR_CLOSE
            try:
                books = await asyncio.gather(
                    *(
                        self._public[venue].watch_order_book(self._instruments[venue])
                        for venue in (self._route.long_venue, self._route.short_venue)
                    )
                )
                book_by_venue = {book.venue: book for book in books}
                quality = tuple(
                    self._registry.accept(
                        book,
                        max_age_ms=self._settings.market_data.max_l2_age_ms,
                        max_clock_skew_ms=self._settings.market_data.max_clock_skew_ms,
                    )
                    for book in books
                )
                states, funding = await asyncio.gather(
                    collect_private_states(self._private, self._instruments),
                    self._funding(),
                )
            except Exception:
                return CloseReason.STALE_DATA
            signals = CanaryCloseSignals(
                target_converged=self._target_converged(book_by_venue),
                risk_deteriorated=_private_risk_deteriorated(self._settings, states),
                funding_deteriorated=self._funding_deteriorated(funding),
                public_or_private_data_stale=not all(item.accepted for item in quality),
            )
            reason = first_close_reason(signals)
            if reason is not None:
                return reason
            await asyncio.sleep(
                float(min(Decimal(1), max(Decimal("0.01"), Decimal(timeout_seconds) / 100)))
            )
        return CloseReason.CANARY_TIMEOUT

    async def _funding(self) -> dict[Venue, FundingSnapshot]:
        venues = (self._route.long_venue, self._route.short_venue)
        values = await asyncio.gather(
            *(self._public[venue].fetch_funding(self._instruments[venue]) for venue in venues)
        )
        return {snapshot.venue: snapshot for snapshot in values}

    def _target_converged(self, books: dict[Venue, OrderBookSnapshot]) -> bool:
        long_exit = executable_vwap(
            books[self._route.long_venue].bids,
            self._quantity,
        )
        short_exit = executable_vwap(
            books[self._route.short_venue].asks,
            self._quantity,
        )
        if long_exit is None or short_exit is None:
            return False
        spread_bps = (short_exit.price - long_exit.price) / long_exit.price * Decimal(10_000)
        return spread_bps <= self._target_exit_spread_bps

    def _funding_deteriorated(self, current: dict[Venue, FundingSnapshot]) -> bool:
        initial_long = self._initial_funding[self._route.long_venue].rate
        initial_short = self._initial_funding[self._route.short_venue].rate
        current_long = current[self._route.long_venue].rate
        current_short = current[self._route.short_venue].rate
        if None in {initial_long, initial_short, current_long, current_short}:
            return True
        assert initial_long is not None
        assert initial_short is not None
        assert current_long is not None
        assert current_short is not None
        initial_cost = initial_long - initial_short
        current_cost = current_long - current_short
        return current_cost > max(Decimal("0.0001"), initial_cost * Decimal(2))


def _private_risk_deteriorated(
    settings: Settings,
    states: dict[Venue, VenuePrivateState],
) -> bool:
    for state in states.values():
        if state.error is not None or state.account is None or state.account.equity_usdt <= 0:
            return True
        account = state.account
        position_notional = sum(
            (
                position.base_quantity * (position.mark_price or position.entry_price or Decimal(0))
                for position in state.positions
            ),
            Decimal(0),
        )
        if (
            state.open_orders
            or account.free_margin_usdt / account.equity_usdt
            < settings.live.canary_free_margin_floor_ratio
            or position_notional / account.equity_usdt > settings.live.canary_effective_leverage_cap
        ):
            return True
    return False


class ImmediateRecoveryCloseMonitor(CanaryMonitor):
    """A restarted action is reduced immediately instead of reopening its holding window."""

    async def wait_for_close(self, timeout_seconds: int) -> CloseReason:
        del timeout_seconds
        return CloseReason.EMERGENCY


class OnDemandLiveControlPlane:
    """Account-wide Telegram live control that opens private transports only per command."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def snapshot(self) -> dict[str, object]:
        return cast(dict[str, object], await self._invoke("snapshot"))

    async def close_all_live(self) -> LiveControlResult:
        return cast(LiveControlResult, await self._invoke("close_all_live"))

    async def cancel_all_live(self) -> LiveControlResult:
        return cast(LiveControlResult, await self._invoke("cancel_all_live"))

    async def emergency_flatten(self) -> LiveControlResult:
        return cast(LiveControlResult, await self._invoke("emergency_flatten"))

    async def kill(self) -> LiveControlResult:
        return cast(LiveControlResult, await self._invoke("kill"))

    async def _invoke(self, operation: str) -> object:
        state_path = Path(self._settings.storage.sqlite_path)
        await initialise_state(state_path)
        journal = LiveOrderJournal(state_path)
        await journal.initialise()
        active_actions = await journal.active_actions()
        active = active_actions[0] if len(active_actions) == 1 else None
        venues = {Venue(value) for value in self._settings.venues.wave1_public}
        private: dict[Venue, CcxtPrivateAdapter] = {}
        cached_private: dict[Venue, CanaryVenueAdapter] = {}
        private_stop: asyncio.Event | None = None
        private_task: asyncio.Task[None] | None = None
        try:
            for venue in venues:
                private[venue] = CcxtPrivateAdapter(
                    venue,
                    PrivateCredentials.from_environment(venue),
                )
            discovered = await asyncio.gather(
                *(adapter.list_instruments() for adapter in private.values())
            )
            registry = {
                (instrument.venue, instrument.symbol): instrument
                for batch in discovered
                for instrument in batch
            }
            _, private_stop, private_task, cached_private = await _start_private_state_supervisor(
                private, state_path
            )
            control = LiveControlService(
                journal,
                cached_private,
                registry,
                active.route if active is not None else None,
                active.qualification_hash if active is not None else None,
                _flat_barrier_policy(self._settings),
            )
            if operation == "snapshot":
                return await control.snapshot()
            if operation == "close_all_live":
                return await control.close_all_live()
            if operation == "cancel_all_live":
                return await control.cancel_all_live()
            if operation == "emergency_flatten":
                return await control.emergency_flatten()
            if operation == "kill":
                return await control.kill()
            raise ValueError(f"unsupported live control operation: {operation}")
        finally:
            await _stop_private_state_supervisor(private_stop, private_task)
            await asyncio.gather(
                *(adapter.close() for adapter in private.values()),
                return_exceptions=True,
            )
            await shutdown_private_requests(cached_private)


def _wave1_emergency_venue(settings: Settings, route: DirectedRouteKey) -> Venue:
    candidates = {
        Venue(value)
        for value in settings.venues.wave1_public
        if value not in {route.long_venue.value, route.short_venue.value}
    }
    if len(candidates) != 1:
        raise ValueError("the exact route must leave one Wave 1 emergency venue")
    return next(iter(candidates))


def _rebuild_active_plan(
    action: LiveJournalAction,
    instruments: dict[Venue, Instrument],
    timeout_seconds: int,
) -> CanaryExecutionPlan:
    def opening_leg(venue: Venue, side: Side, suffix: str) -> JournalLeg:
        stored_ids = action.risk_reservation.get("opening_client_order_ids", {})
        exact_id = (
            str(stored_ids.get(suffix))
            if isinstance(stored_ids, dict) and stored_ids.get(suffix)
            else f"{action.pair_action_id}-{suffix}"
        )
        exact = tuple(leg for leg in action.legs if leg.client_order_id == exact_id)
        if len(exact) == 1:
            return exact[0]
        candidates = tuple(
            leg
            for leg in action.legs
            if leg.venue == venue and leg.side == side and leg.protected_price is not None
        )
        if len(candidates) != 1:
            raise ValueError(f"cannot identify durable {suffix} opening leg")
        return candidates[0]

    long_leg = opening_leg(action.route.long_venue, Side.BUY, "long")
    short_leg = opening_leg(action.route.short_venue, Side.SELL, "short")
    if long_leg.intended_base_quantity != short_leg.intended_base_quantity:
        raise ValueError("durable pair opening quantities differ")

    def rebuild(leg: JournalLeg) -> VenueOrderRequest:
        if leg.protected_price is None:
            raise ValueError("initial canary leg lost its protected price")
        request = translate_protected_order(
            ExecutionIntent(
                leg.client_order_id,
                leg.venue,
                leg.side,
                OrderPurpose.NORMAL_OPEN,
                leg.intended_base_quantity,
                leg.protected_price,
            ),
            instruments[leg.venue],
        )
        if (
            request.symbol != leg.symbol
            or request_payload_hash(request) != leg.request_payload_hash
        ):
            raise ValueError("durable request does not match the exact current translation")
        return request

    return CanaryExecutionPlan(
        pair_action_id=action.pair_action_id,
        route=action.route,
        tranche_id=action.tranche_id,
        quantity=long_leg.intended_base_quantity,
        long_request=rebuild(long_leg),
        short_request=rebuild(short_leg),
        risk_reservation=dict(action.risk_reservation),
        qualification_hash=action.qualification_hash,
        timeout_seconds=timeout_seconds,
    )


async def _coordinate_live_action(
    settings: Settings,
    journal: LiveOrderJournal,
    adapters: dict[Venue, CanaryVenueAdapter],
    private_capability_adapters: dict[Venue, CcxtPrivateAdapter],
    instruments: dict[Venue, Instrument],
    public_adapters: dict[Venue, CcxtProAdapter],
    plan: CanaryExecutionPlan,
    monitor: CanaryMonitor,
    emergency_venue: Venue,
) -> CanaryCycleResult:
    protection = PublicProtectionProvider(settings, public_adapters, instruments)

    async def opening_gate(current_plan: CanaryExecutionPlan) -> bool:
        capability_venues = tuple(sorted(adapters, key=lambda venue: venue.value))

        async def collect_snapshot() -> _OpeningGateSnapshot:
            async def collect_public_reports() -> tuple[CapabilityReport, ...]:
                return tuple(
                    await asyncio.gather(
                        *(
                            public_adapters[venue].probe_public_capabilities()
                            for venue in capability_venues
                        )
                    )
                )

            async def collect_private_reports() -> tuple[PrivateCapabilityReport, ...]:
                return tuple(
                    await asyncio.gather(
                        *(
                            private_capability_adapters[venue].probe_private_capabilities()
                            for venue in capability_venues
                        )
                    )
                )

            async def collect_funding() -> tuple[FundingSnapshot, ...]:
                return tuple(
                    await asyncio.gather(
                        *(
                            public_adapters[venue].fetch_funding(instruments[venue])
                            for venue in capability_venues
                        )
                    )
                )

            results = await asyncio.gather(
                collect_public_reports(),
                collect_private_reports(),
                collect_private_states(
                    adapters,
                    instruments,
                    reconciliation_trigger="PRE_SUBMIT_CAPABILITY_GATE",
                ),
                _fresh_books(settings, public_adapters, instruments),
                collect_funding(),
                read_runtime_controls(Path(settings.storage.sqlite_path)),
                journal.load(current_plan.pair_action_id),
                journal.known_client_order_ids(),
            )
            books, quality = cast(
                tuple[dict[Venue, OrderBookSnapshot], dict[Venue, DataQualityAssessment]],
                results[3],
            )
            return _OpeningGateSnapshot(
                public_reports=cast(tuple[CapabilityReport, ...], results[0]),
                private_reports=cast(tuple[PrivateCapabilityReport, ...], results[1]),
                private_states=cast(dict[Venue, VenuePrivateState], results[2]),
                books=books,
                quality=quality,
                funding=cast(tuple[FundingSnapshot, ...], results[4]),
                controls=cast(RuntimeControls, results[5]),
                action=cast(LiveJournalAction | None, results[6]),
                known_client_ids=cast(set[str], results[7]),
            )

        try:
            snapshot = await _await_owned_opening_operation(
                collect_snapshot(),
                name=f"opening-capability-gate-{current_plan.pair_action_id}",
            )
        except TimeoutError:
            return False
        public_reports = {report.venue: report for report in snapshot.public_reports}
        private_reports = {report.venue: report for report in snapshot.private_reports}
        funding = {item.venue: item for item in snapshot.funding}
        if snapshot.action is None:
            return False
        reconciliation = reconcile_private_states(
            snapshot.action,
            snapshot.private_states,
            snapshot.known_client_ids,
            set(adapters),
        )
        try:
            projected_stress = Decimal(str(current_plan.risk_reservation["projected_stress_usdt"]))
        except (ArithmeticError, KeyError, ValueError):
            return False
        risk_current = (
            projected_stress.is_finite()
            and Decimal(0) <= projected_stress <= settings.live.canary_pair_stressed_loss_limit_usdt
            and not _private_risk_deteriorated(settings, snapshot.private_states)
        )
        preflight_items: list[PrivatePreflightReport] = []
        for venue in capability_venues:
            state = snapshot.private_states[venue]
            if state.account is None:
                continue
            preflight_items.append(
                run_private_preflight(
                    PrivatePreflightInput(
                        capability=private_reports[venue],
                        account=state.account,
                        instrument=instruments[venue],
                        fee_rate=state.taker_fee_rate,
                        funding_known=(
                            funding[venue].rate is not None
                            and funding[venue].next_funding_timestamp_ms is not None
                            and funding[venue].interval is not None
                            and funding[venue].exchange_timestamp_ms is not None
                        ),
                        clock_skew_ms=public_reports[venue].clock_skew_ms,
                        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
                        symbol_available=True,
                        data_quality_passed=(
                            snapshot.quality[venue].accepted and public_reports[venue].public_ready
                        ),
                        reconciliation_passed=(
                            reconciliation.consistent and reconciliation.flat_verified
                        ),
                        risk_passed=risk_current,
                        free_margin_floor_ratio=settings.live.canary_free_margin_floor_ratio,
                    )
                )
            )
        all_accounts_ready = len(preflight_items) == len(capability_venues) and all(
            report.passed for report in preflight_items
        )
        account_preflight_passed = (
            frozenset(capability_venues) if all_accounts_ready else frozenset()
        )
        capability_matrix = build_venue_capability_matrix(
            settings,
            public_reports=public_reports,
            private_reports=private_reports,
            account_preflight_passed=account_preflight_passed,
            now=datetime.now(UTC),
            maximum_report_age_seconds=1,
            require_all_profiles=False,
        )
        executable_books = {
            venue: (
                executable_vwap(snapshot.books[venue].asks, current_plan.quantity),
                executable_vwap(snapshot.books[venue].bids, current_plan.quantity),
            )
            for venue in capability_venues
        }
        if any(buy is None or sell is None for buy, sell in executable_books.values()):
            return False
        long_buy = executable_books[current_plan.long_request.venue][0]
        short_sell = executable_books[current_plan.short_request.venue][1]
        assert long_buy is not None and short_sell is not None
        current_long = protected_ioc_price(
            Side.BUY,
            long_buy.marginal_price,
            instruments[current_plan.long_request.venue].price_tick,
            settings.live.canary_entry_slippage_cap_bps,
        )
        current_short = protected_ioc_price(
            Side.SELL,
            short_sell.marginal_price,
            instruments[current_plan.short_request.venue].price_tick,
            settings.live.canary_entry_slippage_cap_bps,
        )
        return (
            not snapshot.controls.paused
            and not snapshot.controls.killed
            and all(
                capability_matrix.for_venue(venue).live_capability == CapabilityState.QUALIFIED
                for venue in capability_venues
            )
            and _stored_opening_request_is_still_protected(
                current_plan.long_request,
                current_long,
                long_buy.marginal_price,
            )
            and _stored_opening_request_is_still_protected(
                current_plan.short_request,
                current_short,
                short_sell.marginal_price,
            )
        )

    return await LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        protection,
        monitor,
        emergency_venue,
        flat_barrier_policy=_flat_barrier_policy(settings),
        opening_gate=opening_gate,
        final_opening_gate=lambda: _final_opening_controls_allow(settings),
    ).run(plan)


def _stored_opening_request_is_still_protected(
    request: VenueOrderRequest,
    current_protected_price: Decimal,
    current_marginal_price: Decimal | None = None,
) -> bool:
    if request.price is None or not current_protected_price.is_finite():
        return False
    if current_marginal_price is not None and (
        not current_marginal_price.is_finite() or current_marginal_price <= 0
    ):
        return False
    if request.side == Side.BUY:
        return request.price <= current_protected_price and (
            current_marginal_price is None or request.price >= current_marginal_price
        )
    return request.price >= current_protected_price and (
        current_marginal_price is None or request.price <= current_marginal_price
    )


async def _final_opening_controls_allow(settings: Settings) -> bool:
    try:
        controls = await _await_owned_opening_operation(
            read_runtime_controls_bounded(Path(settings.storage.sqlite_path)),
            name="final-opening-controls",
            timeout_seconds=0.25,
        )
    except TimeoutError:
        return False
    return not controls.paused and not controls.killed


async def _quarantine_prepared_before_submit(
    journal: LiveOrderJournal,
    active: LiveJournalAction,
    recovery_action: str,
) -> CanaryRunEvidence:
    quarantined = await journal.transition(
        active.pair_action_id,
        LiveActionState.QUARANTINED,
        {"reason": recovery_action},
        recovery_action=recovery_action,
    )
    return CanaryRunEvidence(
        submitted=False,
        success=False,
        reason=ReasonCode.VENUE_QUARANTINED,
        route=active.route.value,
        quantity=None,
        orders_sent=0,
        hedged=False,
        residual_delta=quarantined.residual_delta,
        recovery_action=recovery_action,
        terminal_state=quarantined.state,
        economic_decision=None,
        preflights=(),
        reconciliation=None,
        owner_instruction="FAILED_QUARANTINED: no opening order was submitted",
    )


async def _resume_active_canary(
    settings: Settings,
    journal: LiveOrderJournal,
    active: LiveJournalAction,
) -> CanaryRunEvidence:
    try:
        emergency_venue = _wave1_emergency_venue(settings, active.route)
    except ValueError as error:
        return CanaryRunEvidence(
            False,
            False,
            ReasonCode.RECONCILIATION_INCOMPLETE,
            active.route.value,
            None,
            0,
            False,
            active.residual_delta,
            "BLOCKED_INVALID_DURABLE_ROUTE",
            active.state,
            None,
            (),
            None,
            f"FAILED_QUARANTINED: keep live disabled; {error}",
        )
    venues = {active.route.long_venue, active.route.short_venue, emergency_venue}
    public_adapters = {venue: CcxtProAdapter(venue) for venue in venues}
    private_adapters: dict[Venue, CcxtPrivateAdapter] = {}
    typed_adapters: dict[Venue, CanaryVenueAdapter] = {}
    private_stop: asyncio.Event | None = None
    private_task: asyncio.Task[None] | None = None
    try:
        if active.state == LiveActionState.PREPARED:
            try:
                instruments, _ = await _await_owned_opening_operation(
                    _discover_instruments(active.route.base, public_adapters),
                    name=f"prepared-discovery-{active.pair_action_id}",
                )
            except TimeoutError:
                return await _quarantine_prepared_before_submit(
                    journal,
                    active,
                    "PRE_SUBMIT_DISCOVERY_DEADLINE",
                )
        else:
            instruments, _ = await _discover_instruments(active.route.base, public_adapters)
        for venue in venues:
            private_adapters[venue] = CcxtPrivateAdapter(
                venue,
                PrivateCredentials.from_environment(venue),
            )
        _, private_stop, private_task, typed_adapters = await _start_private_state_supervisor(
            private_adapters,
            Path(settings.storage.sqlite_path),
        )
        states = await collect_private_states(
            typed_adapters,
            instruments,
            reconciliation_trigger="RESTART",
        )
        reconciliation = reconcile_private_states(
            active,
            states,
            await journal.known_client_order_ids(),
            venues,
        )
        try:
            plan = _rebuild_active_plan(
                active,
                instruments,
                settings.live.canary_timeout_seconds,
            )
        except ValueError as error:
            return CanaryRunEvidence(
                submitted=False,
                success=False,
                reason=ReasonCode.RECONCILIATION_INCOMPLETE,
                route=active.route.value,
                quantity=None,
                orders_sent=0,
                hedged=False,
                residual_delta=reconciliation.residual_delta,
                recovery_action="BLOCKED_INVALID_DURABLE_REQUEST",
                terminal_state=active.state,
                economic_decision=None,
                preflights=(),
                reconciliation=reconciliation,
                owner_instruction=(
                    "FAILED_QUARANTINED: keep live disabled and use emergency-flatten; "
                    f"automatic restart validation failed: {error}"
                ),
            )
        monitor: CanaryMonitor = ImmediateRecoveryCloseMonitor()
        if (
            active.state == LiveActionState.PREPARED
            and active.risk_reservation.get("supervisor_intent") == "LIVE_CANARY"
            and active.risk_reservation.get("supervisor_queued") is True
        ):

            async def fetch_initial_funding() -> tuple[FundingSnapshot, ...]:
                return tuple(
                    await asyncio.gather(
                        *(
                            public_adapters[venue].fetch_funding(instruments[venue])
                            for venue in venues
                        )
                    )
                )

            try:
                funding_values = await _await_owned_opening_operation(
                    fetch_initial_funding(),
                    name=f"prepared-initial-funding-{active.pair_action_id}",
                )
            except TimeoutError:
                return await _quarantine_prepared_before_submit(
                    journal,
                    active,
                    "PRE_SUBMIT_FUNDING_DEADLINE",
                )
            initial_funding = {snapshot.venue: snapshot for snapshot in funding_values}
            monitor = RuntimeCanaryMonitor(
                settings,
                active.route,
                plan.quantity,
                Decimal(str(active.risk_reservation["target_exit_spread_bps"])),
                public_adapters,
                typed_adapters,
                instruments,
                initial_funding,
                Path(settings.storage.sqlite_path),
            )
        result = await _coordinate_live_action(
            settings,
            journal,
            typed_adapters,
            private_adapters,
            instruments,
            public_adapters,
            plan,
            monitor,
            emergency_venue,
        )
        return CanaryRunEvidence(
            submitted=result.orders_sent > 0,
            success=result.success,
            reason=result.reason,
            route=active.route.value,
            quantity=plan.quantity,
            orders_sent=result.orders_sent,
            hedged=result.hedged,
            residual_delta=result.residual_delta,
            recovery_action=result.recovery_action or "AUTOMATIC_RESTART_RECOVERY",
            terminal_state=result.terminal_state,
            economic_decision=None,
            preflights=(),
            reconciliation=result.reconciliation,
            owner_instruction=result.owner_instruction,
        )
    finally:
        cleanup_errors: list[str] = []
        await _stop_private_state_supervisor(private_stop, private_task)
        _cancel_opening_gate_tasks()

        async def close_adapters() -> None:
            results = await asyncio.gather(
                *(adapter.close() for adapter in public_adapters.values()),
                *(adapter.close() for adapter in private_adapters.values()),
                return_exceptions=True,
            )
            failures = tuple(result for result in results if isinstance(result, BaseException))
            if failures:
                raise RuntimeError(
                    "; ".join(f"{type(error).__name__}: {error}" for error in failures)
                )

        try:
            await _await_owned_opening_operation(
                close_adapters(),
                name=f"canary-adapter-close-{active.pair_action_id}",
            )
        except (RuntimeError, TimeoutError) as error:
            cleanup_errors.append(f"adapter close: {type(error).__name__}: {error}")
        try:
            await shutdown_private_requests(typed_adapters)
        except Exception as error:
            cleanup_errors.append(f"private request shutdown: {type(error).__name__}: {error}")
        try:
            await _shutdown_opening_gate_tasks()
        except RuntimeError as error:
            cleanup_errors.append(str(error))
        if cleanup_errors:
            raise RuntimeError("canary cleanup failed: " + "; ".join(cleanup_errors))


async def recover_active_canary(
    settings: Settings,
    journal: LiveOrderJournal,
    active: LiveJournalAction,
) -> CanaryRunEvidence | LiveControlResult:
    """Supervisor recovery entry point; deliberately has no owner or qualification gates."""
    if active.risk_reservation.get(
        "qualification_bypassed_for_risk_reduction"
    ) is True or active.recovery_action in {
        "EMERGENCY_FLATTEN",
        "KILL_CANCEL_FLATTEN",
        "CLOSE_ALL_LIVE",
    }:
        return await OnDemandLiveControlPlane(settings).emergency_flatten()
    return await _resume_active_canary(settings, journal, active)


async def recover_active_actions(
    settings: Settings,
    journal: LiveOrderJournal,
    active: tuple[LiveJournalAction, ...],
) -> object:
    """Recover one action normally or flatten a multi-action account as one owned batch."""
    if not active:
        return object()
    if len(active) == 1:
        return await recover_active_canary(settings, journal, active[0])
    result = await OnDemandLiveControlPlane(settings).emergency_flatten()
    if not result.success:
        raise RuntimeError(
            result.instruction or "multi-action account recovery did not verify stable FLAT"
        )
    return result


async def run_canary_once(
    settings: Settings,
    config_path: Path,
    qualification_path: Path,
    repo_root: Path,
    owner_confirmation: str,
) -> CanaryRunEvidence:
    if owner_confirmation != OWNER_CONFIRMATION:
        return _denied(ReasonCode.OWNER_CONFIRMATION_MISSING)
    state_path = Path(settings.storage.sqlite_path)
    await initialise_state(state_path)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    active_actions = await journal.active_actions()
    if len(active_actions) == 1:
        return await _resume_active_canary(settings, journal, active_actions[0])
    if active_actions:
        return _denied(ReasonCode.RECONCILIATION_INCOMPLETE, active_actions[0].route)
    risk_stage = await read_risk_stage(state_path)
    if risk_stage.stage == RiskStage.SHADOW:
        return _denied(ReasonCode.CANARY_POLICY_VIOLATION)
    if not qualification_path.is_file():
        return _denied(ReasonCode.CURRENT_QUALIFICATION_MISSING)
    try:
        evidence = load_qualification(qualification_path)
    except (ValueError, KeyError):
        return _denied(ReasonCode.CURRENT_QUALIFICATION_MISSING)
    if evidence.route is None or evidence.strategy is None or evidence.replay_shadow is None:
        return _denied(ReasonCode.CURRENT_QUALIFICATION_MISSING)
    route = evidence.route
    locked_stages = load_locked_risk_stage_table(
        config_path.resolve().parent / "RUNTIME_POLICY.yaml"
    )
    stage_limits = next(
        limits for limits in locked_stages.stages if limits.stage == risk_stage.stage
    )
    if (
        risk_stage.qualification_hash != evidence.qualification_hash
        or risk_stage.runtime_policy_sha256 != locked_stages.runtime_policy_sha256
    ):
        return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route)
    try:
        emergency_venue = _wave1_emergency_venue(settings, route)
    except ValueError:
        return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route)
    image_digest = os.environ.get("IPEG_CONTAINER_IMAGE_DIGEST")
    qualification_valid, _ = qualification_is_current(
        evidence,
        repo_root,
        config_path,
        Path(settings.storage.parquet_dir),
        settings.live.qualification_max_age_seconds,
        expected_route=route,
        current_container_image_digest=image_digest,
    )
    if not qualification_valid:
        return _denied(ReasonCode.CURRENT_QUALIFICATION_MISSING, route)

    required_venues = {route.long_venue, route.short_venue, emergency_venue}
    public_adapters = {venue: CcxtProAdapter(venue) for venue in required_venues}
    private_adapters: dict[Venue, CcxtPrivateAdapter] = {}
    typed_adapters: dict[Venue, CanaryVenueAdapter] = {}
    private_stop: asyncio.Event | None = None
    private_task: asyncio.Task[None] | None = None
    try:
        instruments, public_reports = await _discover_instruments(
            route.base,
            public_adapters,
        )
        capabilities = {}
        for venue in required_venues:
            credentials = PrivateCredentials.from_environment(venue)
            private_adapters[venue] = CcxtPrivateAdapter(venue, credentials)
            capabilities[venue] = await private_adapters[venue].probe_private_capabilities()
        _, private_stop, private_task, typed_adapters = await _start_private_state_supervisor(
            private_adapters, state_path
        )
        books, quality = await _fresh_books(settings, public_adapters, instruments)
        funding_values = await asyncio.gather(
            *(public_adapters[venue].fetch_funding(instruments[venue]) for venue in required_venues)
        )
        funding = {snapshot.venue: snapshot for snapshot in funding_values}
        try:
            quantity = minimum_common_base_quantity(
                instruments[route.long_venue],
                instruments[route.short_venue],
                books[route.long_venue].asks[0].price,
                books[route.short_venue].bids[0].price,
            )
        except ValueError:
            return _denied(ReasonCode.CONTRACT_METADATA_UNKNOWN, route)
        if quantity != evidence.strategy.size_bucket_base_quantity:
            return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route, quantity)
        quote = evaluate_directed_route(
            instruments[route.long_venue],
            instruments[route.short_venue],
            books[route.long_venue],
            books[route.short_venue],
            funding[route.long_venue],
            funding[route.short_venue],
            quality[route.long_venue],
            quality[route.short_venue],
            quantity,
        )
        if not quote.eligible:
            return _denied(quote.reason, route, quantity)

        states = await collect_private_states(
            typed_adapters,
            instruments,
            reconciliation_trigger="PRE_SUBMIT",
        )
        reconciliation = reconcile_private_states(
            None,
            states,
            await journal.known_client_order_ids(),
            required_venues,
        )
        fee_rates = {
            venue: state.taker_fee_rate
            for venue, state in states.items()
            if state.taker_fee_rate is not None
        }
        route_fees_match = all(
            fee_rates.get(venue) == evidence.private_taker_fee_rates.get(venue)
            for venue in (route.long_venue, route.short_venue)
        )
        if not route_fees_match:
            qualification_valid = False

        emergency_state = states[emergency_venue]
        emergency_account = emergency_state.account
        emergency_assessment = evaluate_emergency_venue(
            instruments[route.long_venue],
            instruments[route.short_venue],
            instruments[emergency_venue],
            books[emergency_venue],
            fee_rates.get(emergency_venue),
            quantity,
            capability_ready=capabilities[emergency_venue].ready,
            account_ready=(
                emergency_state.error is None
                and emergency_account is not None
                and emergency_account.margin_mode == "cross"
                and emergency_account.position_mode == "oneway"
                and emergency_account.trading_enabled is True
                and emergency_account.withdrawal_enabled is False
                and emergency_account.transfer_enabled is False
            ),
            data_quality_ready=(
                quality[emergency_venue].accepted and public_reports[emergency_venue].public_ready
            ),
            slippage_cap_bps=settings.live.canary_close_slippage_cap_bps,
        )
        if not emergency_assessment.passed:
            return _denied(
                emergency_assessment.reason,
                route,
                quantity,
                emergency_assessment=emergency_assessment,
                reconciliation=reconciliation,
            )

        provisional_risk = RiskDecision(
            True,
            ReasonCode.RISK_RESERVED,
            {"projected_route_stress_usdt": Decimal(0)},
        )
        economic_policy = LiveEconomicPolicy(
            settings.live.canary_entry_slippage_cap_bps,
            settings.execution.latency_reserve_bps,
            settings.execution.partial_fill_reserve_bps,
            settings.execution.emergency_hedge_reserve_bps,
            settings.execution.reconciliation_forced_exit_reserve_bps,
            settings.execution.funding_stress_multiplier,
            settings.live.canary_minimum_profit_usdt,
        )
        economic = evaluate_live_entry(
            quote,
            instruments[route.long_venue],
            instruments[route.short_venue],
            books[route.long_venue],
            books[route.short_venue],
            funding[route.long_venue],
            funding[route.short_venue],
            {
                route.long_venue: fee_rates.get(route.long_venue, Decimal("-1")),
                route.short_venue: fee_rates.get(route.short_venue, Decimal("-1")),
            },
            evidence.strategy,
            economic_policy,
            provisional_risk,
            emergency_assessment=emergency_assessment,
        )
        if economic.signal is None:
            return _denied(economic.reason, route, quantity, economic=economic)
        projected_stress = max(
            economic.signal.cost.stressed_total_cost_usdt,
            evidence.replay_shadow.maximum_adverse_excursion_usdt,
        )
        notional = quantity * max(
            cast(Decimal, quote.entry_long_vwap),
            cast(Decimal, quote.entry_short_vwap),
        )
        risk = evaluate_canary_risk_from_private_state(
            route,
            states,
            notional,
            projected_stress,
            pair_stress_limit_usdt=stage_limits.pair_usdt,
            portfolio_stress_limit_usdt=stage_limits.portfolio_usdt,
            free_margin_floor_ratio=settings.live.canary_free_margin_floor_ratio,
            effective_leverage_cap=stage_limits.leverage,
            exit_depth_sufficient=emergency_assessment.passed,
        )
        economic = evaluate_live_entry(
            quote,
            instruments[route.long_venue],
            instruments[route.short_venue],
            books[route.long_venue],
            books[route.short_venue],
            funding[route.long_venue],
            funding[route.short_venue],
            {
                route.long_venue: fee_rates.get(route.long_venue, Decimal("-1")),
                route.short_venue: fee_rates.get(route.short_venue, Decimal("-1")),
            },
            evidence.strategy,
            economic_policy,
            risk,
            emergency_assessment=emergency_assessment,
        )
        existing_positions = sum(len(state.positions) for state in states.values())
        existing_orders = sum(len(state.open_orders) for state in states.values())
        maximum_leverage, minimum_free_margin = _actual_canary_ratios(states, notional)
        action = CanaryAction(
            route,
            1,
            notional,
            notional,
            projected_stress,
            maximum_leverage,
            minimum_free_margin,
            existing_positions,
            existing_orders,
        )
        policy = CanaryPolicy(
            route.base,
            route,
            stage_limits.pair_usdt,
            stage_limits.leverage,
            settings.live.canary_free_margin_floor_ratio,
        )
        policy_passed, policy_reason = policy.evaluate(action)
        if not economic.accepted:
            return _denied(economic.reason, route, quantity, economic=economic)
        if not policy_passed:
            return _denied(
                policy_reason or ReasonCode.CANARY_POLICY_VIOLATION,
                route,
                quantity,
                economic=economic,
            )

        preflights = _preflight_reports(
            settings,
            required_venues,
            capabilities,
            states,
            instruments,
            funding,
            public_reports,
            quality,
            reconciliation,
            risk,
        )
        controls = await read_runtime_controls(state_path)
        all_preflights_passed = len(preflights) == len(required_venues) and all(
            report.passed for report in preflights
        )
        capability_matrix = build_venue_capability_matrix(
            settings,
            public_reports=public_reports,
            private_reports=capabilities,
            account_preflight_passed=(
                frozenset(required_venues) if all_preflights_passed else frozenset()
            ),
            require_all_profiles=False,
        )
        all_preflights_passed = all_preflights_passed and all(
            capability_matrix.for_venue(venue).live_capability == CapabilityState.QUALIFIED
            for venue in required_venues
        )
        live_context = LiveContext(
            ci_or_test=_ci_or_test_environment(),
            simulation_or_replay=settings.app.mode != "live",
            local_unlock_present=bool(os.environ.get("IPEG_LOCAL_UNLOCK_SECRET")),
            telegram_challenge_valid=await live_confirmation_valid(state_path),
            current_qualification_valid=qualification_valid,
            route_allowlisted=evidence.route == route,
            canary_policy_passed=policy_passed,
            capability_preflight_passed=all_preflights_passed,
            account_preflight_passed=all_preflights_passed,
            market_data_preflight_passed=all(item.accepted for item in quality.values()),
            reconciliation_passed=reconciliation.consistent and reconciliation.flat_verified,
            risk_preflight_passed=risk.accepted,
            pause_or_kill_active=controls.paused or controls.killed,
            unknown_order_exists=bool(reconciliation.unknown_client_order_ids),
        )
        guard = evaluate_live_order(settings, live_context)
        if not guard.allowed:
            return _denied(
                guard.reason or ReasonCode.PREFLIGHT_FAILED,
                route,
                quantity,
                economic=economic,
                preflights=preflights,
                reconciliation=reconciliation,
            )
        assert economic.long_protected_price is not None
        assert economic.short_protected_price is not None
        prefix = f"ipeg-canary-{time.time_ns()}-{uuid4().hex[:8]}"
        long_client_id = venue_client_order_id(prefix, "long")
        short_client_id = venue_client_order_id(prefix, "short")
        long_intent = ExecutionIntent(
            long_client_id,
            route.long_venue,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            quantity,
            economic.long_protected_price,
        )
        short_intent = ExecutionIntent(
            short_client_id,
            route.short_venue,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            quantity,
            economic.short_protected_price,
        )
        plan = CanaryExecutionPlan(
            pair_action_id=prefix,
            route=route,
            tranche_id=f"{prefix}-tranche-1",
            quantity=quantity,
            long_request=translate_protected_order(
                long_intent,
                instruments[route.long_venue],
            ),
            short_request=translate_protected_order(
                short_intent,
                instruments[route.short_venue],
            ),
            risk_reservation={
                "risk": risk.breakdown,
                "projected_stress_usdt": projected_stress,
                "qualification_hash": evidence.qualification_hash,
                "supervisor_intent": "LIVE_CANARY",
                "supervisor_queued": True,
                "target_exit_spread_bps": evidence.strategy.target_exit_spread_bps,
                "initial_funding_rates": {
                    venue.value: funding[venue].rate for venue in required_venues
                },
                "opening_client_order_ids": {
                    "long": long_client_id,
                    "short": short_client_id,
                },
            },
            qualification_hash=evidence.qualification_hash,
            timeout_seconds=settings.live.canary_timeout_seconds,
        )
        queued_action = await LiveCanaryCoordinator(
            journal,
            typed_adapters,
            instruments,
            PublicProtectionProvider(settings, public_adapters, instruments),
            ImmediateRecoveryCloseMonitor(),
            emergency_venue,
            flat_barrier_policy=_flat_barrier_policy(settings),
        ).prepare(plan)
        return CanaryRunEvidence(
            submitted=False,
            success=True,
            reason=None,
            route=route.value,
            quantity=quantity,
            orders_sent=0,
            hedged=False,
            residual_delta=queued_action.residual_delta,
            recovery_action="QUEUED_FOR_LIVE_SAFETY_SUPERVISOR",
            terminal_state=queued_action.state,
            economic_decision=economic,
            preflights=preflights,
            reconciliation=reconciliation,
            owner_instruction=(
                "Intent durably queued; the long-running live safety supervisor owns submission, "
                "monitoring, and recovery."
            ),
            emergency_venue=emergency_assessment,
        )
    finally:
        await _stop_private_state_supervisor(private_stop, private_task)
        await asyncio.gather(
            *(adapter.close() for adapter in public_adapters.values()),
            *(adapter.close() for adapter in private_adapters.values()),
            return_exceptions=True,
        )
        await shutdown_private_requests(typed_adapters)


async def run_emergency_flatten(
    settings: Settings,
    config_path: Path,
    qualification_path: Path,
    repo_root: Path,
    confirmation: str,
) -> LiveControlResult | None:
    del config_path, qualification_path, repo_root
    if not emergency_unlock_valid(confirmation):
        return None
    journal = LiveOrderJournal(Path(settings.storage.sqlite_path))
    await journal.initialise()
    active_actions = await journal.active_actions()
    active = active_actions[0] if len(active_actions) == 1 else None
    venues = {Venue(value) for value in settings.venues.wave1_public}
    private: dict[Venue, CcxtPrivateAdapter] = {}
    try:
        for venue in venues:
            private[venue] = CcxtPrivateAdapter(
                venue,
                PrivateCredentials.from_environment(venue),
            )
        discovered = await asyncio.gather(
            *(adapter.list_instruments() for adapter in private.values())
        )
        registry = {
            (instrument.venue, instrument.symbol): instrument
            for batch in discovered
            for instrument in batch
        }
        return await LiveControlService(
            journal,
            cast(dict[Venue, CanaryVenueAdapter], private),
            registry,
            active.route if active is not None else None,
            active.qualification_hash if active is not None else None,
            _flat_barrier_policy(settings),
        ).emergency_flatten()
    finally:
        await asyncio.gather(
            *(adapter.close() for adapter in private.values()),
            return_exceptions=True,
        )
        await shutdown_private_requests(cast(dict[Venue, CanaryVenueAdapter], private))


async def _discover_instruments(
    base: str,
    adapters: dict[Venue, CcxtProAdapter],
) -> tuple[dict[Venue, Instrument], dict[Venue, CapabilityReport]]:
    instruments: dict[Venue, Instrument] = {}
    reports: dict[Venue, CapabilityReport] = {}
    for venue, adapter in adapters.items():
        reports[venue] = await adapter.probe_public_capabilities()
        discovered = await adapter.discover_instruments()
        selected = next(
            (
                instrument
                for instrument in discovered
                if instrument.base == base and instrument.settle == "USDT"
            ),
            None,
        )
        if selected is None:
            raise RuntimeError(f"{venue.value}:{ReasonCode.SYMBOL_UNAVAILABLE.value}")
        instruments[venue] = selected
    return instruments, reports


async def _fresh_books(
    settings: Settings,
    adapters: dict[Venue, CcxtProAdapter],
    instruments: dict[Venue, Instrument],
) -> tuple[dict[Venue, OrderBookSnapshot], dict[Venue, DataQualityAssessment]]:
    registry = BookRegistry()
    await asyncio.gather(
        *(adapter.watch_order_book(instruments[venue]) for venue, adapter in adapters.items())
    )
    values = await asyncio.gather(
        *(adapter.watch_order_book(instruments[venue]) for venue, adapter in adapters.items())
    )
    books = {book.venue: book for book in values}
    quality = {
        venue: registry.accept(
            book,
            max_age_ms=settings.market_data.max_l2_age_ms,
            max_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        )
        for venue, book in books.items()
    }
    return books, quality


def _preflight_reports(
    settings: Settings,
    venues: set[Venue],
    capabilities: dict[Venue, PrivateCapabilityReport],
    states: dict[Venue, VenuePrivateState],
    instruments: dict[Venue, Instrument],
    funding: dict[Venue, FundingSnapshot],
    public_reports: dict[Venue, CapabilityReport],
    quality: dict[Venue, DataQualityAssessment],
    reconciliation: ReconciliationReport,
    risk: RiskDecision,
) -> tuple[PrivatePreflightReport, ...]:
    reports: list[PrivatePreflightReport] = []
    for venue in sorted(venues, key=lambda item: item.value):
        state = states[venue]
        capability = capabilities[venue]
        public = public_reports[venue]
        data_quality = quality[venue]
        if state.account is None:
            continue
        reports.append(
            run_private_preflight(
                PrivatePreflightInput(
                    capability=capability,
                    account=state.account,
                    instrument=instruments[venue],
                    fee_rate=state.taker_fee_rate,
                    funding_known=(
                        funding[venue].rate is not None
                        and funding[venue].next_funding_timestamp_ms is not None
                        and funding[venue].interval is not None
                        and funding[venue].exchange_timestamp_ms is not None
                    ),
                    clock_skew_ms=public.clock_skew_ms,
                    maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
                    symbol_available=True,
                    data_quality_passed=data_quality.accepted and public.public_ready,
                    reconciliation_passed=reconciliation.consistent,
                    risk_passed=risk.accepted,
                    free_margin_floor_ratio=settings.live.canary_free_margin_floor_ratio,
                )
            )
        )
    return tuple(reports)


def _actual_canary_ratios(
    states: dict[Venue, VenuePrivateState],
    notional: Decimal,
) -> tuple[Decimal, Decimal]:
    leverages: list[Decimal] = []
    free_ratios: list[Decimal] = []
    for state in states.values():
        if state.account is None or state.account.equity_usdt <= 0:
            return Decimal("Infinity"), Decimal(0)
        leverages.append(notional / state.account.equity_usdt)
        free_ratios.append(state.account.free_margin_usdt / state.account.equity_usdt)
    return max(leverages, default=Decimal("Infinity")), min(
        free_ratios,
        default=Decimal(0),
    )


def _denied(
    reason: ReasonCode,
    route: DirectedRouteKey | None = None,
    quantity: Decimal | None = None,
    *,
    economic: LiveEconomicDecision | None = None,
    preflights: tuple[PrivatePreflightReport, ...] = (),
    reconciliation: ReconciliationReport | None = None,
    emergency_assessment: EmergencyVenueAssessment | None = None,
) -> CanaryRunEvidence:
    return CanaryRunEvidence(
        submitted=False,
        success=False,
        reason=reason,
        route=route.value if route is not None else None,
        quantity=quantity,
        orders_sent=0,
        hedged=False,
        residual_delta=Decimal(0),
        recovery_action=None,
        terminal_state=None,
        economic_decision=economic,
        preflights=preflights,
        reconciliation=reconciliation,
        owner_instruction=None,
        emergency_venue=emergency_assessment,
    )


def _ci_or_test_environment() -> bool:
    return os.environ.get("CI", "").lower() == "true" or "PYTEST_CURRENT_TEST" in os.environ
