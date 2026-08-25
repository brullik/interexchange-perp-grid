from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import time
from collections.abc import Coroutine
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter, PrivateCredentials
from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveDecisionPolicy,
    AggressiveEntryStage,
    AggressiveExitInput,
    AggressiveExitReason,
    HybridEntryInput,
    VenueFundingProjection,
    canonical_executable_spread_bps,
    load_aggressive_decision_policy,
    revalidate_hybrid_entry_once,
    select_aggressive_exit_reason,
)
from interexchange_perp_grid.aggressive_live import (
    AggressiveLaptopLiveStage,
    AggressiveLiveIntentEnvelope,
    aggressive_intent_from_mapping,
    aggressive_intent_sha256,
    prepare_aggressive_live_plan,
)
from interexchange_perp_grid.aggressive_model import DivergenceDirection
from interexchange_perp_grid.aggressive_qualification import AggressiveQualificationBinding
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
    combined_event_watermark,
    evaluate_canary_risk_from_private_state,
    reconcile_private_states,
    reconciliation_position_signature_sha256,
    shutdown_private_requests,
    wait_for_stable_flat,
)
from interexchange_perp_grid.market_data import BookRegistry, DataQualityAssessment
from interexchange_perp_grid.native_runtime import resolve_runtime_artifact_digest
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
    laptop_owner_exception_authorized,
    laptop_owner_exception_policy,
    load_qualification,
    qualification_is_current,
    qualification_policy_from_settings,
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
PILOT_A_OWNER_CONFIRMATION = "I_ACCEPT_AGGRESSIVE_PILOT_A_RISK"
_OPENING_GATE_TASKS: set[asyncio.Task[object]] = set()


@dataclass(frozen=True, slots=True)
class AuthoritativeFlatEvidence:
    stable_flat: bool
    private_event_watermark: int
    reconciliation_sha256: str


async def collect_authoritative_live_flat_evidence(
    settings: Settings,
    base: str,
) -> AuthoritativeFlatEvidence:
    """Take a fresh account-wide private stable-FLAT barrier without submit authority."""
    state_path = Path(settings.storage.sqlite_path)
    await initialise_state(state_path)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    venues = {Venue(value) for value in settings.venues.wave1_public}
    public = {venue: CcxtProAdapter(venue) for venue in venues}
    private: dict[Venue, CcxtPrivateAdapter] = {}
    try:
        instruments, _ = await _discover_instruments(base, public)
        private = {
            venue: CcxtPrivateAdapter(venue, PrivateCredentials.from_environment(venue))
            for venue in venues
        }

        async def report_factory() -> ReconciliationReport:
            states = await collect_private_states(
                private,
                instruments,
                reconciliation_trigger="LAPTOP_ACCEPTANCE_FINAL_FLAT",
            )
            return reconcile_private_states(
                (),
                states,
                await journal.known_client_order_ids(),
                venues,
            )

        barrier = await wait_for_stable_flat(
            report_factory,
            lambda: combined_event_watermark(private, journal.event_watermark),
            _flat_barrier_policy(settings),
        )
        return AuthoritativeFlatEvidence(
            stable_flat=barrier.verified,
            private_event_watermark=barrier.event_watermark,
            reconciliation_sha256=reconciliation_position_signature_sha256(barrier.report),
        )
    finally:
        await shutdown_private_requests(private)
        await asyncio.gather(
            *(adapter.close() for adapter in public.values()),
            *(adapter.close() for adapter in private.values()),
            return_exceptions=True,
        )


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
    actions: tuple[LiveJournalAction, ...]
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
        *,
        pair_action_id: str | None = None,
        direction: DivergenceDirection | None = None,
        effective_stop_bps: Decimal | None = None,
        projected_route_loss_usdt: Decimal = Decimal(0),
        projected_portfolio_loss_usdt: Decimal = Decimal(0),
        route_hard_loss_usdt: Decimal = Decimal("Infinity"),
        portfolio_hard_loss_usdt: Decimal = Decimal("Infinity"),
        holding_deadline: datetime | None = None,
        aggressive_policy: AggressiveDecisionPolicy | None = None,
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
        self._pair_action_id = pair_action_id
        self._direction = direction
        self._effective_stop_bps = effective_stop_bps
        self._projected_route_loss_usdt = projected_route_loss_usdt
        self._projected_portfolio_loss_usdt = projected_portfolio_loss_usdt
        self._route_hard_loss_usdt = route_hard_loss_usdt
        self._portfolio_hard_loss_usdt = portfolio_hard_loss_usdt
        self._holding_deadline = holding_deadline
        self._aggressive_policy = aggressive_policy
        self._registry = BookRegistry()

    async def wait_for_close(self, timeout_seconds: int) -> CloseReason:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            controls = await read_runtime_controls(self._state_path)
            if controls.killed:
                return CloseReason.EMERGENCY
            if controls.paused:
                return CloseReason.OPERATOR_CLOSE
            if self._aggressive_policy is not None:
                try:
                    await self._refresh_durable_aggressive_risk()
                except Exception:
                    return CloseReason.EMERGENCY
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
                journal = LiveOrderJournal(self._state_path)
                active_actions, known_client_ids = await asyncio.gather(
                    journal.active_actions(),
                    journal.known_client_order_ids(),
                )
                reconciliation = reconcile_private_states(
                    active_actions,
                    states,
                    known_client_ids,
                    set(self._private),
                )
            except Exception:
                return CloseReason.STALE_DATA
            executable_spread = self._executable_spread(book_by_venue)
            data_stale = not all(item.accepted for item in quality)
            risk_deteriorated = (
                _private_risk_deteriorated(self._settings, states) or not reconciliation.consistent
            )
            funding_deteriorated = self._funding_deteriorated(funding)
            aggressive_reason = self._aggressive_close_reason(
                executable_spread,
                funding_deteriorated,
            )
            signals = CanaryCloseSignals(
                emergency_active=(aggressive_reason == AggressiveExitReason.EMERGENCY_OR_UNKNOWN),
                target_converged=aggressive_reason == AggressiveExitReason.REVERSE_GRID_TARGET,
                hard_stop_or_loss=(
                    aggressive_reason == AggressiveExitReason.HARD_PROJECTED_LOSS_OR_REFERENCE_STOP
                ),
                hard_holding_time=aggressive_reason == AggressiveExitReason.HARD_HOLDING_TIME,
                risk_deteriorated=risk_deteriorated,
                funding_deteriorated=(
                    funding_deteriorated
                    if self._aggressive_policy is None
                    else aggressive_reason == AggressiveExitReason.ADVERSE_FUNDING
                ),
                public_or_private_data_stale=data_stale,
            )
            reason = first_close_reason(signals)
            if reason is not None:
                return reason
            await asyncio.sleep(
                float(min(Decimal(1), max(Decimal("0.01"), Decimal(timeout_seconds) / 100)))
            )
        return CloseReason.CANARY_TIMEOUT

    async def _refresh_durable_aggressive_risk(self) -> None:
        """Refresh aggregate limits and the route deadline from the durable owner set."""
        if self._pair_action_id is None:
            raise RuntimeError("aggressive monitor has no durable action identity")
        journal = LiveOrderJournal(self._state_path)
        active = await journal.active_actions()
        current = next(
            (item for item in active if item.pair_action_id == self._pair_action_id),
            None,
        )
        if current is None or current.route != self._route:
            raise RuntimeError("aggressive monitor lost its durable action")
        aggressive = tuple(
            item
            for item in active
            if item.risk_reservation.get("strategy") == "AGGRESSIVE_SYMBIOSIS_V1"
        )
        if not aggressive:
            raise RuntimeError("aggressive monitor has no durable portfolio")
        self._projected_route_loss_usdt = sum(
            (
                _effective_reserved_stress(item.risk_reservation)
                for item in aggressive
                if item.route == self._route
            ),
            Decimal(0),
        )
        self._projected_portfolio_loss_usdt = sum(
            (_effective_reserved_stress(item.risk_reservation) for item in aggressive),
            Decimal(0),
        )
        try:
            deadlines = tuple(
                datetime.fromisoformat(str(item.risk_reservation["hard_holding_deadline"]))
                for item in aggressive
                if item.route == self._route
            )
        except (KeyError, ValueError) as error:
            raise RuntimeError("aggressive route deadline is incomplete") from error
        if not deadlines or any(
            deadline.tzinfo is None or deadline.utcoffset() is None for deadline in deadlines
        ):
            raise RuntimeError("aggressive route deadline is invalid")
        self._holding_deadline = min(deadlines)

    async def _funding(self) -> dict[Venue, FundingSnapshot]:
        venues = (self._route.long_venue, self._route.short_venue)
        values = await asyncio.gather(
            *(self._public[venue].fetch_funding(self._instruments[venue]) for venue in venues)
        )
        return {snapshot.venue: snapshot for snapshot in values}

    def _executable_spread(
        self,
        books: dict[Venue, OrderBookSnapshot],
    ) -> Decimal | None:
        long_exit = executable_vwap(
            books[self._route.long_venue].bids,
            self._quantity,
        )
        short_exit = executable_vwap(
            books[self._route.short_venue].asks,
            self._quantity,
        )
        if long_exit is None or short_exit is None:
            return None
        if self._direction is None:
            return (short_exit.price - long_exit.price) / long_exit.price * Decimal(10_000)
        return canonical_executable_spread_bps(
            self._direction,
            long_exit.price,
            short_exit.price,
        )

    def _target_converged(self, spread_bps: Decimal | None) -> bool:
        if spread_bps is None:
            return False
        if self._direction == DivergenceDirection.NEGATIVE:
            return spread_bps >= self._target_exit_spread_bps
        return spread_bps <= self._target_exit_spread_bps

    def _aggressive_close_reason(
        self,
        spread_bps: Decimal | None,
        adverse_funding_destroys_profit: bool,
    ) -> AggressiveExitReason:
        if self._aggressive_policy is None:
            return (
                AggressiveExitReason.REVERSE_GRID_TARGET
                if self._target_converged(spread_bps)
                else AggressiveExitReason.NONE
            )
        if (
            spread_bps is None
            or self._direction is None
            or self._effective_stop_bps is None
            or self._holding_deadline is None
        ):
            return AggressiveExitReason.EMERGENCY_OR_UNKNOWN
        if (
            self._projected_route_loss_usdt >= self._route_hard_loss_usdt
            or self._projected_portfolio_loss_usdt >= self._portfolio_hard_loss_usdt
        ):
            return AggressiveExitReason.HARD_PROJECTED_LOSS_OR_REFERENCE_STOP
        return select_aggressive_exit_reason(
            AggressiveExitInput(
                direction=self._direction,
                executable_spread_bps=spread_bps,
                effective_stop_bps=self._effective_stop_bps,
                reverse_target_bps=self._target_exit_spread_bps,
                projected_route_loss_usdt=self._projected_route_loss_usdt,
                projected_portfolio_loss_usdt=self._projected_portfolio_loss_usdt,
                holding_deadline=self._holding_deadline,
                now=datetime.now(UTC),
                emergency_or_unknown=False,
                adverse_funding_destroys_profit=adverse_funding_destroys_profit,
            ),
            self._aggressive_policy,
        )

    def _funding_deteriorated(self, current: dict[Venue, FundingSnapshot]) -> bool:
        now = datetime.now(UTC)
        now_ms = int(now.timestamp() * 1000)
        # The laptop live program does not claim exchange funding-ledger evidence.
        # Close before the first stored funding event so accepted stage PnL never
        # depends on an unobserved credit/debit.
        for venue in (self._route.long_venue, self._route.short_venue):
            next_timestamp = self._initial_funding[venue].next_funding_timestamp_ms
            if next_timestamp is None or now_ms >= next_timestamp - 60_000:
                return True
        maximum_hold_seconds = (
            self._aggressive_policy.hard_max_hold_seconds
            if self._aggressive_policy is not None
            else self._settings.live.canary_timeout_seconds
        )
        venues = (self._route.long_venue, self._route.short_venue)
        if any(venue not in current for venue in venues) or any(
            _gate_funding_projection(
                current[venue],
                venue,
                self._instruments[venue].symbol,
                now,
                maximum_hold_seconds,
                self._settings.strategy.calibration_funding_refresh_seconds * 1000,
                self._settings.market_data.max_clock_skew_ms,
            )
            is None
            for venue in venues
        ):
            return True
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


def _effective_reserved_stress(risk_reservation: object) -> Decimal:
    """Return the conservative per-action stress after durable fill repricing."""
    if not isinstance(risk_reservation, dict):
        raise ValueError("live risk reservation is invalid")
    try:
        planned = Decimal(str(risk_reservation["projected_stress_usdt"]))
        actual = risk_reservation.get("actual_fill_risk")
        repriced = (
            Decimal(str(actual["incremental_stress_usdt"]))
            if isinstance(actual, dict) and "incremental_stress_usdt" in actual
            else planned
        )
    except (ArithmeticError, KeyError, ValueError) as error:
        raise ValueError("live risk reservation is invalid") from error
    if not planned.is_finite() or not repriced.is_finite() or min(planned, repriced) <= 0:
        raise ValueError("live risk reservation is invalid")
    return max(planned, repriced)


def _gate_funding_projection(
    snapshot: FundingSnapshot,
    venue: Venue,
    symbol: str,
    now: datetime,
    maximum_hold_seconds: int,
    maximum_age_ms: int,
    maximum_future_skew_ms: int,
) -> VenueFundingProjection | None:
    if (
        snapshot.venue != venue
        or snapshot.symbol != symbol
        or snapshot.rate is None
        or snapshot.mark_price is None
        or snapshot.next_funding_timestamp_ms is None
        or snapshot.interval is None
        or snapshot.exchange_timestamp_ms is None
    ):
        return None
    now_ms = int(now.timestamp() * 1000)
    age_ms = now_ms - snapshot.exchange_timestamp_ms
    if age_ms < -maximum_future_skew_ms or age_ms > maximum_age_ms:
        return None
    raw_interval = snapshot.interval.strip().lower()
    if (
        len(raw_interval) < 2
        or raw_interval[-1] not in {"h", "m"}
        or not raw_interval[:-1].isdigit()
    ):
        return None
    interval_seconds = int(raw_interval[:-1]) * (3600 if raw_interval[-1] == "h" else 60)
    remaining_ms = snapshot.next_funding_timestamp_ms - int(now.timestamp() * 1000)
    if interval_seconds <= 0 or remaining_ms < 0:
        return None
    horizon_ms = maximum_hold_seconds * 1000
    event_count = (
        0
        if remaining_ms > horizon_ms
        else 1 + (horizon_ms - remaining_ms) // (interval_seconds * 1000)
    )
    try:
        return VenueFundingProjection(
            venue=venue,
            rate=snapshot.rate,
            mark_price=snapshot.mark_price,
            event_count=event_count,
            next_funding_timestamp_ms=snapshot.next_funding_timestamp_ms,
            interval_seconds=interval_seconds,
        )
    except ValueError:
        return None


class ImmediateRecoveryCloseMonitor(CanaryMonitor):
    """A restarted action is reduced immediately instead of reopening its holding window."""

    async def wait_for_close(self, timeout_seconds: int) -> CloseReason:
        del timeout_seconds
        return CloseReason.EMERGENCY


class PortfolioInterruptMonitor(CanaryMonitor):
    """Wake a tranche monitor immediately when the shared route watcher fires."""

    def __init__(self, delegate: CanaryMonitor, route_close_event: asyncio.Event) -> None:
        self._delegate = delegate
        self._route_close_event = route_close_event

    async def wait_for_close(self, timeout_seconds: int) -> CloseReason:
        delegate = asyncio.create_task(self._delegate.wait_for_close(timeout_seconds))
        interrupt = asyncio.create_task(self._route_close_event.wait())
        done, pending = await asyncio.wait(
            (delegate, interrupt),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if interrupt in done and interrupt.result():
            return CloseReason.HARD_STOP_OR_LOSS
        return delegate.result()


async def _watch_aggressive_route_safety(
    settings: Settings,
    journal: LiveOrderJournal,
    route: DirectedRouteKey,
    close_event: asyncio.Event,
    stop_event: asyncio.Event,
) -> None:
    """Continuously observe route-wide stop/deadline/risk between tranche cycles."""
    public = {venue: CcxtProAdapter(venue) for venue in (route.long_venue, route.short_venue)}
    registry = BookRegistry()
    try:
        instruments, _ = await _discover_instruments(route.base, public)
        while not stop_event.is_set() and not close_event.is_set():
            active = tuple(
                item
                for item in await journal.active_actions()
                if item.route == route
                and item.risk_reservation.get("strategy") == "AGGRESSIVE_SYMBIOSIS_V1"
            )
            if not active:
                return
            parameters = tuple(_aggressive_monitor_parameters(item) for item in active)
            if any(item is None for item in parameters):
                raise RuntimeError("route watcher found an incompatible durable action")
            typed = tuple(item for item in parameters if item is not None)
            directions = {item[0] for item in typed}
            stops = {item[1] for item in typed}
            route_limits = {item[4] for item in typed}
            portfolio_limits = {item[5] for item in typed}
            if any(
                len(values) != 1 for values in (directions, stops, route_limits, portfolio_limits)
            ):
                raise RuntimeError("route watcher found mutable route safety geometry")
            direction = next(iter(directions))
            effective_stop = next(iter(stops))
            route_limit = next(iter(route_limits))
            portfolio_limit = next(iter(portfolio_limits))
            deadlines = tuple(item[6] for item in typed)
            route_risk = sum(
                (_effective_reserved_stress(item.risk_reservation) for item in active),
                Decimal(0),
            )
            portfolio_risk = sum(
                (
                    _effective_reserved_stress(item.risk_reservation)
                    for item in await journal.active_actions()
                    if item.risk_reservation.get("strategy") == "AGGRESSIVE_SYMBIOSIS_V1"
                ),
                Decimal(0),
            )
            if (
                route_risk >= route_limit
                or portfolio_risk >= portfolio_limit
                or datetime.now(UTC) >= min(deadlines)
            ):
                close_event.set()
                return
            quantity = sum(
                (
                    sum(
                        (
                            leg.filled_base_quantity
                            if leg.venue == route.long_venue and leg.side == Side.BUY
                            else -leg.filled_base_quantity
                            if leg.venue == route.long_venue and leg.side == Side.SELL
                            else Decimal(0)
                            for leg in item.legs
                        ),
                        Decimal(0),
                    )
                    for item in active
                ),
                Decimal(0),
            )
            if quantity <= 0:
                raise RuntimeError("route watcher lost positive long ownership")
            books = await asyncio.gather(
                *(
                    public[venue].watch_order_book(instruments[venue])
                    for venue in (route.long_venue, route.short_venue)
                )
            )
            if not all(
                registry.accept(
                    book,
                    max_age_ms=settings.market_data.max_l2_age_ms,
                    max_clock_skew_ms=settings.market_data.max_clock_skew_ms,
                ).accepted
                for book in books
            ):
                close_event.set()
                return
            by_venue = {book.venue: book for book in books}
            long_exit = executable_vwap(by_venue[route.long_venue].bids, quantity)
            short_exit = executable_vwap(by_venue[route.short_venue].asks, quantity)
            if long_exit is None or short_exit is None:
                close_event.set()
                return
            spread = canonical_executable_spread_bps(
                direction,
                long_exit.price,
                short_exit.price,
            )
            if (direction == DivergenceDirection.POSITIVE and spread >= effective_stop) or (
                direction == DivergenceDirection.NEGATIVE and spread <= effective_stop
            ):
                close_event.set()
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=0.01)
    except Exception:
        close_event.set()
        raise
    finally:
        await asyncio.gather(
            *(adapter.close() for adapter in public.values()),
            return_exceptions=True,
        )


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


def _aggressive_monitor_parameters(
    action: LiveJournalAction,
) -> tuple[DivergenceDirection, Decimal, Decimal, Decimal, Decimal, Decimal, datetime] | None:
    if action.risk_reservation.get("strategy") != "AGGRESSIVE_SYMBIOSIS_V1":
        return None
    try:
        actual = action.risk_reservation.get("actual_fill_risk")
        route_total = (
            actual.get("route_total_usdt")
            if isinstance(actual, dict)
            else action.risk_reservation["projected_route_total_usdt"]
        )
        portfolio_total = (
            actual.get("portfolio_total_usdt")
            if isinstance(actual, dict)
            else action.risk_reservation["projected_portfolio_total_usdt"]
        )
        if route_total is None or portfolio_total is None:
            raise KeyError("actual fill risk totals are incomplete")
        values = (
            DivergenceDirection(str(action.risk_reservation["direction"])),
            Decimal(str(action.risk_reservation["effective_stop_bps"])),
            Decimal(str(route_total)),
            Decimal(str(portfolio_total)),
            Decimal(str(action.risk_reservation["route_hard_loss_usdt"])),
            Decimal(str(action.risk_reservation["portfolio_hard_loss_usdt"])),
            datetime.fromisoformat(str(action.risk_reservation["hard_holding_deadline"])),
        )
    except (KeyError, ValueError, ArithmeticError) as error:
        raise ValueError("aggressive monitor reservation is incomplete") from error
    _, stop, route_loss, portfolio_loss, route_limit, portfolio_limit, deadline = values
    if (
        any(
            not value.is_finite()
            for value in (stop, route_loss, portfolio_loss, route_limit, portfolio_limit)
        )
        or min(route_loss, portfolio_loss) < 0
        or min(route_limit, portfolio_limit) <= 0
        or deadline.tzinfo is None
        or deadline.utcoffset() is None
    ):
        raise ValueError("aggressive monitor reservation is invalid")
    return values


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
    *,
    portfolio_mode: bool = False,
    aggressive_policy: AggressiveDecisionPolicy | None = None,
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
                journal.active_actions(),
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
                actions=cast(tuple[LiveJournalAction, ...], results[6]),
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
        gate_now = datetime.now(UTC)
        funding_projections = {
            venue: _gate_funding_projection(
                funding[venue],
                venue,
                instruments[venue].symbol,
                gate_now,
                (
                    aggressive_policy.hard_max_hold_seconds
                    if aggressive_policy is not None
                    else settings.live.canary_timeout_seconds
                ),
                settings.strategy.calibration_funding_refresh_seconds * 1000,
                settings.market_data.max_clock_skew_ms,
            )
            for venue in capability_venues
        }
        if not any(
            action.pair_action_id == current_plan.pair_action_id for action in snapshot.actions
        ):
            return False
        reconciliation = reconcile_private_states(
            snapshot.actions,
            snapshot.private_states,
            snapshot.known_client_ids,
            set(adapters),
        )
        try:
            projected_stress = _effective_reserved_stress(current_plan.risk_reservation)
            route_stress = sum(
                (
                    _effective_reserved_stress(action.risk_reservation)
                    for action in snapshot.actions
                    if action.route == current_plan.route
                ),
                Decimal(0),
            )
            portfolio_stress = sum(
                (
                    _effective_reserved_stress(action.risk_reservation)
                    for action in snapshot.actions
                ),
                Decimal(0),
            )
        except ValueError:
            return False
        aggressive_stage = current_plan.risk_reservation.get("stage")
        is_pilot_a = aggressive_stage == AggressiveLaptopLiveStage.PILOT_A.value
        route_admission_limit = (
            Decimal("4.5") if is_pilot_a else settings.live.canary_pair_stressed_loss_limit_usdt
        )
        portfolio_admission_limit = Decimal("5") if is_pilot_a else route_admission_limit
        risk_current = (
            projected_stress.is_finite()
            and Decimal(0) < projected_stress <= route_admission_limit
            and route_stress <= route_admission_limit
            and portfolio_stress <= portfolio_admission_limit
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
                        funding_known=funding_projections[venue] is not None,
                        clock_skew_ms=public_reports[venue].clock_skew_ms,
                        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
                        symbol_available=True,
                        data_quality_passed=(
                            snapshot.quality[venue].accepted and public_reports[venue].public_ready
                        ),
                        reconciliation_passed=(
                            reconciliation.consistent
                            and (is_pilot_a or reconciliation.flat_verified)
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
        aggressive_revalidated = True
        if current_plan.risk_reservation.get("strategy") == "AGGRESSIVE_SYMBIOSIS_V1":
            aggressive_revalidated = False
            try:
                if aggressive_policy is None:
                    raise ValueError("aggressive policy is unavailable")
                intent = aggressive_intent_from_mapping(
                    current_plan.risk_reservation["aggressive_intent"]
                )
                stored_hash = str(current_plan.risk_reservation["aggressive_intent_sha256"])
                now = datetime.now(UTC)
                last_closed_minute = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                outer_stage = AggressiveLaptopLiveStage(str(current_plan.risk_reservation["stage"]))
                expected_stage = (
                    AggressiveEntryStage.LOCKED_CANARY
                    if outer_stage == AggressiveLaptopLiveStage.CANARY
                    else AggressiveEntryStage.NORMAL
                )
                long_venue = Venue(intent.long_venue)
                short_venue = Venue(intent.short_venue)
                binding_hash = str(current_plan.risk_reservation["aggressive_binding_sha256"])
                if (
                    aggressive_intent_sha256(intent) != stored_hash
                    or intent.intent_id != current_plan.pair_action_id
                    or intent.quantity != current_plan.quantity
                    or intent.entry_stage != expected_stage
                    or aggressive_stage != outer_stage.value
                    or intent.reference_interval_start != last_closed_minute
                    or current_plan.route.value != intent.route_identity
                    or current_plan.long_request.venue != long_venue
                    or current_plan.short_request.venue != short_venue
                    or current_plan.long_request.symbol != intent.long_symbol
                    or current_plan.short_request.symbol != intent.short_symbol
                    or len(current_plan.qualification_hash) != 64
                    or len(binding_hash) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in current_plan.qualification_hash + binding_hash
                    )
                    or current_plan.risk_reservation.get("strategy_profile_sha256")
                    != intent.strategy_profile_sha256
                ):
                    raise ValueError("aggressive final-gate identity is stale or inconsistent")
                long_state = snapshot.private_states[long_venue]
                short_state = snapshot.private_states[short_venue]
                current_entry = HybridEntryInput(
                    route_identity=intent.route_identity,
                    direction=intent.direction,
                    level_index=intent.level_index,
                    reference_spread_bps=intent.reference_spread_bps,
                    reference_trigger_bps=intent.reference_trigger_bps,
                    grid_step_bps=intent.grid_step_bps,
                    stressed_cost_move_bps=intent.stressed_cost_move_bps,
                    minimum_profit_move_bps=intent.minimum_profit_move_bps,
                    normal_low_bps=intent.normal_low_bps,
                    normal_high_bps=intent.normal_high_bps,
                    quantity=intent.quantity,
                    long_venue=long_venue,
                    short_venue=short_venue,
                    long_book=snapshot.books[long_venue],
                    short_book=snapshot.books[short_venue],
                    long_private_taker_fee_rate=long_state.taker_fee_rate,
                    short_private_taker_fee_rate=short_state.taker_fee_rate,
                    long_funding=funding_projections[long_venue],
                    short_funding=funding_projections[short_venue],
                    reserves=intent.reserves,
                    observed_monotonic_ns=time.monotonic_ns(),
                    maximum_book_age_ms=settings.market_data.max_l2_age_ms,
                    now=now,
                    reference_interval_start=intent.reference_interval_start,
                    stage=expected_stage,
                    state_reconciled=reconciliation.consistent,
                    historical_model_eligible=True,
                    regime_ready=True,
                )
                aggressive_revalidated = revalidate_hybrid_entry_once(
                    current_entry,
                    policy=aggressive_policy,
                ).accepted
            except (KeyError, TypeError, ValueError, ArithmeticError):
                aggressive_revalidated = False
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
            and aggressive_revalidated
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
        # Re-run the complete bounded gate after durable submit-attempt ownership.
        # No transport submit has happened yet, so any TOCTOU drift still fails closed.
        final_opening_gate=lambda: opening_gate(plan),
        portfolio_mode=portfolio_mode,
        aggressive_policy=aggressive_policy,
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
    *,
    portfolio_mode: bool = False,
    portfolio_close_event: asyncio.Event | None = None,
) -> CanaryRunEvidence:
    if active.state == LiveActionState.PREPARED and not _fresh_supervisor_handoff(active):
        # Only a very recent, pristine supervisor handoff may reach the complete
        # opening gates below. Unknown/stale PREPARED state has no exchange
        # exposure and is quarantined without constructing a transport.
        return await _quarantine_prepared_before_submit(
            journal,
            active,
            "PREPARED_RESTART_REQUIRES_FRESH_ENTRY_EPOCH",
        )
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
                1 if portfolio_mode else settings.live.canary_timeout_seconds,
            )
            monitor_parameters = _aggressive_monitor_parameters(active)
            aggressive_policy: AggressiveDecisionPolicy | None = None
            if monitor_parameters is not None:
                loaded_policy = load_aggressive_decision_policy(
                    Path(__file__).resolve().parents[2] / "config" / "AGGRESSIVE_SYMBIOSIS_V1.yaml"
                )
                if (
                    active.risk_reservation.get("strategy_profile_sha256")
                    != loaded_policy.profile_sha256
                ):
                    raise ValueError("aggressive monitor strategy profile identity changed")
                aggressive_policy = loaded_policy.policy
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
            active.state in {LiveActionState.PREPARED, LiveActionState.HEDGED}
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
                funding_values = None
            initial_funding = (
                {}
                if funding_values is None
                else {snapshot.venue: snapshot for snapshot in funding_values}
            )
            funding_monitor_valid = funding_values is not None
            if funding_monitor_valid:
                stored_rates = active.risk_reservation.get("initial_funding_rates")
                stored_next_timestamps = active.risk_reservation.get(
                    "initial_funding_next_timestamp_ms"
                )
                if not isinstance(stored_rates, dict) or not isinstance(
                    stored_next_timestamps, dict
                ):
                    funding_monitor_valid = False
                else:
                    try:
                        initial_funding = {
                            venue: replace(
                                snapshot,
                                rate=Decimal(str(stored_rates[venue.value])),
                                next_funding_timestamp_ms=int(
                                    str(stored_next_timestamps[venue.value])
                                ),
                            )
                            for venue, snapshot in initial_funding.items()
                        }
                    except (KeyError, ValueError, ArithmeticError):
                        funding_monitor_valid = False
            if funding_monitor_valid:
                try:
                    target_exit_spread_bps = Decimal(
                        str(active.risk_reservation["target_exit_spread_bps"])
                    )
                except (KeyError, ValueError, ArithmeticError):
                    target_exit_spread_bps = Decimal("NaN")
                if not target_exit_spread_bps.is_finite():
                    funding_monitor_valid = False
            if funding_monitor_valid:
                monitor = (
                    RuntimeCanaryMonitor(
                        settings,
                        active.route,
                        plan.quantity,
                        target_exit_spread_bps,
                        public_adapters,
                        typed_adapters,
                        instruments,
                        initial_funding,
                        Path(settings.storage.sqlite_path),
                    )
                    if monitor_parameters is None
                    else RuntimeCanaryMonitor(
                        settings,
                        active.route,
                        plan.quantity,
                        target_exit_spread_bps,
                        public_adapters,
                        typed_adapters,
                        instruments,
                        initial_funding,
                        Path(settings.storage.sqlite_path),
                        pair_action_id=active.pair_action_id,
                        direction=monitor_parameters[0],
                        effective_stop_bps=monitor_parameters[1],
                        projected_route_loss_usdt=monitor_parameters[2],
                        projected_portfolio_loss_usdt=monitor_parameters[3],
                        route_hard_loss_usdt=monitor_parameters[4],
                        portfolio_hard_loss_usdt=monitor_parameters[5],
                        holding_deadline=monitor_parameters[6],
                        aggressive_policy=aggressive_policy,
                    )
                )
        if portfolio_close_event is not None:
            monitor = PortfolioInterruptMonitor(monitor, portfolio_close_event)
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
            portfolio_mode=portfolio_mode,
            aggressive_policy=aggressive_policy,
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


def _fresh_supervisor_handoff(active: LiveJournalAction, *, now: datetime | None = None) -> bool:
    observed = now or datetime.now(UTC)
    age_seconds = (observed - active.created_at).total_seconds()
    opening_ids = active.risk_reservation.get("opening_client_order_ids")
    return (
        active.risk_reservation.get("supervisor_intent") == "LIVE_CANARY"
        and active.risk_reservation.get("supervisor_queued") is True
        and active.risk_reservation.get("qualification_hash") == active.qualification_hash
        and isinstance(opening_ids, dict)
        and set(map(str, opening_ids.values())) == {leg.client_order_id for leg in active.legs}
        and active.recovery_action is None
        and 0 <= age_seconds <= 30
        and all(
            not leg.submit_attempted
            and leg.order_id is None
            and leg.status is None
            and leg.filled_base_quantity == 0
            for leg in active.legs
        )
    )


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
    """Recover one canary or one compatible aggressive multi-tranche portfolio."""
    if not active:
        return object()
    if len(active) == 1:
        return await recover_active_canary(settings, journal, active[0])
    if _compatible_aggressive_pilot_portfolio(active):
        results: list[CanaryRunEvidence] = []
        priorities = {
            LiveActionState.CLOSING: 0,
            LiveActionState.RECOVERING: 1,
            LiveActionState.UNKNOWN: 1,
            LiveActionState.PARTIAL: 2,
            LiveActionState.FILLED: 3,
            LiveActionState.HEDGED: 4,
            LiveActionState.PREPARED: 5,
        }
        route_close_event = asyncio.Event()
        route_watcher_stop = asyncio.Event()
        route_watcher = (
            asyncio.create_task(
                _watch_aggressive_route_safety(
                    settings,
                    journal,
                    active[0].route,
                    route_close_event,
                    route_watcher_stop,
                ),
                name="aggressive-route-safety-watcher",
            )
            if all(item.state == LiveActionState.HEDGED for item in active)
            else None
        )
        try:
            for snapshot in sorted(
                active,
                key=lambda item: (
                    priorities.get(item.state, 99),
                    item.created_at,
                    item.pair_action_id,
                ),
            ):
                current = await journal.load(snapshot.pair_action_id)
                if current is None or current.state == LiveActionState.FLAT:
                    continue
                result = await _resume_active_canary(
                    settings,
                    journal,
                    current,
                    portfolio_mode=True,
                    portfolio_close_event=route_close_event,
                )
                results.append(result)
                if not result.success and result.terminal_state != LiveActionState.PREPARED:
                    raise RuntimeError("aggressive portfolio recovery failed closed")
        except Exception:
            flattened = await OnDemandLiveControlPlane(settings).emergency_flatten()
            if not flattened.success:
                raise RuntimeError(
                    flattened.instruction
                    or "aggressive portfolio emergency flatten did not verify stable FLAT"
                ) from None
            return flattened
        finally:
            route_watcher_stop.set()
            if route_watcher is not None:
                try:
                    await route_watcher
                except Exception:
                    if not route_close_event.is_set():
                        raise
        return tuple(results)
    control_result = await OnDemandLiveControlPlane(settings).emergency_flatten()
    if not control_result.success:
        raise RuntimeError(
            control_result.instruction or "multi-action account recovery did not verify stable FLAT"
        )
    return control_result


def _compatible_aggressive_pilot_portfolio(
    actions: tuple[LiveJournalAction, ...],
) -> bool:
    if not 2 <= len(actions) <= 5:
        return False
    first = actions[0]
    expected_identity = (
        first.route,
        first.qualification_hash,
        first.risk_reservation.get("aggressive_binding_sha256"),
        first.risk_reservation.get("strategy_profile_sha256"),
    )
    levels: set[int] = set()
    for action in actions:
        reservation = action.risk_reservation
        try:
            level = int(str(reservation["level_index"]))
        except (KeyError, ValueError):
            return False
        identity = (
            action.route,
            action.qualification_hash,
            reservation.get("aggressive_binding_sha256"),
            reservation.get("strategy_profile_sha256"),
        )
        if (
            reservation.get("strategy") != "AGGRESSIVE_SYMBIOSIS_V1"
            or reservation.get("stage") != AggressiveLaptopLiveStage.PILOT_A.value
            or identity != expected_identity
            or not 1 <= level <= 5
            or level in levels
            or action.state
            not in {
                LiveActionState.PREPARED,
                LiveActionState.PARTIAL,
                LiveActionState.FILLED,
                LiveActionState.HEDGED,
                LiveActionState.CLOSING,
                LiveActionState.RECOVERING,
            }
        ):
            return False
        levels.add(level)
    return True


async def run_canary_once(
    settings: Settings,
    config_path: Path,
    qualification_path: Path,
    repo_root: Path,
    owner_confirmation: str,
    aggressive_intent: AggressiveLiveIntentEnvelope | None = None,
    aggressive_binding: AggressiveQualificationBinding | None = None,
    aggressive_stage: AggressiveLaptopLiveStage = AggressiveLaptopLiveStage.CANARY,
) -> CanaryRunEvidence:
    if (aggressive_intent is None) != (aggressive_binding is None):
        return _denied(ReasonCode.CANARY_POLICY_VIOLATION)
    if aggressive_intent is None and aggressive_stage != AggressiveLaptopLiveStage.CANARY:
        return _denied(ReasonCode.CANARY_POLICY_VIOLATION)
    required_confirmation = (
        OWNER_CONFIRMATION
        if aggressive_stage == AggressiveLaptopLiveStage.CANARY
        else PILOT_A_OWNER_CONFIRMATION
    )
    if owner_confirmation != required_confirmation:
        return _denied(ReasonCode.OWNER_CONFIRMATION_MISSING)
    state_path = Path(settings.storage.sqlite_path)
    await initialise_state(state_path)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    active_actions = await journal.active_actions()
    if aggressive_stage == AggressiveLaptopLiveStage.CANARY:
        if len(active_actions) == 1:
            return await _resume_active_canary(settings, journal, active_actions[0])
        if active_actions:
            return _denied(ReasonCode.RECONCILIATION_INCOMPLETE, active_actions[0].route)
    elif any(
        action.state != LiveActionState.HEDGED
        or action.risk_reservation.get("strategy") != "AGGRESSIVE_SYMBIOSIS_V1"
        or action.risk_reservation.get("stage") != AggressiveLaptopLiveStage.PILOT_A.value
        for action in active_actions
    ):
        return _denied(ReasonCode.RECONCILIATION_INCOMPLETE, active_actions[0].route)
    risk_stage = await read_risk_stage(state_path)
    expected_risk_stage = (
        RiskStage.CANARY
        if aggressive_stage == AggressiveLaptopLiveStage.CANARY
        else RiskStage.PILOT_A
    )
    if risk_stage.stage != expected_risk_stage or risk_stage.completion_frozen:
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
    if aggressive_intent is not None and aggressive_binding is not None:
        candidate = aggressive_intent.intent
        try:
            aggressive_route = DirectedRouteKey(
                candidate.base,
                Venue(candidate.long_venue),
                Venue(candidate.short_venue),
            )
        except ValueError:
            return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route)
        intent_age = (datetime.now(UTC) - candidate.decided_at.astimezone(UTC)).total_seconds()
        if (
            not aggressive_binding.accepted
            or aggressive_binding.binding_sha256 != aggressive_intent.aggressive_binding_sha256
            or aggressive_binding.qualification_hash != evidence.qualification_hash
            or aggressive_intent.qualification_hash != evidence.qualification_hash
            or not 1
            <= candidate.level_index
            <= (1 if aggressive_stage == AggressiveLaptopLiveStage.CANARY else 5)
            or intent_age < 0
            or intent_age > settings.live.canary_timeout_seconds
            or aggressive_route.base != route.base
            or {aggressive_route.long_venue, aggressive_route.short_venue}
            != {route.long_venue, route.short_venue}
        ):
            return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route)
        route = aggressive_route
        active_levels = {
            int(str(action.risk_reservation.get("level_index", 0))) for action in active_actions
        }
        if (
            candidate.level_index in active_levels
            or len(active_actions)
            >= (1 if aggressive_stage == AggressiveLaptopLiveStage.CANARY else 5)
            or any(
                action.route != route or action.qualification_hash != evidence.qualification_hash
                for action in active_actions
            )
        ):
            return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route)
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
    try:
        image_digest = resolve_runtime_artifact_digest(repo_root, config_path)
    except (OSError, ValueError, subprocess.SubprocessError):
        return _denied(ReasonCode.CURRENT_QUALIFICATION_MISSING, route)
    standard_policy = qualification_policy_from_settings(settings)
    accepted_policies = (
        (standard_policy, laptop_owner_exception_policy(settings))
        if laptop_owner_exception_authorized()
        else (standard_policy,)
    )
    qualification_valid, _ = qualification_is_current(
        evidence,
        repo_root,
        config_path,
        Path(settings.storage.parquet_dir),
        settings.live.qualification_max_age_seconds,
        expected_route=route,
        current_container_image_digest=image_digest,
        accepted_policies=accepted_policies,
    )
    if not qualification_valid:
        return _denied(ReasonCode.CURRENT_QUALIFICATION_MISSING, route)
    if evidence.policy == laptop_owner_exception_policy(settings):
        completed_exception_canaries = await journal.completed_actions_since(
            evidence.generated_at,
            evidence.qualification_hash,
        )
        if completed_exception_canaries:
            return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route)

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
        expected_quantity = (
            aggressive_intent.intent.quantity
            if aggressive_intent is not None
            else evidence.strategy.size_bucket_base_quantity
        )
        if (
            aggressive_stage == AggressiveLaptopLiveStage.CANARY and quantity != expected_quantity
        ) or (
            aggressive_stage == AggressiveLaptopLiveStage.PILOT_A and expected_quantity < quantity
        ):
            return _denied(ReasonCode.CANARY_POLICY_VIOLATION, route, quantity)
        quantity = expected_quantity
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
            active_actions or None,
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
        projected_stress = (
            max(
                economic.signal.cost.stressed_total_cost_usdt,
                aggressive_intent.intent.incremental_tranche_loss_usdt,
            )
            if aggressive_intent is not None
            else max(
                economic.signal.cost.stressed_total_cost_usdt,
                evidence.replay_shadow.maximum_adverse_excursion_usdt,
            )
        )
        try:
            existing_projected_stress = sum(
                (_effective_reserved_stress(action.risk_reservation) for action in active_actions),
                Decimal(0),
            )
        except ValueError:
            return _denied(ReasonCode.RISK_PREFLIGHT_FAILED, route, quantity)
        cumulative_projected_stress = existing_projected_stress + projected_stress
        notional = quantity * max(
            cast(Decimal, quote.entry_long_vwap),
            cast(Decimal, quote.entry_short_vwap),
        )
        risk = evaluate_canary_risk_from_private_state(
            route,
            states,
            notional,
            cumulative_projected_stress,
            pair_stress_limit_usdt=min(stage_limits.pair_usdt, Decimal("4.5")),
            portfolio_stress_limit_usdt=min(stage_limits.portfolio_usdt, Decimal("45")),
            free_margin_floor_ratio=settings.live.canary_free_margin_floor_ratio,
            effective_leverage_cap=stage_limits.leverage,
            exit_depth_sufficient=emergency_assessment.passed,
            allow_existing_matched_exposure=(aggressive_stage == AggressiveLaptopLiveStage.PILOT_A),
            maximum_routes=stage_limits.routes,
            maximum_tranches_per_route=stage_limits.tranches,
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
            len(active_actions) + 1,
            notional,
            notional,
            projected_stress,
            maximum_leverage,
            minimum_free_margin,
            existing_positions,
            existing_orders,
        )
        if aggressive_stage == AggressiveLaptopLiveStage.CANARY:
            policy = CanaryPolicy(
                route.base,
                route,
                stage_limits.pair_usdt,
                stage_limits.leverage,
                settings.live.canary_free_margin_floor_ratio,
            )
            policy_passed, policy_reason = policy.evaluate(action)
        else:
            policy_passed = (
                action.route == route
                and action.route.base == route.base
                and 1 <= action.tranche_count <= stage_limits.tranches
                and action.notional_usdt >= action.minimum_valid_notional_usdt > 0
                and Decimal(0) < action.projected_stressed_loss_usdt <= stage_limits.pair_usdt
                and action.maximum_effective_leverage <= stage_limits.leverage
                and action.minimum_stressed_free_margin_ratio
                >= settings.live.canary_free_margin_floor_ratio
                and action.existing_open_order_count == 0
                and reconciliation.consistent
            )
            policy_reason = None if policy_passed else ReasonCode.CANARY_POLICY_VIOLATION
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
            route_allowlisted=(
                evidence.route == route
                or (
                    aggressive_binding is not None
                    and route.base == evidence.route.base
                    and {route.long_venue, route.short_venue}
                    == {evidence.route.long_venue, evidence.route.short_venue}
                )
            ),
            canary_policy_passed=policy_passed,
            capability_preflight_passed=all_preflights_passed,
            account_preflight_passed=all_preflights_passed,
            market_data_preflight_passed=all(item.accepted for item in quality.values()),
            reconciliation_passed=(
                reconciliation.consistent
                and (
                    aggressive_stage == AggressiveLaptopLiveStage.PILOT_A
                    or reconciliation.flat_verified
                )
            ),
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
        if aggressive_intent is not None and aggressive_binding is not None:
            final_intent_age_ms = (
                datetime.now(UTC) - aggressive_intent.intent.decided_at.astimezone(UTC)
            ).total_seconds() * 1000
            if (
                final_intent_age_ms < 0
                or final_intent_age_ms > settings.live.canary_timeout_seconds * 1000
            ):
                return _denied(ReasonCode.BOOK_STALE, route, quantity)
            aggressive_plan = prepare_aggressive_live_plan(
                aggressive_intent.intent,
                aggressive_binding,
                instruments[route.long_venue],
                instruments[route.short_venue],
                long_protected_price=economic.long_protected_price,
                short_protected_price=economic.short_protected_price,
                stage=aggressive_stage,
                timeout_seconds=settings.live.canary_timeout_seconds,
            )
            route_opened_at = aggressive_intent.intent.decided_at.astimezone(UTC)
            if aggressive_stage == AggressiveLaptopLiveStage.PILOT_A:
                for existing in active_actions:
                    if existing.route != route:
                        continue
                    try:
                        existing_opened_at = datetime.fromisoformat(
                            str(
                                existing.risk_reservation.get(
                                    "route_opened_at",
                                    existing.risk_reservation["decided_at"],
                                )
                            )
                        ).astimezone(UTC)
                    except (KeyError, ValueError):
                        return _denied(ReasonCode.RISK_PREFLIGHT_FAILED, route, quantity)
                    route_opened_at = min(route_opened_at, existing_opened_at)
            plan = replace(
                aggressive_plan,
                risk_reservation={
                    **aggressive_plan.risk_reservation,
                    "route_opened_at": route_opened_at.isoformat(),
                    "hard_holding_deadline": (route_opened_at + timedelta(hours=24)).isoformat(),
                    "risk": risk.breakdown,
                    "qualification_hash": evidence.qualification_hash,
                    "supervisor_intent": "LIVE_CANARY",
                    "supervisor_queued": True,
                    "initial_funding_rates": {
                        venue.value: funding[venue].rate for venue in required_venues
                    },
                    "initial_funding_next_timestamp_ms": {
                        venue.value: funding[venue].next_funding_timestamp_ms
                        for venue in required_venues
                    },
                },
            )
        else:
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
                    "initial_funding_next_timestamp_ms": {
                        venue.value: funding[venue].next_funding_timestamp_ms
                        for venue in required_venues
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
