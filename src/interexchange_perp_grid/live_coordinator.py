from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from interexchange_perp_grid.aggressive_evaluator import AggressiveDecisionPolicy, CostReserves
from interexchange_perp_grid.aggressive_model import DivergenceDirection
from interexchange_perp_grid.aggressive_runtime import (
    ActualFillRiskInput,
    recompute_actual_fill_risk,
)
from interexchange_perp_grid.client_ids import (
    is_bot_client_order_id,
    venue_client_order_id,
)
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.live_reconciliation import (
    FlatBarrierPolicy,
    FlatBarrierResult,
    PrivateStateAdapter,
    ReconciliationReport,
    ReconciliationStatus,
    collect_private_states,
    combined_event_watermark,
    flat_barrier_failure_reason,
    reconcile_private_states,
    reconciliation_position_signature_sha256,
    wait_for_stable_flat,
    wait_for_stable_reconciliation,
)
from interexchange_perp_grid.private_domain import (
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.private_execution import translate_protected_order
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey


class CloseReason(StrEnum):
    HARD_STOP_OR_LOSS = "HARD_STOP_OR_LOSS"
    HARD_HOLDING_TIME = "HARD_HOLDING_TIME"
    TARGET_CONVERGENCE = "TARGET_CONVERGENCE"
    CANARY_TIMEOUT = "CANARY_TIMEOUT"
    RISK_DETERIORATION = "RISK_DETERIORATION"
    FUNDING_DETERIORATION = "FUNDING_DETERIORATION"
    STALE_DATA = "STALE_DATA"
    OPERATOR_CLOSE = "OPERATOR_CLOSE"
    EMERGENCY = "EMERGENCY"


@dataclass(frozen=True, slots=True)
class CanaryCloseSignals:
    target_converged: bool = False
    risk_deteriorated: bool = False
    funding_deteriorated: bool = False
    public_or_private_data_stale: bool = False
    operator_close_requested: bool = False
    emergency_active: bool = False
    hard_stop_or_loss: bool = False
    hard_holding_time: bool = False


def first_close_reason(signals: CanaryCloseSignals) -> CloseReason | None:
    ordered = (
        (signals.emergency_active, CloseReason.EMERGENCY),
        (signals.operator_close_requested, CloseReason.OPERATOR_CLOSE),
        (signals.public_or_private_data_stale, CloseReason.STALE_DATA),
        (signals.hard_stop_or_loss, CloseReason.HARD_STOP_OR_LOSS),
        (signals.hard_holding_time, CloseReason.HARD_HOLDING_TIME),
        (signals.risk_deteriorated, CloseReason.RISK_DETERIORATION),
        (signals.funding_deteriorated, CloseReason.FUNDING_DETERIORATION),
        (signals.target_converged, CloseReason.TARGET_CONVERGENCE),
    )
    return next((reason for active, reason in ordered if active), None)


class CanaryMonitor(Protocol):
    async def wait_for_close(self, timeout_seconds: int) -> CloseReason: ...


class ProtectionProvider(Protocol):
    async def price(
        self,
        venue: Venue,
        side: Side,
        quantity: Decimal,
        purpose: OrderPurpose,
    ) -> Decimal: ...


class CanaryVenueAdapter(PrivateStateAdapter, Protocol):
    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder: ...

    async def watch_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]: ...

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder | None: ...

    async def cancel_order(
        self,
        order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder: ...

    async def resolve_instrument(self, symbol: str) -> Instrument | None: ...

    async def list_instruments(self) -> tuple[Instrument, ...]: ...


@dataclass(frozen=True, slots=True)
class CanaryExecutionPlan:
    pair_action_id: str
    route: DirectedRouteKey
    tranche_id: str
    quantity: Decimal
    long_request: VenueOrderRequest
    short_request: VenueOrderRequest
    risk_reservation: dict[str, object]
    qualification_hash: str
    timeout_seconds: int
    activation_hash: str | None = None
    fast_live_preflight_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CanaryCycleResult:
    success: bool
    reason: ReasonCode | None
    orders_sent: int
    hedged: bool
    residual_delta: Decimal
    recovery_action: str | None
    close_reason: CloseReason | None
    terminal_state: LiveActionState
    reconciliation: ReconciliationReport | None
    owner_instruction: str | None
    flat_barrier_verified: bool = False
    flat_barrier_timed_out: bool = False
    flat_barrier_snapshots: int = 0
    flat_barrier_watermark: int = -1
    portfolio_reconciled: bool = False


class LiveCanaryCoordinator:
    """Durable paired open/recovery/close lifecycle; success requires private-verified FLAT."""

    def __init__(
        self,
        journal: LiveOrderJournal,
        adapters: Mapping[Venue, CanaryVenueAdapter],
        instruments: Mapping[Venue, Instrument] | Mapping[tuple[Venue, str], Instrument],
        protection: ProtectionProvider,
        monitor: CanaryMonitor,
        emergency_venue: Venue,
        *,
        terminal_timeout_seconds: Decimal = Decimal("2"),
        flat_barrier_policy: FlatBarrierPolicy | None = None,
        opening_gate: Callable[[CanaryExecutionPlan], Awaitable[bool]] | None = None,
        final_opening_gate: Callable[[], Awaitable[bool]] | None = None,
        portfolio_mode: bool = False,
        aggressive_policy: AggressiveDecisionPolicy | None = None,
    ) -> None:
        self._journal = journal
        self._adapters = dict(adapters)
        self._instruments = {
            (instrument.venue, instrument.symbol): instrument for instrument in instruments.values()
        }
        self._account_instruments = {
            venue: next(
                instrument
                for (instrument_venue, _), instrument in self._instruments.items()
                if instrument_venue == venue
            )
            for venue in self._adapters
        }
        self._protection = protection
        self._monitor = monitor
        self._emergency_venue = emergency_venue
        self._terminal_timeout_seconds = terminal_timeout_seconds
        self._flat_barrier_policy = flat_barrier_policy or FlatBarrierPolicy()
        self._opening_gate = opening_gate
        self._final_opening_gate = final_opening_gate
        self._portfolio_mode = portfolio_mode
        self._aggressive_policy = aggressive_policy
        self._orders_sent = 0
        self._sequence = 0

    async def prepare(self, plan: CanaryExecutionPlan) -> LiveJournalAction:
        """Persist an exact pair intent without performing any network submission."""
        await self._journal.initialise()
        existing = await self._journal.load(plan.pair_action_id)
        if existing is not None:
            return existing
        return await self._journal.prepare(
            plan.pair_action_id,
            plan.route,
            plan.tranche_id,
            plan.long_request,
            plan.short_request,
            {
                plan.route.long_venue: plan.quantity,
                plan.route.short_venue: plan.quantity,
            },
            {
                plan.route.long_venue: _required_price(plan.long_request),
                plan.route.short_venue: _required_price(plan.short_request),
            },
            plan.risk_reservation,
            plan.qualification_hash,
            activation_hash=plan.activation_hash,
            fast_live_preflight_sha256=plan.activation_hash,
            fast_live_preflight_expires_at=plan.fast_live_preflight_expires_at,
        )

    async def run(self, plan: CanaryExecutionPlan) -> CanaryCycleResult:
        await self._journal.initialise()
        action = await self._journal.load(plan.pair_action_id)
        if action is not None and action.state == LiveActionState.FLAT:
            barrier = await self._verify_stable_flat(action)
            report = barrier.report
            stable_flat_verified = barrier.verified
            if stable_flat_verified:
                action, barrier = await self._to_flat(
                    action,
                    "REVERIFIED_COMPLETED_FLAT",
                    barrier,
                )
            if (
                not barrier.verified
                and action.state != LiveActionState.QUARANTINED
                and (not stable_flat_verified or action.state != LiveActionState.FLAT)
            ):
                action = await self._quarantine(
                    action,
                    report,
                    flat_barrier_failure_reason(barrier).value,
                )
            return CanaryCycleResult(
                barrier.verified,
                None if barrier.verified else flat_barrier_failure_reason(barrier),
                0,
                False,
                report.residual_delta,
                action.recovery_action,
                None,
                action.state,
                report,
                None if barrier.verified else "Previously flat action no longer verifies flat.",
                barrier.verified,
                barrier.timed_out,
                barrier.consecutive_snapshots,
                barrier.event_watermark,
            )
        if action is None:
            action = await self.prepare(plan)
        if action.state == LiveActionState.PREPARED:
            opening_allowed = True
            if self._opening_gate is not None:
                try:
                    opening_allowed = await self._opening_gate(plan)
                except Exception:
                    opening_allowed = False
            if not opening_allowed:
                states = await collect_private_states(
                    self._adapters,
                    self._account_instruments,
                    reconciliation_trigger="PRE_SUBMIT_GATE_DENIED",
                )
                report = reconcile_private_states(
                    action,
                    states,
                    await self._journal.known_client_order_ids(),
                    set(self._adapters),
                )
                action = await self._quarantine(action, report, "OPENING_GATE_DENIED")
                return self._failed(
                    action,
                    ReasonCode.VENUE_QUARANTINED,
                    recovery_action="OPENING_GATE_DENIED",
                    reconciliation=report,
                )
            await self._journal.mark_submit_attempted(
                action.pair_action_id,
                (plan.long_request.client_order_id, plan.short_request.client_order_id),
            )
            final_opening_allowed = True
            if self._final_opening_gate is not None:
                try:
                    final_opening_allowed = await self._final_opening_gate()
                except Exception:
                    final_opening_allowed = False
            if not final_opening_allowed:
                current = await self._journal.load(action.pair_action_id)
                if current is not None:
                    action = current
                states = await collect_private_states(
                    self._adapters,
                    self._account_instruments,
                    reconciliation_trigger="FINAL_PRE_SUBMIT_GATE_DENIED",
                )
                report = reconcile_private_states(
                    action,
                    states,
                    await self._journal.known_client_order_ids(),
                    set(self._adapters),
                )
                action = await self._quarantine(action, report, "FINAL_OPENING_GATE_DENIED")
                return self._failed(
                    action,
                    ReasonCode.VENUE_QUARANTINED,
                    recovery_action="FINAL_OPENING_GATE_DENIED",
                    reconciliation=report,
                )
            self._orders_sent += 2
            long_order, short_order = await asyncio.gather(
                self._submit_and_resolve(
                    action.pair_action_id,
                    plan.long_request,
                    self._instrument(plan.route.long_venue, plan.long_request.symbol),
                ),
                self._submit_and_resolve(
                    action.pair_action_id,
                    plan.short_request,
                    self._instrument(plan.route.short_venue, plan.short_request.symbol),
                ),
            )
            action = await self._classify_open_pair(action.pair_action_id, long_order, short_order)
        else:
            action = await self._refresh_action(action)

        resuming_close = action.state == LiveActionState.CLOSING
        was_hedged = action.state == LiveActionState.HEDGED
        opening_recovery: str | None = action.recovery_action
        partial_hard_breach = False
        if action.state == LiveActionState.PARTIAL:
            action, partial_hard_breach = await self._record_partial_fill_risk(action)
        hedged = resuming_close or await self._is_hedged(action)
        if not hedged and not resuming_close:
            if partial_hard_breach:
                action = await self._journal.transition(
                    action.pair_action_id,
                    LiveActionState.RECOVERING,
                    {"reason": "ACTUAL_FILL_HARD_BREACH"},
                    residual_delta=_journal_delta(action),
                    recovery_action="ACTUAL_FILL_HARD_BREACH_REDUCE",
                )
                await self._close_exchange_positions(action, emergency=False)
                action = await self._refresh_action(action)
                opening_recovery = "ACTUAL_FILL_HARD_BREACH_REDUCE"
            else:
                action, opening_recovery = await self._recover_opening(action)
            hedged = await self._is_hedged(action)
            if not hedged:
                barrier = await self._verify_stable_flat(action)
                report = barrier.report
                if barrier.verified:
                    action, barrier = await self._to_flat(
                        action,
                        "OPEN_RECOVERY_FLATTENED",
                        barrier,
                    )
                    if barrier.verified:
                        return CanaryCycleResult(
                            False,
                            ReasonCode.FORCED_CLOSED,
                            self._orders_sent,
                            False,
                            report.residual_delta,
                            opening_recovery,
                            None,
                            action.state,
                            report,
                            None,
                            barrier.verified,
                            barrier.timed_out,
                            barrier.consecutive_snapshots,
                            barrier.event_watermark,
                        )
                if action.state != LiveActionState.QUARANTINED:
                    action = await self._quarantine(
                        action,
                        report,
                        flat_barrier_failure_reason(barrier).value,
                    )
                return self._failed(
                    action,
                    flat_barrier_failure_reason(barrier),
                    recovery_action=opening_recovery,
                    reconciliation=report,
                    flat_barrier=barrier,
                )

        if not resuming_close and action.state != LiveActionState.HEDGED:
            action = await self._advance_to_hedged(action)
        actual_hard_breach = False
        if action.state == LiveActionState.HEDGED:
            action, actual_hard_breach = await self._record_actual_fill_risk(action)
        if (
            self._portfolio_mode
            and not resuming_close
            and not was_hedged
            and not actual_hard_breach
        ):
            report = await self._verify(action)
            return CanaryCycleResult(
                True,
                None,
                self._orders_sent,
                True,
                report.residual_delta,
                opening_recovery,
                None,
                action.state,
                report,
                None,
                portfolio_reconciled=report.consistent,
            )
        close_reason = (
            CloseReason.EMERGENCY
            if resuming_close
            else CloseReason.HARD_STOP_OR_LOSS
            if actual_hard_breach
            else await self._wait_close(plan.timeout_seconds)
        )
        if self._portfolio_mode and close_reason == CloseReason.CANARY_TIMEOUT:
            report = await self._verify(action)
            return CanaryCycleResult(
                report.consistent,
                None if report.consistent else ReasonCode.RECONCILIATION_INCOMPLETE,
                self._orders_sent,
                True,
                report.residual_delta,
                opening_recovery,
                None,
                action.state,
                report,
                None,
                portfolio_reconciled=report.consistent,
            )
        if self._portfolio_mode and close_reason not in {
            CloseReason.TARGET_CONVERGENCE,
            CloseReason.CANARY_TIMEOUT,
        }:
            route_actions = tuple(
                item for item in await self._journal.active_actions() if item.route == action.route
            )
            if len(route_actions) > 1:
                return await self._close_entire_route(
                    action,
                    route_actions,
                    close_reason,
                    opening_recovery,
                )
        if not resuming_close:
            action = await self._require_action(action.pair_action_id)
            if action.state == LiveActionState.HEDGED:
                action = await self._journal.transition(
                    action.pair_action_id,
                    LiveActionState.CLOSING,
                    {"close_reason": close_reason.value},
                    residual_delta=Decimal(0),
                )
            elif action.state not in {
                LiveActionState.CLOSING,
                LiveActionState.RECOVERING,
                LiveActionState.QUARANTINED,
            }:
                raise RuntimeError(f"unexpected live-control state {action.state.value}")
        await self._close_exchange_positions(action, emergency=False)
        action = await self._require_action(action.pair_action_id)
        active_before_terminal = await self._journal.active_actions()
        partial_portfolio_close = self._portfolio_mode and len(active_before_terminal) > 1
        barrier = (
            await self._verify_stable_portfolio()
            if partial_portfolio_close
            else await self._verify_stable_flat(action)
        )
        report = barrier.report
        recovery_action: str | None = opening_recovery
        if not barrier.verified and partial_portfolio_close:
            raise RuntimeError("scoped tranche close did not reconcile the remaining portfolio")
        if not barrier.verified:
            action = await self._journal.transition(
                action.pair_action_id,
                LiveActionState.RECOVERING,
                {"reason": "CLOSE_NOT_FLAT"},
                residual_delta=report.residual_delta,
                recovery_action="EMERGENCY_FLATTEN",
            )
            recovery_action = "EMERGENCY_FLATTEN"
            await self._cancel_all_bot_orders()
            await self._close_exchange_positions(action, emergency=True)
            action = await self._require_action(action.pair_action_id)
            barrier = await self._verify_stable_flat(action)
            report = barrier.report
        if barrier.verified:
            action, barrier = (
                await self._to_portfolio_flat(
                    action,
                    active_before_terminal,
                    "EXCHANGE_VERIFIED_SCOPED_CLOSE",
                    barrier,
                )
                if partial_portfolio_close
                else await self._to_flat(action, "EXCHANGE_VERIFIED_FLAT", barrier)
            )
        if not barrier.verified:
            if action.state != LiveActionState.QUARANTINED:
                action = await self._quarantine(
                    action,
                    report,
                    flat_barrier_failure_reason(barrier).value,
                )
            return self._failed(
                action,
                flat_barrier_failure_reason(barrier),
                hedged=True,
                recovery_action=recovery_action,
                close_reason=close_reason,
                reconciliation=report,
                flat_barrier=barrier,
            )
        return CanaryCycleResult(
            True,
            None,
            self._orders_sent,
            True,
            Decimal(0),
            recovery_action,
            close_reason,
            action.state,
            report,
            None,
            flat_barrier_verified=barrier.verified and not partial_portfolio_close,
            flat_barrier_timed_out=barrier.timed_out,
            flat_barrier_snapshots=barrier.consecutive_snapshots,
            flat_barrier_watermark=barrier.event_watermark,
            portfolio_reconciled=partial_portfolio_close,
        )

    async def _close_entire_route(
        self,
        action: LiveJournalAction,
        route_actions: tuple[LiveJournalAction, ...],
        close_reason: CloseReason,
        opening_recovery: str | None,
    ) -> CanaryCycleResult:
        """Risk-reduce every tranche on a route before committing any one FLAT."""
        closing: list[LiveJournalAction] = []
        for snapshot in route_actions:
            current = await self._require_action(snapshot.pair_action_id)
            if current.state == LiveActionState.HEDGED:
                current = await self._journal.transition(
                    current.pair_action_id,
                    LiveActionState.CLOSING,
                    {
                        "close_reason": close_reason.value,
                        "scope": "ENTIRE_ROUTE",
                    },
                    residual_delta=Decimal(0),
                )
            if current.state not in {LiveActionState.CLOSING, LiveActionState.RECOVERING}:
                raise RuntimeError(
                    "route-wide close encountered a non-reducible durable action state"
                )
            closing.append(current)
        await asyncio.gather(
            *(self._close_exchange_positions(item, emergency=False) for item in closing)
        )
        barrier = await self._verify_stable_portfolio()
        if not barrier.verified:
            raise RuntimeError("route-wide close did not reconcile the remaining portfolio")
        observed_before = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if observed_before != barrier.event_watermark:
            raise RuntimeError("route-wide close lost its private-event barrier")
        journal_watermark = await self._journal.event_watermark()
        action_ids = tuple(item.pair_action_id for item in route_actions)
        commit = await self._journal.commit_flat_barrier_many(
            action_ids,
            journal_watermark,
            {
                "reason": "EXCHANGE_VERIFIED_ROUTE_FLAT",
                "close_reason": close_reason.value,
                "reconciliation_position_signature_sha256": (
                    reconciliation_position_signature_sha256(barrier.report)
                ),
            },
        )
        observed_after = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        committed = commit.committed and observed_after == barrier.event_watermark
        terminal = next(
            (item for item in commit.actions if item.pair_action_id == action.pair_action_id),
            None,
        )
        if not committed or terminal is None:
            raise RuntimeError("route-wide FLAT commit lost its exact reconciliation barrier")
        return CanaryCycleResult(
            True,
            None,
            self._orders_sent,
            True,
            Decimal(0),
            opening_recovery,
            close_reason,
            terminal.state,
            barrier.report,
            None,
            flat_barrier_verified=True,
            flat_barrier_timed_out=barrier.timed_out,
            flat_barrier_snapshots=barrier.consecutive_snapshots,
            flat_barrier_watermark=barrier.event_watermark,
            portfolio_reconciled=True,
        )

    async def _classify_open_pair(
        self,
        pair_action_id: str,
        long_order: PrivateOrder,
        short_order: PrivateOrder,
    ) -> LiveJournalAction:
        statuses = {long_order.status, short_order.status}
        fills = (long_order.filled_base_quantity, short_order.filled_base_quantity)
        if PrivateOrderStatus.UNKNOWN in statuses:
            state = LiveActionState.UNKNOWN
        elif any(fill > 0 for fill in fills) and (
            fills[0] != fills[1] or PrivateOrderStatus.PARTIAL in statuses
        ):
            state = LiveActionState.PARTIAL
        elif fills[0] == fills[1] and fills[0] > 0:
            state = LiveActionState.FILLED
        elif PrivateOrderStatus.REJECTED in statuses:
            state = LiveActionState.REJECTED
        else:
            state = LiveActionState.REJECTED
        return await self._journal.transition(
            pair_action_id,
            state,
            {
                "long_status": long_order.status.value,
                "short_status": short_order.status.value,
                "long_fill": str(fills[0]),
                "short_fill": str(fills[1]),
            },
            residual_delta=fills[0] - fills[1],
        )

    async def _submit_and_resolve(
        self,
        pair_action_id: str,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        adapter = self._adapters[request.venue]
        try:
            order = await adapter.submit_order(request, instrument)
        except Exception:
            order = None
        if order is not None:
            await self._record(pair_action_id, order)
        if order is not None and order.status in {
            PrivateOrderStatus.FILLED,
            PrivateOrderStatus.PARTIAL,
            PrivateOrderStatus.REJECTED,
            PrivateOrderStatus.CANCELLED,
        }:
            return order
        stream_order: PrivateOrder | None = None
        try:
            streamed = await asyncio.wait_for(
                adapter.watch_orders(instrument),
                timeout=float(self._terminal_timeout_seconds),
            )
            stream_order = _latest_matching(streamed, request.client_order_id)
        except Exception:
            stream_order = None
        try:
            resolved = stream_order or await adapter.find_order_by_client_id(
                request.client_order_id,
                instrument,
            )
        except Exception:
            resolved = None
        if resolved is None:
            unknown = _unknown_order(request, instrument)
            await self._record(pair_action_id, unknown)
            return unknown
        order = resolved
        await self._record(pair_action_id, order)
        if order.status == PrivateOrderStatus.OPEN and order.order_id is not None:
            with contextlib.suppress(Exception):
                await adapter.cancel_order(order.order_id, instrument)
            try:
                late = await adapter.find_order_by_client_id(
                    request.client_order_id,
                    instrument,
                )
            except Exception:
                late = None
            if late is not None:
                order = late
                await self._record(pair_action_id, order)
        return order

    async def _record(self, pair_action_id: str, order: PrivateOrder) -> None:
        key = (
            f"{order.order_id or 'none'}:{order.status.value}:"
            f"{order.filled_base_quantity}:{order.observed_at.isoformat()}"
        )
        await self._journal.record_order_event(pair_action_id, order, key)

    async def _refresh_action(self, action: LiveJournalAction) -> LiveJournalAction:
        states = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="POST_SUBMIT_OR_UNKNOWN",
        )
        known = {leg.client_order_id for leg in action.legs}
        for state in states.values():
            for order in (*state.open_orders, *state.recent_orders):
                if order.client_order_id in known:
                    await self._record(action.pair_action_id, order)
        return await self._require_action(action.pair_action_id)

    async def _is_hedged(self, action: LiveJournalAction) -> bool:
        report = await self._verify(action)
        gross = sum((abs(value) for value in report.actual_signed_positions.values()), Decimal(0))
        locally_owned = _journal_signed_positions(action)
        local_gross = sum((abs(value) for value in locally_owned.values()), Decimal(0))
        return (
            report.status == ReconciliationStatus.CONSISTENT
            and report.snapshots_complete
            and not report.unknown_client_order_ids
            and report.open_position_count > 0
            and gross > 0
            and report.residual_delta == 0
            and report.actual_signed_positions == report.expected_signed_positions
            and local_gross > 0
            and any(value > 0 for value in locally_owned.values())
            and any(value < 0 for value in locally_owned.values())
            and _journal_delta(action) == 0
        )

    async def _recover_opening(
        self,
        action: LiveJournalAction,
    ) -> tuple[LiveJournalAction, str]:
        if action.state != LiveActionState.RECOVERING:
            action = await self._journal.transition(
                action.pair_action_id,
                LiveActionState.RECOVERING,
                {"reason": "OPEN_RESIDUAL"},
                residual_delta=_journal_delta(action),
            )
        action = await self._refresh_action(action)
        delta = _journal_delta(action)
        if delta == 0:
            return action, "NO_RESIDUAL"
        top_up_venue = action.route.short_venue if delta > 0 else action.route.long_venue
        top_up_side = Side.SELL if delta > 0 else Side.BUY
        await self._attempt_recovery_leg(
            action,
            top_up_venue,
            top_up_side,
            abs(delta),
            OrderPurpose.NORMAL_OPEN,
            "TOP_UP_SMALLER_LEG",
            emergency=False,
        )
        action = await self._refresh_action(action)
        if _journal_delta(action) == 0:
            return action, "TOP_UP_SMALLER_LEG"

        delta = _journal_delta(action)
        reduce_venue = action.route.long_venue if delta > 0 else action.route.short_venue
        reduce_side = Side.SELL if delta > 0 else Side.BUY
        await self._attempt_recovery_leg(
            action,
            reduce_venue,
            reduce_side,
            abs(delta),
            OrderPurpose.NORMAL_CLOSE,
            "REDUCE_LARGER_LEG",
            emergency=False,
        )
        action = await self._refresh_action(action)
        if _journal_delta(action) == 0:
            return action, "REDUCE_LARGER_LEG"

        delta = _journal_delta(action)
        emergency_side = Side.SELL if delta > 0 else Side.BUY
        await self._attempt_recovery_leg(
            action,
            self._emergency_venue,
            emergency_side,
            abs(delta),
            OrderPurpose.EMERGENCY_HEDGE,
            "THIRD_VENUE_HEDGE",
            emergency=False,
        )
        action = await self._refresh_action(action)
        if _journal_delta(action) == 0:
            return action, "THIRD_VENUE_HEDGE"

        await self._cancel_all_bot_orders()
        await self._close_exchange_positions(action, emergency=True)
        return await self._require_action(action.pair_action_id), "EMERGENCY_FLATTEN"

    async def _attempt_recovery_leg(
        self,
        action: LiveJournalAction,
        venue: Venue,
        side: Side,
        quantity: Decimal,
        purpose: OrderPurpose,
        label: str,
        *,
        emergency: bool,
    ) -> PrivateOrder | None:
        try:
            return await self._submit_recovery_leg(
                action,
                venue,
                side,
                quantity,
                purpose,
                label,
                emergency=emergency,
            )
        except Exception:
            return None

    async def _submit_recovery_leg(
        self,
        action: LiveJournalAction,
        venue: Venue,
        side: Side,
        quantity: Decimal,
        purpose: OrderPurpose,
        label: str,
        *,
        emergency: bool,
        symbol: str | None = None,
    ) -> PrivateOrder:
        instrument = (
            self._instrument(venue, symbol)
            if symbol is not None
            else self._base_instrument(venue, action.route.base)
        )
        self._sequence += 1
        client_id = venue_client_order_id(
            action.pair_action_id,
            f"{label}{venue.value}{side.value}",
            self._sequence,
        )
        protected = (
            None if emergency else await self._protection.price(venue, side, quantity, purpose)
        )
        intent = ExecutionIntent(
            client_id,
            venue,
            side,
            purpose,
            quantity,
            protected,
            emergency,
        )
        request = translate_protected_order(intent, instrument)
        await self._journal.append_order_leg(
            action.pair_action_id,
            request,
            quantity,
            protected,
        )
        await self._journal.mark_leg_submit_attempted(action.pair_action_id, client_id)
        self._orders_sent += 1
        return await self._submit_and_resolve(action.pair_action_id, request, instrument)

    async def _close_exchange_positions(
        self,
        action: LiveJournalAction,
        *,
        emergency: bool,
    ) -> None:
        if emergency:
            states = await collect_private_states(
                self._adapters,
                self._account_instruments,
                reconciliation_trigger="PRE_CLOSE",
            )
            positions = tuple(position for state in states.values() for position in state.positions)
            close_requests = tuple(
                (
                    position.venue,
                    Side.SELL if position.side == Side.BUY else Side.BUY,
                    position.base_quantity,
                    position.symbol,
                )
                for position in positions
            )
        else:
            signed_positions = _journal_signed_positions(action)
            close_requests = tuple(
                (
                    venue,
                    Side.SELL if quantity > 0 else Side.BUY,
                    abs(quantity),
                    self._base_instrument(venue, action.route.base).symbol,
                )
                for venue, quantity in signed_positions.items()
                if quantity != 0
            )
        await asyncio.gather(
            *(
                self._submit_recovery_leg(
                    action,
                    venue,
                    side,
                    quantity,
                    OrderPurpose.EMERGENCY_CLOSE if emergency else OrderPurpose.NORMAL_CLOSE,
                    "EMERGENCY_FLATTEN" if emergency else "CLOSE",
                    emergency=emergency,
                    symbol=symbol,
                )
                for venue, side, quantity, symbol in close_requests
            ),
            return_exceptions=True,
        )

    async def _cancel_all_bot_orders(self) -> None:
        states = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="PRE_CANCEL",
        )
        await asyncio.gather(
            *(
                self._cancel_one(order)
                for state in states.values()
                for order in state.open_orders
                if is_bot_client_order_id(order.client_order_id) and order.order_id is not None
            ),
            return_exceptions=True,
        )

    async def _cancel_one(self, order: PrivateOrder) -> None:
        assert order.order_id is not None
        await self._adapters[order.venue].cancel_order(
            order.order_id,
            self._instrument(order.venue, order.symbol),
        )

    async def _verify(self, action: LiveJournalAction) -> ReconciliationReport:
        refreshed = await self._refresh_action(action)
        expected: LiveJournalAction | tuple[LiveJournalAction, ...] = refreshed
        if self._portfolio_mode:
            expected = await self._journal.active_actions()
        states = await collect_private_states(self._adapters, self._account_instruments)
        return reconcile_private_states(
            expected,
            states,
            await self._journal.known_client_order_ids(),
            set(self._adapters),
        )

    async def _verify_stable_portfolio(self) -> FlatBarrierResult:
        async def report_factory() -> ReconciliationReport:
            actions = await self._journal.active_actions()
            states = await collect_private_states(self._adapters, self._account_instruments)
            return reconcile_private_states(
                actions,
                states,
                await self._journal.known_client_order_ids(),
                set(self._adapters),
            )

        return await wait_for_stable_reconciliation(
            report_factory,
            lambda: combined_event_watermark(
                self._adapters,
                self._journal.event_watermark,
            ),
            self._flat_barrier_policy,
        )

    async def _verify_stable_flat(self, action: LiveJournalAction) -> FlatBarrierResult:
        async def report_factory() -> ReconciliationReport:
            current = await self._require_action(action.pair_action_id)
            return await self._verify(current)

        return await wait_for_stable_flat(
            report_factory,
            lambda: combined_event_watermark(
                self._adapters,
                self._journal.event_watermark,
            ),
            self._flat_barrier_policy,
        )

    async def _record_actual_fill_risk(
        self,
        action: LiveJournalAction,
    ) -> tuple[LiveJournalAction, bool]:
        reservation = action.risk_reservation
        if reservation.get("strategy") != "AGGRESSIVE_SYMBIOSIS_V1":
            return action, False
        if self._aggressive_policy is None:
            raise RuntimeError("aggressive actual-fill risk policy is unavailable")
        orders = await self._journal.latest_order_events(action.pair_action_id)
        signed_positions = _journal_signed_positions(action)
        long_quantity = signed_positions.get(action.route.long_venue, Decimal(0))
        short_quantity = -signed_positions.get(action.route.short_venue, Decimal(0))
        if long_quantity <= 0 or long_quantity != short_quantity:
            raise RuntimeError("aggressive actual-fill risk requires a matched paired position")

        def weighted_price(venue: Venue, side: Side) -> Decimal:
            fills = tuple(
                order
                for order in orders
                if order.venue == venue
                and order.side == side
                and order.filled_base_quantity > 0
                and order.average_price is not None
            )
            quantity = sum((order.filled_base_quantity for order in fills), Decimal(0))
            if quantity <= 0:
                raise RuntimeError("aggressive actual-fill price evidence is incomplete")
            return (
                sum(
                    (
                        order.filled_base_quantity * order.average_price
                        for order in fills
                        if order.average_price is not None
                    ),
                    Decimal(0),
                )
                / quantity
            )

        if any(order.fee_usdt is None for order in orders if order.filled_base_quantity > 0):
            raise RuntimeError("aggressive actual-fill fee evidence is incomplete")
        actual_fees = sum(
            (
                order.fee_usdt
                for order in orders
                if order.filled_base_quantity > 0 and order.fee_usdt is not None
            ),
            Decimal(0),
        )
        try:
            direction = DivergenceDirection(str(reservation["direction"]))
            effective_stop = Decimal(str(reservation["effective_stop_bps"]))
            raw_intent = reservation["aggressive_intent"]
            if not isinstance(raw_intent, dict) or not isinstance(raw_intent.get("reserves"), dict):
                raise ValueError("aggressive reserve breakdown is unavailable")
            reserve_values = raw_intent["reserves"]
            assert isinstance(reserve_values, dict)
            explicit_reserves = CostReserves(
                **{str(key): Decimal(str(value)) for key, value in reserve_values.items()}
            ).total()
            adverse_funding = Decimal(str(raw_intent["adverse_funding_reserve_usdt"]))
            remaining_close_fees = Decimal(str(raw_intent["remaining_close_fees_usdt"]))
            route_hard_limit = Decimal(str(reservation["route_hard_loss_usdt"]))
            portfolio_hard_limit = Decimal(str(reservation["portfolio_hard_loss_usdt"]))
        except (KeyError, TypeError, ValueError, ArithmeticError) as error:
            raise RuntimeError("aggressive actual-fill reservation is incomplete") from error
        if (
            not adverse_funding.is_finite()
            or adverse_funding < 0
            or not remaining_close_fees.is_finite()
            or remaining_close_fees < 0
        ):
            raise RuntimeError("aggressive future cost reserve is invalid")
        active = await self._journal.active_actions()

        def effective_stress(other: LiveJournalAction) -> Decimal:
            actual = other.risk_reservation.get("actual_fill_risk")
            if isinstance(actual, dict) and "incremental_stress_usdt" in actual:
                return max(
                    Decimal(str(other.risk_reservation["projected_stress_usdt"])),
                    Decimal(str(actual["incremental_stress_usdt"])),
                )
            return Decimal(str(other.risk_reservation["projected_stress_usdt"]))

        existing_route = sum(
            (
                effective_stress(other)
                for other in active
                if other.pair_action_id != action.pair_action_id and other.route == action.route
            ),
            Decimal(0),
        )
        existing_portfolio = sum(
            (
                effective_stress(other)
                for other in active
                if other.pair_action_id != action.pair_action_id
            ),
            Decimal(0),
        )
        result = recompute_actual_fill_risk(
            ActualFillRiskInput(
                direction=direction,
                base_quantity=long_quantity,
                long_fill_price=weighted_price(action.route.long_venue, Side.BUY),
                short_fill_price=weighted_price(action.route.short_venue, Side.SELL),
                actual_fees_usdt=actual_fees,
                adverse_funding_usdt=adverse_funding,
                other_reserves_usdt=explicit_reserves + remaining_close_fees,
                effective_stop_bps=effective_stop,
                existing_route_loss_usdt=existing_route,
                existing_portfolio_loss_usdt=existing_portfolio,
            ),
            self._aggressive_policy,
        )
        incremental = result.projected_route_loss_usdt - existing_route
        watermark = await self._journal.event_watermark()
        actual_payload: dict[str, object] = {
            "incremental_stress_usdt": str(incremental),
            "route_total_usdt": str(result.projected_route_loss_usdt),
            "portfolio_total_usdt": str(result.projected_portfolio_loss_usdt),
            "actual_entry_spread_bps": str(result.actual_entry_spread_bps),
            "fill_event_watermark": watermark,
        }
        updated = await self._journal.update_actual_risk(
            action.pair_action_id,
            watermark,
            actual_payload,
        )
        authoritative = updated.risk_reservation.get("actual_fill_risk")
        if not isinstance(authoritative, dict):
            raise RuntimeError("authoritative actual-fill risk was not persisted")
        try:
            hard_breach = (
                Decimal(str(authoritative["route_total_usdt"])) >= route_hard_limit
                or Decimal(str(authoritative["portfolio_total_usdt"])) >= portfolio_hard_limit
            )
        except (KeyError, ValueError, ArithmeticError) as error:
            raise RuntimeError("authoritative actual-fill risk is incomplete") from error
        return updated, hard_breach

    async def _record_partial_fill_risk(
        self,
        action: LiveJournalAction,
    ) -> tuple[LiveJournalAction, bool]:
        """Persist a deliberately severe risk bound before any opening top-up.

        An unmatched fill is directional exposure.  Until it is paired or
        reduced, reserve its entire filled notional in addition to the planned
        route stress.  This makes the decision fail closed before another
        opening order can increase exposure.
        """
        reservation = action.risk_reservation
        if reservation.get("strategy") != "AGGRESSIVE_SYMBIOSIS_V1":
            return action, False
        orders = await self._journal.latest_order_events(action.pair_action_id)
        fills = tuple(order for order in orders if order.filled_base_quantity > 0)
        if not fills:
            return action, False
        if any(order.average_price is None or order.fee_usdt is None for order in fills):
            raise RuntimeError("aggressive partial-fill evidence is incomplete")
        signed = _journal_signed_positions(action)
        unmatched_quantity = abs(sum(signed.values(), Decimal(0)))
        if unmatched_quantity <= 0:
            return action, False
        maximum_fill_price = max(
            order.average_price for order in fills if order.average_price is not None
        )
        actual_fees = sum(
            (order.fee_usdt for order in fills if order.fee_usdt is not None),
            Decimal(0),
        )
        try:
            planned = Decimal(str(reservation["projected_stress_usdt"]))
            entry_spread = Decimal(
                str(reservation["aggressive_intent"]["executable_entry_spread_bps"])
            )
            route_hard_limit = Decimal(str(reservation["route_hard_loss_usdt"]))
            portfolio_hard_limit = Decimal(str(reservation["portfolio_hard_loss_usdt"]))
        except (KeyError, TypeError, ValueError, ArithmeticError) as error:
            raise RuntimeError("aggressive partial-fill reservation is incomplete") from error
        incremental = planned + unmatched_quantity * maximum_fill_price + actual_fees
        watermark = await self._journal.event_watermark()
        updated = await self._journal.update_actual_risk(
            action.pair_action_id,
            watermark,
            {
                "incremental_stress_usdt": str(incremental),
                "route_total_usdt": str(incremental),
                "portfolio_total_usdt": str(incremental),
                "actual_entry_spread_bps": str(entry_spread),
                "fill_event_watermark": watermark,
            },
        )
        authoritative = updated.risk_reservation.get("actual_fill_risk")
        if not isinstance(authoritative, dict):
            raise RuntimeError("authoritative partial-fill risk was not persisted")
        try:
            hard_breach = (
                Decimal(str(authoritative["route_total_usdt"])) >= route_hard_limit
                or Decimal(str(authoritative["portfolio_total_usdt"])) >= portfolio_hard_limit
            )
        except (KeyError, ValueError, ArithmeticError) as error:
            raise RuntimeError("authoritative partial-fill risk is incomplete") from error
        return updated, hard_breach

    async def _advance_to_hedged(self, action: LiveJournalAction) -> LiveJournalAction:
        if not await self._is_hedged(action):
            report = await self._verify(action)
            return await self._quarantine(action, report, "EXCHANGE_HEDGE_MISMATCH")
        if action.state == LiveActionState.RECOVERING:
            return await self._journal.transition(
                action.pair_action_id,
                LiveActionState.HEDGED,
                {"residual_delta": "0"},
                residual_delta=Decimal(0),
            )
        if action.state in {LiveActionState.FILLED, LiveActionState.PARTIAL}:
            return await self._journal.transition(
                action.pair_action_id,
                LiveActionState.HEDGED,
                {"residual_delta": "0"},
                residual_delta=Decimal(0),
            )
        raise RuntimeError(f"cannot mark {action.state.value} hedged")

    async def _wait_close(self, timeout_seconds: int) -> CloseReason:
        try:
            return await asyncio.wait_for(
                self._monitor.wait_for_close(timeout_seconds),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            return CloseReason.CANARY_TIMEOUT

    async def _to_flat(
        self,
        action: LiveJournalAction,
        reason: str,
        barrier: FlatBarrierResult,
    ) -> tuple[LiveJournalAction, FlatBarrierResult]:
        if not barrier.verified:
            raise RuntimeError(flat_barrier_failure_reason(barrier).value)
        current = await self._require_action(action.pair_action_id)
        if current.state not in {
            LiveActionState.FLAT,
            LiveActionState.QUARANTINED,
            LiveActionState.REJECTED,
            LiveActionState.RECOVERING,
            LiveActionState.CLOSING,
        }:
            current = await self._journal.transition(
                current.pair_action_id,
                LiveActionState.RECOVERING,
                {"reason": reason},
            )
        observed_before = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if observed_before != barrier.event_watermark:
            quarantined = await self._quarantine(current, barrier.report, reason)
            return (
                quarantined,
                FlatBarrierResult(
                    False,
                    barrier.report,
                    0,
                    observed_before,
                    False,
                    ReasonCode.FLAT_BARRIER_EVENT_RACE,
                ),
            )
        journal_watermark = await self._journal.event_watermark()
        commit = await self._journal.commit_flat_barrier(
            current.pair_action_id,
            journal_watermark,
            {"reason": reason},
        )
        if commit.action is None:
            raise RuntimeError("flat barrier commit lost the durable action")
        observed_after = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if commit.committed and observed_after == barrier.event_watermark:
            return commit.action, barrier
        failed_action = commit.action
        preserve_terminal_flat = False
        if (
            failed_action.state == LiveActionState.FLAT
            and not commit.committed
            and commit.event_watermark == journal_watermark
        ):
            preserve_terminal_flat = any(
                active.pair_action_id != failed_action.pair_action_id
                for active in await self._journal.active_actions()
            )
        if failed_action.state != LiveActionState.QUARANTINED and not preserve_terminal_flat:
            failed_action = await self._quarantine(failed_action, barrier.report, reason)
        return (
            failed_action,
            FlatBarrierResult(
                False,
                barrier.report,
                0,
                observed_after,
                False,
                ReasonCode.FLAT_BARRIER_EVENT_RACE,
            ),
        )

    async def _to_portfolio_flat(
        self,
        action: LiveJournalAction,
        active_actions: tuple[LiveJournalAction, ...],
        reason: str,
        barrier: FlatBarrierResult,
    ) -> tuple[LiveJournalAction, FlatBarrierResult]:
        if not barrier.verified:
            raise RuntimeError("portfolio reconciliation barrier is not verified")
        observed_before = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if observed_before != barrier.event_watermark:
            return action, FlatBarrierResult(
                False,
                barrier.report,
                0,
                observed_before,
                False,
                ReasonCode.FLAT_BARRIER_EVENT_RACE,
            )
        journal_watermark = await self._journal.event_watermark()
        commit = await self._journal.commit_reconciled_action(
            action.pair_action_id,
            tuple(item.pair_action_id for item in active_actions),
            journal_watermark,
            reconciliation_position_signature_sha256(barrier.report),
            {"reason": reason},
        )
        if commit.action is None:
            raise RuntimeError("portfolio reconciliation commit lost the durable action")
        observed_after = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if commit.committed and observed_after == barrier.event_watermark:
            return commit.action, barrier
        return commit.action, FlatBarrierResult(
            False,
            barrier.report,
            0,
            observed_after,
            False,
            ReasonCode.FLAT_BARRIER_EVENT_RACE,
        )

    async def _quarantine(
        self,
        action: LiveJournalAction,
        report: ReconciliationReport,
        recovery_action: str | None,
    ) -> LiveJournalAction:
        current = await self._require_action(action.pair_action_id)
        if current.state == LiveActionState.QUARANTINED:
            return current
        return await self._journal.transition(
            current.pair_action_id,
            LiveActionState.QUARANTINED,
            {
                "discrepancies": report.discrepancies,
                "unknown_client_order_ids": report.unknown_client_order_ids,
            },
            residual_delta=report.residual_delta,
            recovery_action=recovery_action,
        )

    async def _require_action(self, pair_action_id: str) -> LiveJournalAction:
        action = await self._journal.load(pair_action_id)
        if action is None:
            raise RuntimeError("live action journal record is missing")
        return action

    def _instrument(self, venue: Venue, symbol: str) -> Instrument:
        instrument = self._instruments.get((venue, symbol))
        if instrument is None:
            raise RuntimeError(f"instrument registry has no {venue.value}:{symbol}")
        return instrument

    def _base_instrument(self, venue: Venue, base: str) -> Instrument:
        matches = tuple(
            instrument
            for (instrument_venue, _), instrument in self._instruments.items()
            if instrument_venue == venue and instrument.base == base
        )
        if len(matches) != 1:
            raise RuntimeError(f"instrument registry has no unique {venue.value}:{base}")
        return matches[0]

    def _failed(
        self,
        action: LiveJournalAction,
        reason: ReasonCode,
        *,
        hedged: bool = False,
        recovery_action: str | None = None,
        close_reason: CloseReason | None = None,
        reconciliation: ReconciliationReport | None = None,
        flat_barrier: FlatBarrierResult | None = None,
    ) -> CanaryCycleResult:
        return CanaryCycleResult(
            False,
            reason,
            self._orders_sent,
            hedged,
            (
                reconciliation.residual_delta
                if reconciliation is not None
                else action.residual_delta
            ),
            recovery_action,
            close_reason,
            action.state,
            reconciliation,
            (
                "FAILED_QUARANTINED: disable live, inspect exchange orders/positions, "
                "cancel bot orders and flatten all involved subaccounts before restart."
                if action.state == LiveActionState.QUARANTINED
                else None
            ),
            flat_barrier.verified if flat_barrier is not None else False,
            flat_barrier.timed_out if flat_barrier is not None else False,
            flat_barrier.consecutive_snapshots if flat_barrier is not None else 0,
            flat_barrier.event_watermark if flat_barrier is not None else -1,
        )


def _required_price(request: VenueOrderRequest) -> Decimal:
    if request.price is None:
        raise ValueError("initial canary requests must have protected prices")
    return request.price


def _latest_matching(
    orders: tuple[PrivateOrder, ...],
    client_order_id: str,
) -> PrivateOrder | None:
    matching = tuple(order for order in orders if order.client_order_id == client_order_id)
    return max(matching, key=lambda order: order.observed_at) if matching else None


def _unknown_order(request: VenueOrderRequest, instrument: Instrument) -> PrivateOrder:
    return PrivateOrder(
        venue=request.venue,
        order_id=None,
        client_order_id=request.client_order_id,
        symbol=request.symbol,
        side=request.side,
        status=PrivateOrderStatus.UNKNOWN,
        requested_base_quantity=request.amount_contracts * instrument.contract_size_base,
        filled_base_quantity=Decimal(0),
        average_price=None,
        fee_usdt=None,
        observed_at=datetime.now(UTC),
        limit_price=request.price,
    )


def _journal_signed_positions(action: LiveJournalAction) -> dict[Venue, Decimal]:
    positions: dict[Venue, Decimal] = {}
    for leg in action.legs:
        signed = leg.filled_base_quantity if leg.side == Side.BUY else -leg.filled_base_quantity
        positions[leg.venue] = positions.get(leg.venue, Decimal(0)) + signed
    return positions


def _journal_delta(action: LiveJournalAction) -> Decimal:
    return sum(_journal_signed_positions(action).values(), Decimal(0))
