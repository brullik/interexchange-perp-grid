from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
    venue_client_order_id,
)
from interexchange_perp_grid.live_reconciliation import (
    PrivateStateAdapter,
    ReconciliationReport,
    collect_private_states,
    reconcile_private_states,
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


def first_close_reason(signals: CanaryCloseSignals) -> CloseReason | None:
    ordered = (
        (signals.emergency_active, CloseReason.EMERGENCY),
        (signals.operator_close_requested, CloseReason.OPERATOR_CLOSE),
        (signals.public_or_private_data_stale, CloseReason.STALE_DATA),
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


class LiveCanaryCoordinator:
    """Durable paired open/recovery/close lifecycle; success requires private-verified FLAT."""

    def __init__(
        self,
        journal: LiveOrderJournal,
        adapters: Mapping[Venue, CanaryVenueAdapter],
        instruments: Mapping[Venue, Instrument],
        protection: ProtectionProvider,
        monitor: CanaryMonitor,
        emergency_venue: Venue,
        *,
        terminal_timeout_seconds: Decimal = Decimal("2"),
    ) -> None:
        self._journal = journal
        self._adapters = dict(adapters)
        self._instruments = dict(instruments)
        self._protection = protection
        self._monitor = monitor
        self._emergency_venue = emergency_venue
        self._terminal_timeout_seconds = terminal_timeout_seconds
        self._orders_sent = 0
        self._sequence = 0

    async def run(self, plan: CanaryExecutionPlan) -> CanaryCycleResult:
        await self._journal.initialise()
        active = await self._journal.active()
        completed = await self._journal.load(plan.pair_action_id)
        if active is None and completed is not None and completed.state == LiveActionState.FLAT:
            report = await self._verify(completed)
            return CanaryCycleResult(
                report.flat_verified,
                None if report.flat_verified else ReasonCode.RECONCILIATION_FAILED,
                0,
                False,
                report.residual_delta,
                completed.recovery_action,
                None,
                completed.state,
                report,
                None if report.flat_verified else "Previously flat action no longer verifies flat.",
            )
        if active is not None and active.pair_action_id != plan.pair_action_id:
            return self._failed(
                active,
                ReasonCode.RECONCILIATION_INCOMPLETE,
                recovery_action="BLOCKED_BY_ACTIVE_ACTION",
            )
        if active is None:
            action = await self._journal.prepare(
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
            )
        else:
            action = active
        if action.state == LiveActionState.PREPARED:
            await self._journal.mark_submit_attempted(
                action.pair_action_id,
                (plan.long_request.client_order_id, plan.short_request.client_order_id),
            )
            self._orders_sent += 2
            long_order, short_order = await asyncio.gather(
                self._submit_and_resolve(
                    action.pair_action_id,
                    plan.long_request,
                    self._instruments[plan.route.long_venue],
                ),
                self._submit_and_resolve(
                    action.pair_action_id,
                    plan.short_request,
                    self._instruments[plan.route.short_venue],
                ),
            )
            action = await self._classify_open_pair(action.pair_action_id, long_order, short_order)
        else:
            action = await self._refresh_action(action)

        resuming_close = action.state == LiveActionState.CLOSING
        opening_recovery: str | None = action.recovery_action
        hedged = resuming_close or await self._is_hedged(action)
        if not hedged and not resuming_close:
            action, opening_recovery = await self._recover_opening(action)
            hedged = await self._is_hedged(action)
            if not hedged:
                report = await self._verify(action)
                if report.flat_verified:
                    action = await self._to_flat(action, "OPEN_RECOVERY_FLATTENED")
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
                    )
                action = await self._quarantine(action, report, opening_recovery)
                return self._failed(
                    action,
                    ReasonCode.RECONCILIATION_FAILED,
                    recovery_action=opening_recovery,
                    reconciliation=report,
                )

        if not resuming_close and action.state != LiveActionState.HEDGED:
            action = await self._advance_to_hedged(action)
        close_reason = (
            CloseReason.EMERGENCY
            if resuming_close
            else await self._wait_close(plan.timeout_seconds)
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
        report = await self._verify(action)
        recovery_action: str | None = opening_recovery
        if not report.flat_verified:
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
            report = await self._verify(action)
        if not report.flat_verified:
            action = await self._quarantine(action, report, recovery_action)
            return self._failed(
                action,
                ReasonCode.RECONCILIATION_FAILED,
                hedged=True,
                recovery_action=recovery_action,
                close_reason=close_reason,
                reconciliation=report,
            )
        action = await self._to_flat(action, "EXCHANGE_VERIFIED_FLAT")
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
            order = _unknown_order(request, instrument)
        await self._record(pair_action_id, order)
        if order.status in {
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
            return _unknown_order(request, instrument)
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
        states = await collect_private_states(self._adapters, self._instruments)
        known = {leg.client_order_id for leg in action.legs}
        for state in states.values():
            for order in (*state.open_orders, *state.recent_orders):
                if order.client_order_id in known:
                    await self._record(action.pair_action_id, order)
        return await self._require_action(action.pair_action_id)

    async def _is_hedged(self, action: LiveJournalAction) -> bool:
        refreshed = await self._refresh_action(action)
        signed = _journal_signed_positions(refreshed)
        gross = sum((abs(value) for value in signed.values()), Decimal(0))
        return gross > 0 and sum(signed.values(), Decimal(0)) == 0

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
    ) -> PrivateOrder:
        instrument = self._instruments[venue]
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
        states = await collect_private_states(self._adapters, self._instruments)
        positions = tuple(position for state in states.values() for position in state.positions)
        await asyncio.gather(
            *(
                self._submit_recovery_leg(
                    action,
                    position.venue,
                    Side.SELL if position.side == Side.BUY else Side.BUY,
                    position.base_quantity,
                    (OrderPurpose.EMERGENCY_CLOSE if emergency else OrderPurpose.NORMAL_CLOSE),
                    "EMERGENCY_FLATTEN" if emergency else "CLOSE",
                    emergency=emergency,
                )
                for position in positions
            ),
            return_exceptions=True,
        )

    async def _cancel_all_bot_orders(self) -> None:
        states = await collect_private_states(self._adapters, self._instruments)
        await asyncio.gather(
            *(
                self._cancel_one(order)
                for state in states.values()
                for order in state.open_orders
                if order.client_order_id.startswith("ipeg-") and order.order_id is not None
            ),
            return_exceptions=True,
        )

    async def _cancel_one(self, order: PrivateOrder) -> None:
        assert order.order_id is not None
        await self._adapters[order.venue].cancel_order(
            order.order_id,
            self._instruments[order.venue],
        )

    async def _verify(self, action: LiveJournalAction) -> ReconciliationReport:
        refreshed = await self._refresh_action(action)
        states = await collect_private_states(self._adapters, self._instruments)
        return reconcile_private_states(
            refreshed,
            states,
            await self._journal.known_client_order_ids(),
            set(self._adapters),
        )

    async def _advance_to_hedged(self, action: LiveJournalAction) -> LiveJournalAction:
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

    async def _to_flat(self, action: LiveJournalAction, reason: str) -> LiveJournalAction:
        current = await self._require_action(action.pair_action_id)
        if current.state == LiveActionState.QUARANTINED:
            return await self._journal.transition(
                current.pair_action_id,
                LiveActionState.FLAT,
                {"reason": reason},
                residual_delta=Decimal(0),
            )
        if current.state not in {
            LiveActionState.REJECTED,
            LiveActionState.RECOVERING,
            LiveActionState.CLOSING,
        }:
            current = await self._journal.transition(
                current.pair_action_id,
                LiveActionState.RECOVERING,
                {"reason": reason},
            )
        return await self._journal.transition(
            current.pair_action_id,
            LiveActionState.FLAT,
            {"reason": reason},
            residual_delta=Decimal(0),
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

    def _failed(
        self,
        action: LiveJournalAction,
        reason: ReasonCode,
        *,
        hedged: bool = False,
        recovery_action: str | None = None,
        close_reason: CloseReason | None = None,
        reconciliation: ReconciliationReport | None = None,
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
