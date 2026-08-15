from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from decimal import Decimal

from interexchange_perp_grid.client_ids import (
    is_bot_client_order_id,
    venue_client_order_id,
)
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_coordinator import CanaryVenueAdapter
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.live_reconciliation import (
    FlatBarrierPolicy,
    FlatBarrierResult,
    ReconciliationReport,
    collect_private_states,
    flat_barrier_failure_reason,
    reconcile_private_states,
    wait_for_stable_flat,
)
from interexchange_perp_grid.private_domain import (
    PositionSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    SnapshotCompleteness,
    VenueOrderRequest,
)
from interexchange_perp_grid.private_execution import translate_protected_order
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey

EMERGENCY_CONFIRMATION = "I_CONFIRM_EMERGENCY_FLATTEN_ALL_LIVE_EXPOSURE"


@dataclass(frozen=True, slots=True)
class LiveControlResult:
    success: bool
    action: str
    orders_sent: int
    cancelled_orders: int
    terminal_state: LiveActionState | None
    reconciliation: ReconciliationReport | None
    instruction: str | None
    flat_barrier_verified: bool = False
    flat_barrier_timed_out: bool = False
    flat_barrier_snapshots: int = 0
    flat_barrier_watermark: int = -1
    reason: ReasonCode | None = None


class LiveControlService:
    def __init__(
        self,
        journal: LiveOrderJournal,
        adapters: Mapping[Venue, CanaryVenueAdapter],
        instruments: Mapping[Venue, Instrument] | Mapping[tuple[Venue, str], Instrument],
        qualified_route: DirectedRouteKey | None = None,
        qualification_hash: str | None = None,
        flat_barrier_policy: FlatBarrierPolicy | None = None,
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
        self._route = qualified_route
        self._qualification_hash = (
            qualification_hash
            or hashlib.sha256(b"EMERGENCY_RISK_REDUCTION_WITHOUT_QUALIFICATION").hexdigest()
        )
        self._flat_barrier_policy = flat_barrier_policy or FlatBarrierPolicy()

    async def snapshot(self) -> dict[str, object]:
        await self._journal.initialise()
        states = await collect_private_states(self._adapters, self._account_instruments)
        active = await self._journal.active()
        positions = [
            {
                "venue": position.venue.value,
                "symbol": position.symbol,
                "side": position.side.value,
                "base_quantity": str(position.base_quantity),
                "entry_price": str(position.entry_price),
                "mark_price": str(position.mark_price),
                "unrealized_pnl_usdt": str(_position_pnl(position)),
            }
            for state in states.values()
            for position in state.positions
        ]
        balances = [
            {
                "venue": venue.value,
                "equity_usdt": str(state.account.equity_usdt) if state.account else None,
                "free_margin_usdt": (
                    str(state.account.free_margin_usdt) if state.account else None
                ),
                "margin_mode": state.account.margin_mode if state.account else None,
                "position_mode": state.account.position_mode if state.account else None,
                "error": state.error,
            }
            for venue, state in sorted(states.items(), key=lambda item: item[0].value)
        ]
        return {
            "source": "PRIVATE_EXCHANGE",
            "status": {
                "journal_state": active.state.value if active else LiveActionState.FLAT.value,
                "pair_action_id": active.pair_action_id if active else None,
                "private_state_errors": {
                    venue.value: state.error
                    for venue, state in states.items()
                    if state.error is not None
                },
                "open_bot_orders": sum(
                    is_bot_client_order_id(order.client_order_id)
                    for state in states.values()
                    for order in state.open_orders
                ),
                "raw_open_orders": sum(state.raw_open_order_count for state in states.values()),
                "raw_nonzero_positions": sum(
                    state.raw_nonzero_position_count for state in states.values()
                ),
                "unknown_active_records": sum(
                    len(state.unknown_active_records) for state in states.values()
                ),
            },
            "positions": positions,
            "balances": balances,
            "pnl": {
                "unrealized_pnl_usdt": str(
                    sum(
                        (
                            _position_pnl(position)
                            for state in states.values()
                            for position in state.positions
                        ),
                        Decimal(0),
                    )
                )
            },
        }

    async def cancel_all_live(self) -> LiveControlResult:
        return await self._cancel_orders(bot_only=True)

    async def _cancel_orders(self, *, bot_only: bool) -> LiveControlResult:
        await self._journal.initialise()
        states = await collect_private_states(self._adapters, self._account_instruments)
        cancellable = tuple(
            order
            for state in states.values()
            for order in state.open_orders
            if (not bot_only or is_bot_client_order_id(order.client_order_id))
            and order.order_id is not None
        )
        results = await asyncio.gather(
            *(self._cancel(order) for order in cancellable),
            return_exceptions=True,
        )
        cancelled = sum(not isinstance(result, BaseException) for result in results)
        refreshed = await collect_private_states(self._adapters, self._account_instruments)
        remaining = sum(
            (not bot_only or is_bot_client_order_id(order.client_order_id))
            for state in refreshed.values()
            for order in state.open_orders
        )
        success = (
            remaining == 0
            and all(state.error is None for state in refreshed.values())
            and all(not state.unknown_active_records for state in refreshed.values())
            and all(
                state.completeness == SnapshotCompleteness.COMPLETE
                and state.raw_open_order_count == len(state.open_orders)
                for state in refreshed.values()
            )
        )
        active = await self._journal.active()
        return LiveControlResult(
            success,
            "CANCEL_ALL_LIVE" if bot_only else "CANCEL_ALL_ACCOUNT_ORDERS",
            0,
            cancelled,
            active.state if active else None,
            None,
            None
            if success
            else "Bot orders remain or private state is unknown; keep live disabled.",
        )

    async def close_all_live(self) -> LiveControlResult:
        return await self._flatten("CLOSE_ALL_LIVE")

    async def emergency_flatten(self) -> LiveControlResult:
        return await self._flatten("EMERGENCY_FLATTEN")

    async def kill(self) -> LiveControlResult:
        return await self._flatten("KILL_CANCEL_FLATTEN")

    async def _flatten(self, action_name: str) -> LiveControlResult:
        cancellation = await self._cancel_orders(bot_only=False)
        states = await collect_private_states(self._adapters, self._account_instruments)
        positions = tuple(position for state in states.values() for position in state.positions)
        active = await self._journal.active()
        if not positions:
            barrier = await self._stable_report(active)
            report = barrier.report
            if barrier.verified:
                active, barrier = await self._mark_flat_if_needed(active, action_name, barrier)
                if barrier.verified:
                    return LiveControlResult(
                        True,
                        action_name,
                        0,
                        cancellation.cancelled_orders,
                        active.state if active else LiveActionState.FLAT,
                        report,
                        None,
                        barrier.verified,
                        barrier.timed_out,
                        barrier.consecutive_snapshots,
                        barrier.event_watermark,
                        None,
                    )
            if active is not None:
                active = await self._quarantine(active, report, action_name)
            return LiveControlResult(
                False,
                action_name,
                0,
                cancellation.cancelled_orders,
                active.state if active is not None else None,
                report,
                "Stable FLAT barrier was not verified; keep live disabled.",
                barrier.verified,
                barrier.timed_out,
                barrier.consecutive_snapshots,
                barrier.event_watermark,
                flat_barrier_failure_reason(barrier),
            )
        requests = tuple(
            translate_protected_order(
                ExecutionIntent(
                    client_order_id=venue_client_order_id(
                        f"emergency-{time.time_ns()}-{secrets.token_hex(2)}",
                        "close",
                        index,
                    ),
                    venue=position.venue,
                    side=Side.SELL if position.side == Side.BUY else Side.BUY,
                    purpose=OrderPurpose.EMERGENCY_CLOSE,
                    quantity=position.base_quantity,
                    worst_acceptable_price=None,
                    unbounded_market=True,
                ),
                self._instrument(position.venue, position.symbol),
            )
            for index, position in enumerate(positions)
        )
        if active is None and requests:
            active = await self._journal.prepare_emergency(
                pair_action_id=f"emergency-{time.time_ns()}",
                route=self._emergency_route(),
                tranche_id="emergency-flatten",
                requests=requests,
                intended_base_quantities={
                    request.client_order_id: position.base_quantity
                    for request, position in zip(requests, positions, strict=True)
                },
                risk_reservation={
                    "action": action_name,
                    "qualification_bypassed_for_risk_reduction": True,
                    "initial_signed_positions": {
                        venue.value: str(
                            sum(
                                (
                                    position.base_quantity
                                    if position.side == Side.BUY
                                    else -position.base_quantity
                                    for position in positions
                                    if position.venue == venue
                                ),
                                Decimal(0),
                            )
                        )
                        for venue in self._adapters
                    },
                },
                qualification_hash=self._qualification_hash,
            )
            await self._journal.mark_submit_attempted(
                active.pair_action_id,
                tuple(request.client_order_id for request in requests),
            )
        elif active is not None:
            active = await self._move_to_recovering(active, action_name)
            for request, position in zip(requests, positions, strict=True):
                await self._journal.append_order_leg(
                    active.pair_action_id,
                    request,
                    position.base_quantity,
                    None,
                )
                await self._journal.mark_leg_submit_attempted(
                    active.pair_action_id,
                    request.client_order_id,
                )
        submitted = await asyncio.gather(
            *(
                self._submit_emergency(active, request)
                for request in requests
                if active is not None
            ),
            return_exceptions=True,
        )
        orders_sent = len(requests)
        if active is not None and active.state == LiveActionState.SUBMITTING:
            active = await self._journal.transition(
                active.pair_action_id,
                LiveActionState.RECOVERING,
                {"action": action_name, "submit_results": len(submitted)},
                recovery_action=action_name,
            )
        if active is None:
            return LiveControlResult(
                False,
                action_name,
                orders_sent,
                cancellation.cancelled_orders,
                None,
                None,
                "Private state did not verify flat and no durable emergency action exists.",
            )
        barrier = await self._stable_report(active)
        report = barrier.report
        if barrier.verified:
            active, barrier = await self._mark_flat_if_needed(active, action_name, barrier)
            if barrier.verified:
                assert active is not None
                return LiveControlResult(
                    True,
                    action_name,
                    orders_sent,
                    cancellation.cancelled_orders,
                    active.state,
                    report,
                    None,
                    barrier.verified,
                    barrier.timed_out,
                    barrier.consecutive_snapshots,
                    barrier.event_watermark,
                    None,
                )
        assert active is not None
        active = await self._quarantine(active, report, action_name)
        return LiveControlResult(
            False,
            action_name,
            orders_sent,
            cancellation.cancelled_orders,
            active.state,
            report,
            (
                "FAILED_QUARANTINED: keep live disabled; inspect every involved exchange, "
                "cancel remaining bot orders, and manually flatten residual positions."
            ),
            barrier.verified,
            barrier.timed_out,
            barrier.consecutive_snapshots,
            barrier.event_watermark,
            flat_barrier_failure_reason(barrier),
        )

    async def _submit_emergency(
        self,
        active: LiveJournalAction,
        request: VenueOrderRequest,
    ) -> PrivateOrder:
        venue = request.venue
        try:
            order = await self._adapters[venue].submit_order(
                request,
                self._instrument(venue, request.symbol),
            )
        except (TimeoutError, ConnectionError):
            raise
        await self._journal.record_order_event(
            active.pair_action_id,
            order,
            (
                f"{order.order_id or 'none'}:{order.status.value}:"
                f"{order.filled_base_quantity}:{order.observed_at.isoformat()}"
            ),
        )
        if order.status not in {
            PrivateOrderStatus.FILLED,
            PrivateOrderStatus.PARTIAL,
            PrivateOrderStatus.REJECTED,
            PrivateOrderStatus.CANCELLED,
        }:
            raise RuntimeError("emergency close result is non-terminal")
        return order

    async def _report(self, active: LiveJournalAction | None) -> ReconciliationReport:
        if active is not None:
            active = await self._journal.load(active.pair_action_id)
        states = await collect_private_states(self._adapters, self._account_instruments)
        return reconcile_private_states(
            active,
            states,
            await self._journal.known_client_order_ids(),
            set(self._adapters),
        )

    async def _stable_report(self, active: LiveJournalAction | None) -> FlatBarrierResult:
        async def report_factory() -> ReconciliationReport:
            current = active
            if current is not None:
                current = await self._journal.load(current.pair_action_id)
            return await self._report(current)

        return await wait_for_stable_flat(
            report_factory,
            self._journal.event_watermark,
            self._flat_barrier_policy,
        )

    async def _cancel(self, order: PrivateOrder) -> PrivateOrder:
        assert order.order_id is not None
        return await self._adapters[order.venue].cancel_order(
            order.order_id,
            self._instrument(order.venue, order.symbol),
        )

    def _instrument(self, venue: Venue, symbol: str) -> Instrument:
        instrument = self._instruments.get((venue, symbol))
        if instrument is None:
            raise RuntimeError(f"instrument registry has no {venue.value}:{symbol}")
        return instrument

    def _emergency_route(self) -> DirectedRouteKey:
        if self._route is not None:
            return self._route
        venues = tuple(sorted(self._adapters, key=lambda venue: venue.value))
        if len(venues) < 2:
            raise RuntimeError("emergency journal requires at least two dedicated venues")
        return DirectedRouteKey("EMERGENCY", venues[0], venues[1])

    async def _move_to_recovering(
        self,
        active: LiveJournalAction,
        action_name: str,
    ) -> LiveJournalAction:
        if active.state == LiveActionState.RECOVERING:
            return active
        if active.state == LiveActionState.HEDGED:
            active = await self._journal.transition(
                active.pair_action_id,
                LiveActionState.CLOSING,
                {"action": action_name},
            )
        if active.state == LiveActionState.PREPARED:
            active = await self._journal.transition(
                active.pair_action_id,
                LiveActionState.QUARANTINED,
                {"action": action_name, "reason": "PREPARED_ABORTED"},
            )
        return await self._journal.transition(
            active.pair_action_id,
            LiveActionState.RECOVERING,
            {"action": action_name},
            recovery_action=action_name,
        )

    async def _mark_flat_if_needed(
        self,
        active: LiveJournalAction | None,
        action_name: str,
        barrier: FlatBarrierResult,
    ) -> tuple[LiveJournalAction | None, FlatBarrierResult]:
        if not barrier.verified:
            raise RuntimeError(flat_barrier_failure_reason(barrier).value)
        if active is not None:
            current = await self._journal.load(active.pair_action_id)
            if current is None:
                raise RuntimeError("flat barrier action disappeared")
            active = current
            if active.state != LiveActionState.FLAT:
                active = await self._move_to_recovering(active, action_name)
        commit = await self._journal.commit_flat_barrier(
            active.pair_action_id if active is not None else None,
            barrier.event_watermark,
            {"action": action_name, "verified": True},
        )
        if commit.committed:
            return commit.action, barrier
        return (
            commit.action,
            FlatBarrierResult(
                False,
                barrier.report,
                0,
                commit.event_watermark,
                False,
                ReasonCode.FLAT_BARRIER_EVENT_RACE,
            ),
        )

    async def _quarantine(
        self,
        active: LiveJournalAction,
        report: ReconciliationReport,
        action_name: str,
    ) -> LiveJournalAction:
        current = await self._journal.load(active.pair_action_id)
        if current is None:
            raise RuntimeError("live control journal action disappeared")
        if current.state == LiveActionState.QUARANTINED:
            return current
        return await self._journal.transition(
            current.pair_action_id,
            LiveActionState.QUARANTINED,
            {
                "action": action_name,
                "unknown": report.unknown_client_order_ids,
                "discrepancies": report.discrepancies,
            },
            residual_delta=report.residual_delta,
            recovery_action=action_name,
        )


def emergency_unlock_valid(
    confirmation: str,
    environ: dict[str, str] | None = None,
) -> bool:
    source = os.environ if environ is None else environ
    expected_secret = source.get("IPEG_EMERGENCY_UNLOCK_SECRET", "")
    supplied_secret = source.get("IPEG_EMERGENCY_UNLOCK", "")
    return (
        bool(expected_secret)
        and bool(supplied_secret)
        and secrets.compare_digest(expected_secret, supplied_secret)
        and secrets.compare_digest(confirmation, EMERGENCY_CONFIRMATION)
    )


def render_control_result(result: LiveControlResult) -> str:
    return json.dumps(asdict(result), default=str, sort_keys=True)


def _position_pnl(position: PositionSnapshot) -> Decimal:
    if position.entry_price is None or position.mark_price is None:
        return Decimal(0)
    price_move = position.mark_price - position.entry_price
    return (
        price_move * position.base_quantity
        if position.side == Side.BUY
        else -price_move * position.base_quantity
    )
