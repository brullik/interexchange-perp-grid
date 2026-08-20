from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from decimal import Decimal, DecimalException

from interexchange_perp_grid.client_ids import (
    is_bot_client_order_id,
    venue_client_order_id,
)
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_coordinator import CanaryVenueAdapter
from interexchange_perp_grid.live_journal import (
    JournalLeg,
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
    request_payload_hash,
)
from interexchange_perp_grid.live_reconciliation import (
    FlatBarrierPolicy,
    FlatBarrierResult,
    ReconciliationReport,
    collect_private_states,
    combined_event_watermark,
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
_ACCOUNT_FLATTEN_LOCKS: dict[str, tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}


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
        active = await self._journal.active_actions()
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
        private_state_known = all(
            state.error is None
            and state.completeness == SnapshotCompleteness.COMPLETE
            and state.account_wide
            and not state.unknown_active_records
            and state.raw_open_order_count == len(state.open_orders)
            and state.raw_nonzero_position_count == len(state.positions)
            for state in states.values()
        )
        risk = _live_risk_snapshot(active, positions, private_state_known=private_state_known)
        return {
            "source": "PRIVATE_EXCHANGE",
            "status": {
                "journal_state": _journal_state(active),
                "pair_action_id": active[0].pair_action_id if len(active) == 1 else None,
                "pair_action_ids": tuple(action.pair_action_id for action in active),
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
            "risk": risk,
        }

    async def cancel_all_live(self) -> LiveControlResult:
        return await self._cancel_orders(bot_only=True)

    async def _cancel_orders(self, *, bot_only: bool) -> LiveControlResult:
        await self._journal.initialise()
        states = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="PRE_CANCEL",
        )
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
        refreshed = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="POST_CANCEL",
        )
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
        active = await self._journal.active_actions()
        return LiveControlResult(
            success,
            "CANCEL_ALL_LIVE" if bot_only else "CANCEL_ALL_ACCOUNT_ORDERS",
            0,
            cancelled,
            _terminal_state(active),
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
        async with _account_flatten_lock(self._journal):
            lease_token = await self._journal.acquire_account_flatten_lease()
            if lease_token is None:
                return LiveControlResult(
                    False,
                    action_name,
                    0,
                    0,
                    None,
                    None,
                    "Another account-wide flatten is in progress; keep live disabled.",
                    reason=ReasonCode.RECONCILIATION_INCOMPLETE,
                )
            try:
                resumed_submits = await self._resume_attempted_emergency_legs()
                result = await self._flatten_owned(action_name)
                if resumed_submits:
                    result = replace(
                        result,
                        orders_sent=result.orders_sent + resumed_submits,
                    )
            except BaseException:
                # Indeterminate network work keeps exclusive ownership. A restarted process
                # may adopt only after the old process incarnation is no longer live.
                raise
            await self._journal.release_account_flatten_lease(lease_token)
            return result

    async def _resume_attempted_emergency_legs(self) -> int:
        active = await self._journal.active_actions()
        submissions = 0
        for action in active:
            for leg in action.legs:
                if (
                    not leg.submit_attempted
                    or leg.protected_price is not None
                    or leg.status
                    in {
                        PrivateOrderStatus.FILLED,
                        PrivateOrderStatus.CANCELLED,
                        PrivateOrderStatus.REJECTED,
                    }
                ):
                    continue
                submissions += await self._resume_attempted_emergency_leg(action, leg)
        return submissions

    async def _resume_attempted_emergency_leg(
        self,
        action: LiveJournalAction,
        leg: JournalLeg,
    ) -> int:
        instrument = self._instrument(leg.venue, leg.symbol)
        request = translate_protected_order(
            ExecutionIntent(
                client_order_id=leg.client_order_id,
                venue=leg.venue,
                side=leg.side,
                purpose=OrderPurpose.EMERGENCY_CLOSE,
                quantity=leg.intended_base_quantity,
                worst_acceptable_price=None,
                unbounded_market=True,
            ),
            instrument,
        )
        if request_payload_hash(request) != leg.request_payload_hash:
            raise RuntimeError("journaled emergency request cannot be reconstructed exactly")
        adapter = self._adapters[leg.venue]
        order = await adapter.find_order_by_client_id(leg.client_order_id, instrument)
        if order is None:
            raise RuntimeError("journaled emergency order remains unknown")
        await self._journal.record_order_event(
            action.pair_action_id,
            order,
            (
                f"{order.order_id or 'none'}:{order.status.value}:"
                f"{order.filled_base_quantity}:{order.observed_at.isoformat()}"
            ),
        )
        if order.status == PrivateOrderStatus.UNKNOWN:
            raise RuntimeError("journaled emergency order remains unknown")
        return 0

    async def _flatten_owned(self, action_name: str) -> LiveControlResult:
        cancellation = await self._cancel_orders(bot_only=False)
        states = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="PRE_CLOSE",
        )
        positions = tuple(position for state in states.values() for position in state.positions)
        active_actions = await self._journal.active_actions()
        if len(active_actions) > 1:
            return await self._flatten_multiple(
                action_name,
                cancellation,
                positions,
                active_actions,
            )
        active = active_actions[0] if active_actions else None
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
        _raise_on_indeterminate_submit(submitted)
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

    async def _flatten_multiple(
        self,
        action_name: str,
        cancellation: LiveControlResult,
        positions: tuple[PositionSnapshot, ...],
        active: tuple[LiveJournalAction, ...],
    ) -> LiveControlResult:
        if not positions:
            barrier = await self._stable_report_many(active)
            if barrier.verified:
                active, barrier = await self._mark_flat_many(active, action_name, barrier)
                if barrier.verified:
                    return LiveControlResult(
                        True,
                        action_name,
                        0,
                        cancellation.cancelled_orders,
                        _terminal_state(active),
                        barrier.report,
                        None,
                        True,
                        barrier.timed_out,
                        barrier.consecutive_snapshots,
                        barrier.event_watermark,
                        None,
                    )
            active = await self._quarantine_many(active, barrier.report, action_name)
            return LiveControlResult(
                False,
                action_name,
                0,
                cancellation.cancelled_orders,
                _terminal_state(active),
                barrier.report,
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
        owners: list[LiveJournalAction] = []
        for position in positions:
            matched = tuple(
                action
                for action in active
                if any(
                    leg.venue == position.venue and leg.symbol == position.symbol
                    for leg in action.legs
                )
            )
            if len(matched) != 1:
                quarantined = await self._quarantine_many(
                    active,
                    await self._report_many(active),
                    action_name,
                )
                return LiveControlResult(
                    False,
                    action_name,
                    0,
                    cancellation.cancelled_orders,
                    _terminal_state(quarantined),
                    None,
                    "Private position ownership is ambiguous; keep live disabled.",
                    reason=ReasonCode.RECONCILIATION_INCOMPLETE,
                )
            owners.append(matched[0])

        recovering = tuple(
            await asyncio.gather(
                *(self._move_to_recovering(action, action_name) for action in active)
            )
        )
        recovering_by_id = {action.pair_action_id: action for action in recovering}
        for owner, request, position in zip(owners, requests, positions, strict=True):
            current_owner = recovering_by_id[owner.pair_action_id]
            await self._journal.append_order_leg(
                current_owner.pair_action_id,
                request,
                position.base_quantity,
                None,
            )
            await self._journal.mark_leg_submit_attempted(
                current_owner.pair_action_id,
                request.client_order_id,
            )
        submitted = await asyncio.gather(
            *(
                self._submit_emergency(recovering_by_id[owner.pair_action_id], request)
                for owner, request in zip(owners, requests, strict=True)
            ),
            return_exceptions=True,
        )
        _raise_on_indeterminate_submit(submitted)
        barrier = await self._stable_report_many(recovering)
        if barrier.verified:
            recovered, barrier = await self._mark_flat_many(recovering, action_name, barrier)
            if barrier.verified:
                return LiveControlResult(
                    True,
                    action_name,
                    len(requests),
                    cancellation.cancelled_orders,
                    _terminal_state(recovered),
                    barrier.report,
                    None,
                    True,
                    barrier.timed_out,
                    barrier.consecutive_snapshots,
                    barrier.event_watermark,
                    None,
                )
        quarantined = await self._quarantine_many(recovering, barrier.report, action_name)
        return LiveControlResult(
            False,
            action_name,
            len(requests),
            cancellation.cancelled_orders,
            _terminal_state(quarantined),
            barrier.report,
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
        states = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="TERMINAL_FLAT",
            recent_instruments=self._recent_instruments((active,) if active is not None else ()),
        )
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
            lambda: combined_event_watermark(
                self._adapters,
                self._journal.event_watermark,
            ),
            self._flat_barrier_policy,
        )

    async def _report_many(
        self,
        active: Sequence[LiveJournalAction],
    ) -> ReconciliationReport:
        loaded = await asyncio.gather(*(self._journal.load(item.pair_action_id) for item in active))
        current = tuple(action for action in loaded if action is not None)
        states = await collect_private_states(
            self._adapters,
            self._account_instruments,
            reconciliation_trigger="TERMINAL_FLAT",
            recent_instruments=self._recent_instruments(current),
        )
        return reconcile_private_states(
            current,
            states,
            await self._journal.known_client_order_ids(),
            set(self._adapters),
        )

    async def _stable_report_many(
        self,
        active: Sequence[LiveJournalAction],
    ) -> FlatBarrierResult:
        return await wait_for_stable_flat(
            lambda: self._report_many(active),
            lambda: combined_event_watermark(
                self._adapters,
                self._journal.event_watermark,
            ),
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

    def _recent_instruments(
        self,
        active: Sequence[LiveJournalAction],
    ) -> dict[Venue, tuple[Instrument, ...]]:
        symbols_by_venue: dict[Venue, set[str]] = {venue: set() for venue in self._adapters}
        for action in active:
            for leg in action.legs:
                if leg.submit_attempted:
                    symbols_by_venue.setdefault(leg.venue, set()).add(leg.symbol)
        return {
            venue: tuple(self._instrument(venue, symbol) for symbol in sorted(symbols))
            or (self._account_instruments[venue],)
            for venue, symbols in symbols_by_venue.items()
        }

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
        observed_before = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if observed_before != barrier.event_watermark:
            if active is not None:
                active = await self._quarantine(active, barrier.report, action_name)
            return (
                active,
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
            active.pair_action_id if active is not None else None,
            journal_watermark,
            {"action": action_name, "verified": True},
        )
        observed_after = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if commit.committed and observed_after == barrier.event_watermark:
            return commit.action, barrier
        failed_action = commit.action
        if failed_action is not None and failed_action.state != LiveActionState.QUARANTINED:
            failed_action = await self._quarantine(failed_action, barrier.report, action_name)
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

    async def _mark_flat_many(
        self,
        active: Sequence[LiveJournalAction],
        action_name: str,
        barrier: FlatBarrierResult,
    ) -> tuple[tuple[LiveJournalAction, ...], FlatBarrierResult]:
        if not barrier.verified:
            raise RuntimeError(flat_barrier_failure_reason(barrier).value)
        loaded = await asyncio.gather(*(self._journal.load(item.pair_action_id) for item in active))
        current = tuple(action for action in loaded if action is not None)
        if len(current) != len(active):
            raise RuntimeError("flat barrier action disappeared")
        recovering = tuple(
            await asyncio.gather(
                *(self._move_to_recovering(action, action_name) for action in current)
            )
        )
        observed_before = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if observed_before != barrier.event_watermark:
            failed = await self._quarantine_many(recovering, barrier.report, action_name)
            return (
                failed,
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
        commit = await self._journal.commit_flat_barrier_many(
            tuple(action.pair_action_id for action in recovering),
            journal_watermark,
            {"action": action_name, "verified": True},
        )
        observed_after = await combined_event_watermark(
            self._adapters,
            self._journal.event_watermark,
        )
        if commit.committed and observed_after == barrier.event_watermark:
            return commit.actions, barrier
        failed = await self._quarantine_many(commit.actions, barrier.report, action_name)
        return (
            failed,
            FlatBarrierResult(
                False,
                barrier.report,
                0,
                observed_after,
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

    async def _quarantine_many(
        self,
        active: Sequence[LiveJournalAction],
        report: ReconciliationReport,
        action_name: str,
    ) -> tuple[LiveJournalAction, ...]:
        return tuple(
            await asyncio.gather(
                *(self._quarantine(action, report, action_name) for action in active)
            )
        )


def _raise_on_indeterminate_submit(results: Sequence[object]) -> None:
    failure = next((result for result in results if isinstance(result, BaseException)), None)
    if failure is not None:
        raise RuntimeError(
            "emergency submit is indeterminate; exclusive flatten ownership retained"
        ) from failure


def _account_flatten_lock(journal: LiveOrderJournal) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    key = str(journal.path.resolve())
    existing = _ACCOUNT_FLATTEN_LOCKS.get(key)
    if existing is not None and existing[0] is loop:
        return existing[1]
    if existing is not None and existing[1].locked():
        raise RuntimeError("account-wide flatten remains active on another event loop")
    lock = asyncio.Lock()
    _ACCOUNT_FLATTEN_LOCKS[key] = (loop, lock)
    return lock


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


def _live_risk_snapshot(
    active: LiveJournalAction | Sequence[LiveJournalAction] | None,
    positions: Sequence[Mapping[str, object]],
    *,
    private_state_known: bool,
) -> dict[str, object]:
    actions = _actions(active)
    if not private_state_known:
        return {"status": "INVALID_RISK_DATA", "reason": "PRIVATE_STATE_UNAVAILABLE"}
    if not actions:
        if positions:
            return {
                "status": "INVALID_RISK_DATA",
                "reason": "UNJOURNALED_PRIVATE_EXPOSURE",
            }
        return {
            "status": "OK",
            "scope": "JOURNAL_RESERVATION",
            "reservation_count": 0,
            "per_route_stress_usdt": {},
            "portfolio_stress_usdt": "0",
        }
    if not _private_positions_match_journal(actions, positions):
        return {
            "status": "INVALID_RISK_DATA",
            "reason": "PRIVATE_POSITION_JOURNAL_MISMATCH",
        }
    per_route: dict[str, str] = {}
    portfolio_stress = Decimal(0)
    for action in actions:
        try:
            projected_stress = Decimal(str(action.risk_reservation["projected_stress_usdt"]))
            if (
                not projected_stress.is_finite()
                or projected_stress < 0
                or projected_stress > Decimal("5")
            ):
                raise ValueError("projected route stress is outside the locked limit")
            portfolio_stress += projected_stress
            if not portfolio_stress.is_finite() or portfolio_stress > Decimal("50"):
                raise ValueError("projected portfolio stress is outside the locked limit")
        except (DecimalException, KeyError, ValueError):
            return {"status": "INVALID_RISK_DATA", "reason": "RISK_RESERVATION_UNKNOWN"}
        per_route[action.route.value] = str(projected_stress)
    return {
        "status": "OK",
        "scope": "JOURNAL_RESERVATION",
        "reservation_count": len(actions),
        "per_route_stress_usdt": per_route,
        "portfolio_stress_usdt": str(portfolio_stress),
    }


def _private_positions_match_journal(
    active: Sequence[LiveJournalAction],
    positions: Sequence[Mapping[str, object]],
) -> bool:
    expected: dict[tuple[str, str], Decimal] = {}
    actual: dict[tuple[str, str], Decimal] = {}
    try:
        for leg in (leg for action in active for leg in action.legs):
            signed = leg.filled_base_quantity if leg.side == Side.BUY else -leg.filled_base_quantity
            key = (leg.venue.value, leg.symbol)
            expected[key] = expected.get(key, Decimal(0)) + signed
        for position in positions:
            quantity = Decimal(str(position["base_quantity"]))
            side = Side(str(position["side"]))
            venue = Venue(str(position["venue"])).value
            symbol = str(position["symbol"])
            if not quantity.is_finite() or quantity <= 0 or not venue or not symbol:
                return False
            key = (venue, symbol)
            signed = quantity if side == Side.BUY else -quantity
            actual[key] = actual.get(key, Decimal(0)) + signed
    except (DecimalException, KeyError, ValueError):
        return False
    return {key: value for key, value in expected.items() if value != 0} == {
        key: value for key, value in actual.items() if value != 0
    }


def _actions(
    active: LiveJournalAction | Sequence[LiveJournalAction] | None,
) -> tuple[LiveJournalAction, ...]:
    if active is None:
        return ()
    if isinstance(active, LiveJournalAction):
        return (active,)
    return tuple(active)


def _terminal_state(active: Sequence[LiveJournalAction]) -> LiveActionState | None:
    states = {action.state for action in active}
    return next(iter(states)) if len(states) == 1 else None


def _journal_state(active: Sequence[LiveJournalAction]) -> str:
    if not active:
        return LiveActionState.FLAT.value
    state = _terminal_state(active)
    return state.value if state is not None else "MULTIPLE"


def _position_pnl(position: PositionSnapshot) -> Decimal:
    if position.entry_price is None or position.mark_price is None:
        return Decimal(0)
    price_move = position.mark_price - position.entry_price
    return (
        price_move * position.base_quantity
        if position.side == Side.BUY
        else -price_move * position.base_quantity
    )
