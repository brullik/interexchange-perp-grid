from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.aggressive_evaluator import load_aggressive_decision_policy
from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_coordinator import (
    CanaryCloseSignals,
    CanaryCycleResult,
    CanaryExecutionPlan,
    CloseReason,
    LiveCanaryCoordinator,
    first_close_reason,
)
from interexchange_perp_grid.live_journal import (
    FlatBarrierCommitResult,
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.live_reconciliation import FlatBarrierPolicy
from interexchange_perp_grid.live_simulator import (
    DeterministicCanaryMonitor,
    DeterministicPrivateExchange,
    ScriptedOrderOutcome,
    StaticProtectionProvider,
)
from interexchange_perp_grid.private_domain import (
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)


class InjectedRestart(RuntimeError):
    pass


class FailingProtectionProvider:
    async def price(
        self,
        venue: Venue,
        side: Side,
        quantity: Decimal,
        purpose: object,
    ) -> Decimal:
        del venue, side, quantity, purpose
        raise RuntimeError("public protection unavailable")


class CrashAfterTransitionJournal(LiveOrderJournal):
    def __init__(self, path: Path, target: LiveActionState) -> None:
        super().__init__(path)
        self.target = target
        self.fired = False

    async def transition(
        self,
        pair_action_id: str,
        state: LiveActionState,
        details: dict[str, object] | None = None,
        *,
        residual_delta: Decimal | None = None,
        recovery_action: str | None = None,
        now: datetime | None = None,
    ) -> LiveJournalAction:
        action = await super().transition(
            pair_action_id,
            state,
            details,
            residual_delta=residual_delta,
            recovery_action=recovery_action,
            now=now,
        )
        if state == self.target and not self.fired:
            self.fired = True
            raise InjectedRestart(state.value)
        return action

    async def commit_flat_barrier(
        self,
        pair_action_id: str | None,
        expected_event_watermark: int,
        details: dict[str, object] | None = None,
        *,
        now: datetime | None = None,
    ) -> FlatBarrierCommitResult:
        result = await super().commit_flat_barrier(
            pair_action_id,
            expected_event_watermark,
            details,
            now=now,
        )
        if result.committed and self.target == LiveActionState.FLAT and not self.fired:
            self.fired = True
            raise InjectedRestart(LiveActionState.FLAT.value)
        return result


@pytest.mark.parametrize(
    ("signals", "expected"),
    [
        (CanaryCloseSignals(target_converged=True), CloseReason.TARGET_CONVERGENCE),
        (CanaryCloseSignals(risk_deteriorated=True), CloseReason.RISK_DETERIORATION),
        (CanaryCloseSignals(funding_deteriorated=True), CloseReason.FUNDING_DETERIORATION),
        (CanaryCloseSignals(public_or_private_data_stale=True), CloseReason.STALE_DATA),
        (CanaryCloseSignals(hard_stop_or_loss=True), CloseReason.HARD_STOP_OR_LOSS),
        (CanaryCloseSignals(hard_holding_time=True), CloseReason.HARD_HOLDING_TIME),
        (CanaryCloseSignals(operator_close_requested=True), CloseReason.OPERATOR_CLOSE),
        (CanaryCloseSignals(emergency_active=True), CloseReason.EMERGENCY),
    ],
)
def test_every_automatic_canary_close_condition_is_explicit(
    signals: CanaryCloseSignals,
    expected: CloseReason,
) -> None:
    assert first_close_reason(signals) == expected


def _instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.00025"),
        Decimal("1"),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.0005"),
        "private",
    )


def _outcome(
    status: PrivateOrderStatus,
    fill: str = "0",
    *,
    submit_fault: str | None = None,
    persist_before_fault: bool = False,
    cancel_failure: bool = False,
    late_fill_ratio_on_cancel: Decimal = Decimal(0),
    duplicate_private_events: bool = False,
) -> ScriptedOrderOutcome:
    return ScriptedOrderOutcome(
        status,
        Decimal(fill),
        submit_fault,
        persist_before_fault,
        cancel_failure,
        late_fill_ratio_on_cancel,
        duplicate_private_events,
    )


def _request(venue: Venue, client_id: str, side: Side) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue,
        client_id,
        "BTC/USDT:USDT",
        side,
        "limit",
        Decimal("4"),
        Decimal("100"),
        "IOC",
        {"timeInForce": "IOC"},
    )


def _plan() -> CanaryExecutionPlan:
    return CanaryExecutionPlan(
        pair_action_id="cycle-1",
        route=_ROUTE,
        tranche_id="tranche-1",
        quantity=Decimal("0.001"),
        long_request=_request(
            Venue.BINANCE_USDM,
            venue_client_order_id("cycle-1", "long"),
            Side.BUY,
        ),
        short_request=_request(
            Venue.OKX,
            venue_client_order_id("cycle-1", "short"),
            Side.SELL,
        ),
        risk_reservation={"projected_stress_usdt": "0.8"},
        qualification_hash="a" * 64,
        timeout_seconds=30,
    )


def _aggressive_hard_breach_plan() -> CanaryExecutionPlan:
    plan = _plan()
    reserves = {
        "entry_impact_usdt": "0.1",
        "exit_impact_usdt": "0.1",
        "entry_slippage_usdt": "0.1",
        "exit_slippage_usdt": "0.1",
        "latency_usdt": "0.1",
        "partial_fill_unmatched_usdt": "0.1",
        "emergency_hedge_usdt": "0.1",
        "reconciliation_forced_exit_usdt": "0.1",
        "liquidation_distance_usdt": "0.1",
    }
    return replace(
        plan,
        risk_reservation={
            "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
            "direction": "POSITIVE",
            "effective_stop_bps": "0",
            "projected_stress_usdt": "0.8",
            "route_hard_loss_usdt": "1",
            "portfolio_hard_loss_usdt": "1",
            "aggressive_intent": {
                "executable_entry_spread_bps": "10",
                "reserves": reserves,
                "adverse_funding_reserve_usdt": "0.1",
                "remaining_close_fees_usdt": "0.2",
            },
        },
    )


async def _run(
    tmp_path: Path,
    long_outcomes: tuple[ScriptedOrderOutcome, ...],
    short_outcomes: tuple[ScriptedOrderOutcome, ...],
    emergency_outcomes: tuple[ScriptedOrderOutcome, ...] = (),
) -> tuple[CanaryCycleResult, dict[Venue, DeterministicPrivateExchange]]:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM, instruments[Venue.BINANCE_USDM], long_outcomes
        ),
        Venue.OKX: DeterministicPrivateExchange(Venue.OKX, instruments[Venue.OKX], short_outcomes),
        Venue.BYBIT: DeterministicPrivateExchange(
            Venue.BYBIT, instruments[Venue.BYBIT], emergency_outcomes
        ),
    }
    protection = StaticProtectionProvider(
        {
            (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
            for venue in Venue
            for side in Side
        }
    )
    coordinator = LiveCanaryCoordinator(
        LiveOrderJournal(tmp_path / "state.sqlite3"),
        adapters,
        instruments,
        protection,
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
        terminal_timeout_seconds=Decimal("0.1"),
    )
    return await coordinator.run(_plan()), adapters


@pytest.mark.asyncio
async def test_prepared_action_rechecks_opening_gate_before_any_submit(tmp_path: Path) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM,
            instruments[Venue.BINANCE_USDM],
            (_outcome(PrivateOrderStatus.FILLED, "1"),),
        ),
        Venue.OKX: DeterministicPrivateExchange(
            Venue.OKX,
            instruments[Venue.OKX],
            (_outcome(PrivateOrderStatus.FILLED, "1"),),
        ),
        Venue.BYBIT: DeterministicPrivateExchange(
            Venue.BYBIT,
            instruments[Venue.BYBIT],
            (),
        ),
    }
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")

    async def deny_opening(_: CanaryExecutionPlan) -> bool:
        return False

    coordinator = LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        StaticProtectionProvider({}),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
        opening_gate=deny_opening,
    )
    prepared = await coordinator.prepare(_plan())
    assert prepared.state == LiveActionState.PREPARED

    result = await coordinator.run(_plan())

    assert result.success is False
    assert result.reason == ReasonCode.VENUE_QUARANTINED
    assert result.orders_sent == 0
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.recovery_action == "OPENING_GATE_DENIED"
    assert sum(adapter.submit_calls for adapter in adapters.values()) == 0
    action = await journal.load(_plan().pair_action_id)
    assert action is not None
    assert action.state == LiveActionState.QUARANTINED


@pytest.mark.asyncio
async def test_full_fill_canary_auto_closes_and_succeeds_only_flat(tmp_path: Path) -> None:
    result, adapters = await _run(
        tmp_path,
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True
    assert result.orders_sent == 4
    assert result.hedged is True
    assert result.residual_delta == 0
    assert result.close_reason == CloseReason.TARGET_CONVERGENCE
    assert result.terminal_state == LiveActionState.FLAT
    assert result.reconciliation is not None
    assert result.reconciliation.flat_verified is True
    positions = [await adapter.fetch_all_positions() for adapter in adapters.values()]
    assert all(not venue_positions for venue_positions in positions)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("long_open", "short_open"),
    [
        (_outcome(PrivateOrderStatus.FILLED, "1"), _outcome(PrivateOrderStatus.PARTIAL, "0.5")),
        (_outcome(PrivateOrderStatus.PARTIAL, "0.5"), _outcome(PrivateOrderStatus.FILLED, "1")),
        (_outcome(PrivateOrderStatus.PARTIAL, "0.75"), _outcome(PrivateOrderStatus.PARTIAL, "0.5")),
        (_outcome(PrivateOrderStatus.FILLED, "1"), _outcome(PrivateOrderStatus.REJECTED)),
        (_outcome(PrivateOrderStatus.REJECTED), _outcome(PrivateOrderStatus.FILLED, "1")),
    ],
)
async def test_one_leg_and_unequal_partial_fills_are_topped_up_then_closed(
    tmp_path: Path,
    long_open: ScriptedOrderOutcome,
    short_open: ScriptedOrderOutcome,
) -> None:
    long_fill = long_open.fill_ratio
    short_fill = short_open.fill_ratio
    long_recovery = (_outcome(PrivateOrderStatus.FILLED, "1"),) if long_fill < short_fill else ()
    short_recovery = (_outcome(PrivateOrderStatus.FILLED, "1"),) if short_fill < long_fill else ()
    result, _ = await _run(
        tmp_path,
        (
            long_open,
            *long_recovery,
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            short_open,
            *short_recovery,
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True
    assert result.recovery_action == "TOP_UP_SMALLER_LEG"
    assert result.terminal_state == LiveActionState.FLAT


@pytest.mark.asyncio
async def test_third_venue_hedge_is_used_after_topup_and_reduce_fail(tmp_path: Path) -> None:
    result, _ = await _run(
        tmp_path,
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.REJECTED),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            _outcome(PrivateOrderStatus.REJECTED),
            _outcome(PrivateOrderStatus.REJECTED),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True
    assert result.recovery_action == "THIRD_VENUE_HEDGE"
    assert result.terminal_state == LiveActionState.FLAT


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["TIMEOUT", "DISCONNECT"])
async def test_ack_fault_after_exchange_acceptance_reconciles_without_resubmit(
    tmp_path: Path,
    fault: str,
) -> None:
    result, adapters = await _run(
        tmp_path,
        (
            _outcome(
                PrivateOrderStatus.FILLED,
                "1",
                submit_fault=fault,
                persist_before_fault=True,
            ),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True
    assert adapters[Venue.BINANCE_USDM].submit_calls == 2


@pytest.mark.asyncio
async def test_late_fill_after_apparent_cancel_is_accounted_and_closed(tmp_path: Path) -> None:
    result, _ = await _run(
        tmp_path,
        (
            _outcome(
                PrivateOrderStatus.OPEN,
                cancel_failure=False,
                late_fill_ratio_on_cancel=Decimal("1"),
                duplicate_private_events=True,
            ),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True
    assert result.terminal_state == LiveActionState.FLAT


@pytest.mark.asyncio
async def test_unknown_unknown_never_reports_success_and_quarantines(tmp_path: Path) -> None:
    result, _ = await _run(
        tmp_path,
        (_outcome(PrivateOrderStatus.UNKNOWN, submit_fault="TIMEOUT"),),
        (_outcome(PrivateOrderStatus.UNKNOWN, submit_fault="TIMEOUT"),),
    )
    assert result.success is False
    assert result.orders_sent == 2
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.owner_instruction is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_venue", [Venue.BINANCE_USDM, Venue.OKX])
async def test_filled_unknown_is_flattened_but_never_reported_success(
    tmp_path: Path,
    unknown_venue: Venue,
) -> None:
    unknown = _outcome(PrivateOrderStatus.UNKNOWN, submit_fault="TIMEOUT")
    filled = _outcome(PrivateOrderStatus.FILLED, "1")
    if unknown_venue == Venue.OKX:
        long_outcomes = (
            filled,
            _outcome(PrivateOrderStatus.FILLED, "1"),
        )
        short_outcomes = (unknown, _outcome(PrivateOrderStatus.REJECTED))
    else:
        long_outcomes = (unknown, _outcome(PrivateOrderStatus.REJECTED))
        short_outcomes = (
            filled,
            _outcome(PrivateOrderStatus.FILLED, "1"),
        )
    result, adapters = await _run(tmp_path, long_outcomes, short_outcomes)
    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    positions = [await adapter.fetch_all_positions() for adapter in adapters.values()]
    assert all(not venue_positions for venue_positions in positions)


@pytest.mark.asyncio
async def test_cancel_failure_blocks_flat_success(tmp_path: Path) -> None:
    result, _ = await _run(
        tmp_path,
        (
            _outcome(PrivateOrderStatus.OPEN, cancel_failure=True),
            _outcome(PrivateOrderStatus.REJECTED),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.reconciliation is not None
    assert result.reconciliation.open_bot_order_count == 1


@pytest.mark.asyncio
async def test_failed_protected_close_uses_emergency_flatten_and_verifies_exchange(
    tmp_path: Path,
) -> None:
    result, _ = await _run(
        tmp_path,
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.REJECTED),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.REJECTED),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True
    assert result.recovery_action == "EMERGENCY_FLATTEN"
    assert result.reconciliation is not None
    assert result.reconciliation.flat_verified is True


@pytest.mark.asyncio
async def test_exchange_reported_residual_after_emergency_is_failed_quarantined(
    tmp_path: Path,
) -> None:
    result, _ = await _run(
        tmp_path,
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.REJECTED),
            _outcome(PrivateOrderStatus.PARTIAL, "0.5"),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.REJECTED),
            _outcome(PrivateOrderStatus.PARTIAL, "0.5"),
        ),
    )
    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.reconciliation is not None
    assert result.reconciliation.flat_verified is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "crash_state",
    [
        LiveActionState.FILLED,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
        LiveActionState.FLAT,
    ],
)
async def test_process_restart_after_durable_transition_resumes_without_duplicate_submit(
    tmp_path: Path,
    crash_state: LiveActionState,
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM,
            instruments[Venue.BINANCE_USDM],
            (
                _outcome(PrivateOrderStatus.FILLED, "1"),
                _outcome(PrivateOrderStatus.FILLED, "1"),
            ),
        ),
        Venue.OKX: DeterministicPrivateExchange(
            Venue.OKX,
            instruments[Venue.OKX],
            (
                _outcome(PrivateOrderStatus.FILLED, "1"),
                _outcome(PrivateOrderStatus.FILLED, "1"),
            ),
        ),
        Venue.BYBIT: DeterministicPrivateExchange(
            Venue.BYBIT,
            instruments[Venue.BYBIT],
            (),
        ),
    }
    prices = StaticProtectionProvider(
        {
            (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
            for venue in Venue
            for side in Side
        }
    )
    state_path = tmp_path / "state.sqlite3"
    crashing = LiveCanaryCoordinator(
        CrashAfterTransitionJournal(state_path, crash_state),
        adapters,
        instruments,
        prices,
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )
    with pytest.raises(InjectedRestart):
        await crashing.run(_plan())

    resumed = LiveCanaryCoordinator(
        LiveOrderJournal(state_path),
        adapters,
        instruments,
        prices,
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )
    result = await resumed.run(_plan())
    assert result.success is True
    assert result.terminal_state == LiveActionState.FLAT
    assert adapters[Venue.BINANCE_USDM].submit_calls == 2
    assert adapters[Venue.OKX].submit_calls == 2


@pytest.mark.asyncio
async def test_reverifying_flat_action_never_reactivates_it_for_an_unexpected_action(
    tmp_path: Path,
) -> None:
    result, adapters = await _run(
        tmp_path,
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
        (
            _outcome(PrivateOrderStatus.FILLED, "1"),
            _outcome(PrivateOrderStatus.FILLED, "1"),
        ),
    )
    assert result.success is True and result.terminal_state == LiveActionState.FLAT
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await journal.prepare(
        "eth-pending",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
        "eth-tranche",
        _request(Venue.BINANCE_USDM, "eth-long", Side.BUY),
        _request(Venue.OKX, "eth-short", Side.SELL),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"projected_stress_usdt": "0.8"},
        "b" * 64,
    )
    instruments = {venue: _instrument(venue) for venue in Venue}
    coordinator = LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        StaticProtectionProvider(
            {
                (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
                for venue in Venue
                for side in Side
            }
        ),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )

    replay = await coordinator.run(_plan())

    assert replay.success is False
    completed = await journal.load("cycle-1")
    unexpected = await journal.load("eth-pending")
    assert completed is not None and completed.state == LiveActionState.FLAT
    assert unexpected is not None and unexpected.state == LiveActionState.QUARANTINED


@pytest.mark.asyncio
async def test_journal_balanced_but_exchange_mismatched_positions_are_never_hedged(
    tmp_path: Path,
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        venue: DeterministicPrivateExchange(venue, instruments[venue], ()) for venue in Venue
    }
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    plan = _plan()
    action = await journal.prepare(
        plan.pair_action_id,
        plan.route,
        plan.tranche_id,
        plan.long_request,
        plan.short_request,
        {plan.route.long_venue: plan.quantity, plan.route.short_venue: plan.quantity},
        {plan.route.long_venue: Decimal("100"), plan.route.short_venue: Decimal("100")},
        plan.risk_reservation,
        plan.qualification_hash,
    )
    await journal.mark_submit_attempted(
        action.pair_action_id,
        (plan.long_request.client_order_id, plan.short_request.client_order_id),
    )
    observed = datetime.now(UTC)
    for request in (plan.long_request, plan.short_request):
        order = PrivateOrder(
            venue=request.venue,
            order_id=f"{request.venue.value}-journal-only",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            status=PrivateOrderStatus.FILLED,
            requested_base_quantity=plan.quantity,
            filled_base_quantity=plan.quantity,
            average_price=Decimal("100"),
            fee_usdt=Decimal(0),
            observed_at=observed,
            limit_price=request.price,
        )
        await journal.record_order_event(action.pair_action_id, order, request.client_order_id)
    await journal.transition(action.pair_action_id, LiveActionState.FILLED)

    coordinator = LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        StaticProtectionProvider(
            {
                (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
                for venue in Venue
                for side in Side
            }
        ),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
        flat_barrier_policy=FlatBarrierPolicy(
            consecutive_snapshots=2,
            quiet_period_seconds=0,
            poll_interval_seconds=0.01,
            timeout_seconds=0.2,
        ),
    )
    result = await coordinator.run(plan)

    assert result.success is False
    assert result.hedged is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.reconciliation is not None
    assert result.reconciliation.actual_signed_positions != (
        result.reconciliation.expected_signed_positions
    )
    assert sum(adapter.submit_calls for adapter in adapters.values()) == 0


@pytest.mark.asyncio
async def test_public_protection_failure_falls_through_to_durable_emergency_flatten(
    tmp_path: Path,
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM,
            instruments[Venue.BINANCE_USDM],
            (
                _outcome(PrivateOrderStatus.FILLED, "1"),
                _outcome(PrivateOrderStatus.FILLED, "1"),
            ),
        ),
        Venue.OKX: DeterministicPrivateExchange(
            Venue.OKX,
            instruments[Venue.OKX],
            (_outcome(PrivateOrderStatus.REJECTED),),
        ),
        Venue.BYBIT: DeterministicPrivateExchange(
            Venue.BYBIT,
            instruments[Venue.BYBIT],
            (),
        ),
    }
    result = await LiveCanaryCoordinator(
        LiveOrderJournal(tmp_path / "state.sqlite3"),
        adapters,
        instruments,
        FailingProtectionProvider(),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    ).run(_plan())

    assert result.success is False
    assert result.reason == ReasonCode.FORCED_CLOSED
    assert result.terminal_state == LiveActionState.FLAT
    assert result.recovery_action == "EMERGENCY_FLATTEN"


@pytest.mark.asyncio
async def test_portfolio_close_reduces_only_one_tranche_and_preserves_remaining_owner(
    tmp_path: Path,
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    filled = tuple(_outcome(PrivateOrderStatus.FILLED, "1") for _ in range(3))
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM, instruments[Venue.BINANCE_USDM], filled
        ),
        Venue.OKX: DeterministicPrivateExchange(Venue.OKX, instruments[Venue.OKX], filled),
        Venue.BYBIT: DeterministicPrivateExchange(Venue.BYBIT, instruments[Venue.BYBIT], ()),
    }
    journal = LiveOrderJournal(tmp_path / "portfolio.sqlite3")
    protection = StaticProtectionProvider(
        {
            (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
            for venue in Venue
            for side in Side
        }
    )

    def plan(index: int) -> CanaryExecutionPlan:
        action_id = f"cycle-{index}"
        return replace(
            _plan(),
            pair_action_id=action_id,
            tranche_id=f"tranche-{index}",
            long_request=_request(
                Venue.BINANCE_USDM,
                venue_client_order_id(action_id, "long"),
                Side.BUY,
            ),
            short_request=_request(
                Venue.OKX,
                venue_client_order_id(action_id, "short"),
                Side.SELL,
            ),
        )

    for index in (1, 2):
        opened = await LiveCanaryCoordinator(
            journal,
            adapters,
            instruments,
            protection,
            DeterministicCanaryMonitor(CloseReason.CANARY_TIMEOUT),
            Venue.BYBIT,
            portfolio_mode=True,
        ).run(plan(index))
        assert opened.success and opened.terminal_state == LiveActionState.HEDGED

    closed = await LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        protection,
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
        portfolio_mode=True,
    ).run(plan(2))

    first = await journal.load("cycle-1")
    second = await journal.load("cycle-2")
    assert closed.success and closed.portfolio_reconciled
    assert not closed.flat_barrier_verified
    assert first is not None and first.state == LiveActionState.HEDGED
    assert second is not None and second.state == LiveActionState.FLAT
    for venue in (Venue.BINANCE_USDM, Venue.OKX):
        assert adapters[venue].submit_calls == 3
        positions = await adapters[venue].fetch_all_positions()
        assert len(positions) == 1 and positions[0].base_quantity == Decimal("0.001")


@pytest.mark.asyncio
async def test_hard_route_exit_closes_all_tranches_under_one_flat_barrier(tmp_path: Path) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    filled = tuple(_outcome(PrivateOrderStatus.FILLED, "1") for _ in range(10))
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM, instruments[Venue.BINANCE_USDM], filled
        ),
        Venue.OKX: DeterministicPrivateExchange(Venue.OKX, instruments[Venue.OKX], filled),
        Venue.BYBIT: DeterministicPrivateExchange(Venue.BYBIT, instruments[Venue.BYBIT], ()),
    }
    journal = LiveOrderJournal(tmp_path / "route-flat.sqlite3")
    protection = StaticProtectionProvider(
        {
            (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
            for venue in Venue
            for side in Side
        }
    )

    def plan(index: int) -> CanaryExecutionPlan:
        action_id = f"route-cycle-{index}"
        return replace(
            _plan(),
            pair_action_id=action_id,
            tranche_id=f"route-tranche-{index}",
            long_request=_request(
                Venue.BINANCE_USDM,
                venue_client_order_id(action_id, "long"),
                Side.BUY,
            ),
            short_request=_request(
                Venue.OKX,
                venue_client_order_id(action_id, "short"),
                Side.SELL,
            ),
        )

    for index in range(1, 6):
        opened = await LiveCanaryCoordinator(
            journal,
            adapters,
            instruments,
            protection,
            DeterministicCanaryMonitor(CloseReason.CANARY_TIMEOUT),
            Venue.BYBIT,
            portfolio_mode=True,
        ).run(plan(index))
        assert opened.success and opened.terminal_state == LiveActionState.HEDGED

    closed = await LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        protection,
        DeterministicCanaryMonitor(CloseReason.HARD_HOLDING_TIME),
        Venue.BYBIT,
        portfolio_mode=True,
    ).run(plan(5))

    assert closed.success and closed.flat_barrier_verified and closed.portfolio_reconciled
    assert closed.close_reason == CloseReason.HARD_HOLDING_TIME
    assert await journal.active_actions() == ()
    for venue in (Venue.BINANCE_USDM, Venue.OKX):
        assert adapters[venue].submit_calls == 10
        assert await adapters[venue].fetch_all_positions() == ()


@pytest.mark.parametrize("strategy", ["AGGRESSIVE_SYMBIOSIS_V1", "AGGRESSIVE_FAST_LIVE_V2"])
@pytest.mark.asyncio
async def test_real_fill_repricing_persists_reserves_and_closes_at_hard_limit(
    tmp_path: Path,
    strategy: str,
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM,
            instruments[Venue.BINANCE_USDM],
            tuple(_outcome(PrivateOrderStatus.FILLED, "1") for _ in range(2)),
        ),
        Venue.OKX: DeterministicPrivateExchange(
            Venue.OKX,
            instruments[Venue.OKX],
            tuple(_outcome(PrivateOrderStatus.FILLED, "1") for _ in range(2)),
        ),
        Venue.BYBIT: DeterministicPrivateExchange(
            Venue.BYBIT,
            instruments[Venue.BYBIT],
            (),
        ),
    }
    journal = LiveOrderJournal(tmp_path / "actual-risk.sqlite3")
    policy = load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    plan = _aggressive_hard_breach_plan()
    plan = replace(
        plan,
        risk_reservation={
            **plan.risk_reservation,
            "strategy": strategy,
            "initial_measured_book_impact_usdt": "0.2",
            "initial_adverse_funding_reserve_usdt": (
                "0.3" if strategy == "AGGRESSIVE_FAST_LIVE_V2" else "0.1"
            ),
            "initial_remaining_close_fees_usdt": (
                "0.4" if strategy == "AGGRESSIVE_FAST_LIVE_V2" else "0.2"
            ),
            "initial_total_reserves_usdt": (
                "1.7" if strategy == "AGGRESSIVE_FAST_LIVE_V2" else "0.9"
            ),
        },
    )
    result = await LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        StaticProtectionProvider(
            {
                (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
                for venue in Venue
                for side in Side
            }
        ),
        DeterministicCanaryMonitor(CloseReason.CANARY_TIMEOUT),
        Venue.BYBIT,
        aggressive_policy=policy,
    ).run(plan)
    stored = await journal.load("cycle-1")

    assert result.success and result.close_reason == CloseReason.HARD_STOP_OR_LOSS
    assert result.terminal_state == LiveActionState.FLAT
    assert stored is not None
    actual = stored.risk_reservation["actual_fill_risk"]
    # Includes the explicit 0.20 USDT reserve for the two still-unexecuted close legs.
    assert Decimal(str(actual["incremental_stress_usdt"])) >= Decimal("1.2001")
    assert Decimal(str(actual["actual_open_fees_usdt"])) > 0
    assert actual["initial_measured_book_impact_usdt"] == "0.2"
    assert actual["other_reserves_usdt"] == (
        "1.7" if strategy == "AGGRESSIVE_FAST_LIVE_V2" else "0.9"
    )
    assert actual["adverse_funding_usdt"] == (
        "0.3" if strategy == "AGGRESSIVE_FAST_LIVE_V2" else "0.1"
    )
    assert actual["remaining_close_fees_usdt"] == (
        "0.4" if strategy == "AGGRESSIVE_FAST_LIVE_V2" else "0.2"
    )
    assert all(
        adapter.submit_calls == 2 for venue, adapter in adapters.items() if venue != Venue.BYBIT
    )


@pytest.mark.parametrize("strategy", ["AGGRESSIVE_SYMBIOSIS_V1", "AGGRESSIVE_FAST_LIVE_V2"])
@pytest.mark.asyncio
async def test_partial_fill_hard_breach_reduces_before_opening_top_up(
    tmp_path: Path,
    strategy: str,
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM,
            instruments[Venue.BINANCE_USDM],
            (
                _outcome(PrivateOrderStatus.FILLED, "1"),
                _outcome(PrivateOrderStatus.FILLED, "1"),
            ),
        ),
        Venue.OKX: DeterministicPrivateExchange(
            Venue.OKX,
            instruments[Venue.OKX],
            (_outcome(PrivateOrderStatus.REJECTED),),
        ),
        Venue.BYBIT: DeterministicPrivateExchange(Venue.BYBIT, instruments[Venue.BYBIT], ()),
    }
    plan = _aggressive_hard_breach_plan()
    plan = replace(
        plan,
        risk_reservation={
            **plan.risk_reservation,
            "route_hard_loss_usdt": "0.85",
            "strategy": strategy,
            "initial_measured_book_impact_usdt": "0.2",
            "initial_adverse_funding_reserve_usdt": "0.1",
            "initial_remaining_close_fees_usdt": "0.2",
            "initial_total_reserves_usdt": "0.9",
        },
    )
    journal = LiveOrderJournal(tmp_path / "partial-risk.sqlite3")
    policy = load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    result = await LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        StaticProtectionProvider(
            {
                (venue, side): Decimal("101") if side == Side.BUY else Decimal("99")
                for venue in Venue
                for side in Side
            }
        ),
        DeterministicCanaryMonitor(CloseReason.CANARY_TIMEOUT),
        Venue.BYBIT,
        aggressive_policy=policy,
    ).run(plan)

    stored = await journal.load(plan.pair_action_id)
    assert result.success is False and result.reason == ReasonCode.FORCED_CLOSED
    assert result.recovery_action == "ACTUAL_FILL_HARD_BREACH_REDUCE"
    assert result.terminal_state == LiveActionState.FLAT
    assert adapters[Venue.BINANCE_USDM].submit_calls == 2
    assert adapters[Venue.OKX].submit_calls == 1  # no risk-increasing top-up
    assert stored is not None
    assert Decimal(
        str(stored.risk_reservation["actual_fill_risk"]["incremental_stress_usdt"])
    ) >= Decimal("0.85")
