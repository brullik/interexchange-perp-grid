from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

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
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.live_simulator import (
    DeterministicCanaryMonitor,
    DeterministicPrivateExchange,
    ScriptedOrderOutcome,
    StaticProtectionProvider,
)
from interexchange_perp_grid.private_domain import PrivateOrderStatus, VenueOrderRequest
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


@pytest.mark.parametrize(
    ("signals", "expected"),
    [
        (CanaryCloseSignals(target_converged=True), CloseReason.TARGET_CONVERGENCE),
        (CanaryCloseSignals(risk_deteriorated=True), CloseReason.RISK_DETERIORATION),
        (CanaryCloseSignals(funding_deteriorated=True), CloseReason.FUNDING_DETERIORATION),
        (CanaryCloseSignals(public_or_private_data_stale=True), CloseReason.STALE_DATA),
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
        long_request=_request(Venue.BINANCE_USDM, "ipeg-cycle-long", Side.BUY),
        short_request=_request(Venue.OKX, "ipeg-cycle-short", Side.SELL),
        risk_reservation={"projected_stress_usdt": "0.8"},
        qualification_hash="a" * 64,
        timeout_seconds=30,
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
