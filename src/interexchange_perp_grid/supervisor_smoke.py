from __future__ import annotations

import asyncio
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.private_domain import VenueOrderRequest
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.supervisor import LiveSafetySupervisor, SupervisorMode


async def run_supervisor_recovery_smoke(
    state_path: Path,
    *,
    hold_after_active: bool,
    ready_path: Path | None = None,
    action_count: int = 10,
) -> dict[str, object]:
    """Docker-only deterministic process-kill smoke; it never opens exchange transports."""
    if not 1 <= action_count <= 10:
        raise ValueError("supervisor recovery smoke action count must be between 1 and 10")
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    active_actions = await journal.active_actions()
    expected_ids = tuple(f"docker-supervisor-recovery-{index:02d}" for index in range(action_count))
    expected_actions = tuple(
        action
        for action in await asyncio.gather(*(journal.load(action_id) for action_id in expected_ids))
        if action is not None
    )
    if hold_after_active:
        if not expected_actions:
            active_actions = tuple(
                [await _prepare_partial(journal, index) for index in range(action_count)]
            )
            expected_actions = active_actions
        _validate_expected_actions(expected_actions, active_actions, expected_ids)
        if ready_path is not None:
            ready_path.write_text(
                "".join(
                    f"{action.pair_action_id}:{action.state.value}\n" for action in active_actions
                ),
                encoding="utf-8",
            )
        await asyncio.Event().wait()
        raise RuntimeError("unreachable")
    if not expected_actions:
        raise RuntimeError("recovery smoke requires durable active state from killed process")
    _validate_expected_actions(expected_actions, active_actions, expected_ids)
    projected_stress = tuple(
        Decimal(str(action.risk_reservation["projected_stress_usdt"])) for action in active_actions
    )
    if any(
        not stress.is_finite() or stress < 0 or stress > Decimal("5") for stress in projected_stress
    ):
        raise RuntimeError("recovery smoke route stress exceeds the locked limit")
    portfolio_stress = sum(projected_stress, Decimal(0))
    if not portfolio_stress.is_finite() or portfolio_stress > Decimal("50"):
        raise RuntimeError("recovery smoke portfolio stress exceeds the locked limit")

    async def recover(action: LiveJournalAction) -> object:
        current = action
        if current.state == LiveActionState.HEDGED:
            current = await journal.transition(current.pair_action_id, LiveActionState.CLOSING)
        if current.state not in {LiveActionState.RECOVERING, LiveActionState.QUARANTINED}:
            current = await journal.transition(
                current.pair_action_id,
                LiveActionState.RECOVERING,
                {"simulator_exchange_reconciled": True},
            )
        await journal.transition(
            current.pair_action_id,
            LiveActionState.FLAT,
            {"simulator_stable_flat": True},
            residual_delta=Decimal(0),
        )
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, poll_interval_seconds=0.01)
    health = await supervisor.reconcile_once()
    if health.mode != SupervisorMode.IDLE or await journal.active_actions():
        raise RuntimeError("supervisor recovery smoke did not reach FLAT")
    return {
        "status": "PASS",
        "process_restart_recovery": True,
        "expected_action_count": len(expected_actions),
        "recovered_action_count": len(active_actions),
        "portfolio_stress_usdt": str(portfolio_stress),
        "production_exchange_transports_opened": 0,
        "health": asdict(health),
    }


def _validate_expected_actions(
    expected_actions: tuple[LiveJournalAction, ...],
    active_actions: tuple[LiveJournalAction, ...],
    expected_ids: tuple[str, ...],
) -> None:
    if tuple(action.pair_action_id for action in expected_actions) != expected_ids:
        raise RuntimeError("recovery smoke durable action set is incomplete")
    if any(action.pair_action_id not in expected_ids for action in active_actions):
        raise RuntimeError("recovery smoke found an unexpected durable active action")


async def _prepare_partial(journal: LiveOrderJournal, index: int) -> LiveJournalAction:
    base = f"A{index:03d}"
    action_id = f"docker-supervisor-recovery-{index:02d}"
    route = DirectedRouteKey(base, Venue.BINANCE_USDM, Venue.OKX)
    long_request = _request(Venue.BINANCE_USDM, Side.BUY, action_id, "long")
    short_request = _request(Venue.OKX, Side.SELL, action_id, "short")
    action = await journal.prepare(
        action_id,
        route,
        f"smoke-tranche-{index:02d}",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "projected_stress_usdt": "5",
            "simulator": True,
            "production_submit_calls": 0,
        },
        "0" * 64,
    )
    await journal.mark_submit_attempted(
        action.pair_action_id,
        (long_request.client_order_id, short_request.client_order_id),
    )
    return await journal.transition(
        action.pair_action_id,
        LiveActionState.PARTIAL,
        {"simulated_one_leg_may_have_filled": True},
        residual_delta=Decimal("0.001"),
    )


def _request(venue: Venue, side: Side, action_id: str, role: str) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue,
        venue_client_order_id(action_id, role),
        "BTC/USDT:USDT",
        side,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {"timeInForce": "IOC"},
    )
