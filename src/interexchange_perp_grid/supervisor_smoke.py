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
) -> dict[str, object]:
    """Docker-only deterministic process-kill smoke; it never opens exchange transports."""
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    active_actions = await journal.active_actions()
    if len(active_actions) > 1:
        raise RuntimeError("recovery smoke supports exactly one durable action")
    active = active_actions[0] if active_actions else None
    if hold_after_active:
        if active is None:
            active = await _prepare_partial(journal)
        if ready_path is not None:
            ready_path.write_text(
                f"{active.pair_action_id}:{active.state.value}\n",
                encoding="utf-8",
            )
        await asyncio.Event().wait()
        raise RuntimeError("unreachable")
    if active is None:
        raise RuntimeError("recovery smoke requires durable active state from killed process")

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
        "production_exchange_transports_opened": 0,
        "health": asdict(health),
    }


async def _prepare_partial(journal: LiveOrderJournal) -> LiveJournalAction:
    route = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
    long_request = _request(Venue.BINANCE_USDM, Side.BUY, "long")
    short_request = _request(Venue.OKX, Side.SELL, "short")
    action = await journal.prepare(
        "docker-supervisor-recovery-smoke",
        route,
        "smoke-tranche",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"simulator": True, "production_submit_calls": 0},
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


def _request(venue: Venue, side: Side, role: str) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue,
        venue_client_order_id("docker-supervisor-recovery-smoke", role),
        "BTC/USDT:USDT",
        side,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {"timeInForce": "IOC"},
    )
