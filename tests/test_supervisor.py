from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest

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

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)


def _request(venue: Venue, side: Side, role: str) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue=venue,
        client_order_id=venue_client_order_id("supervisor-test", role),
        symbol="BTC/USDT:USDT",
        side=side,
        order_type="limit",
        amount_contracts=Decimal("1"),
        price=Decimal("100"),
        time_in_force="IOC",
        params={"timeInForce": "IOC"},
    )


async def _prepare(journal: LiveOrderJournal) -> LiveJournalAction:
    await journal.initialise()
    long_request = _request(Venue.BINANCE_USDM, Side.BUY, "long")
    short_request = _request(Venue.OKX, Side.SELL, "short")
    return await journal.prepare(
        "supervisor-action",
        _ROUTE,
        "tranche-1",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"supervisor_intent": "LIVE_CANARY", "supervisor_queued": True},
        "a" * 64,
    )


async def _move_to(journal: LiveOrderJournal, target: LiveActionState) -> LiveJournalAction:
    action = await _prepare(journal)
    if target == LiveActionState.PREPARED:
        return action
    await journal.mark_submit_attempted(
        action.pair_action_id,
        tuple(leg.client_order_id for leg in action.legs),
    )
    loaded = await journal.load(action.pair_action_id)
    assert loaded is not None
    action = loaded
    if target == LiveActionState.SUBMITTING:
        return action
    action = await journal.transition(action.pair_action_id, LiveActionState.PARTIAL)
    if target == LiveActionState.PARTIAL:
        return action
    action = await journal.transition(action.pair_action_id, LiveActionState.HEDGED)
    if target == LiveActionState.HEDGED:
        return action
    return await journal.transition(action.pair_action_id, LiveActionState.CLOSING)


async def _force_exchange_verified_flat(
    journal: LiveOrderJournal,
    action: LiveJournalAction,
) -> None:
    if action.state == LiveActionState.PREPARED:
        action = await journal.transition(action.pair_action_id, LiveActionState.QUARANTINED)
    if action.state == LiveActionState.HEDGED:
        action = await journal.transition(action.pair_action_id, LiveActionState.CLOSING)
    if action.state not in {LiveActionState.RECOVERING, LiveActionState.QUARANTINED}:
        action = await journal.transition(action.pair_action_id, LiveActionState.RECOVERING)
    await journal.transition(
        action.pair_action_id,
        LiveActionState.FLAT,
        {"exchange_verified": True},
        residual_delta=Decimal(0),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "restart_state",
    [
        LiveActionState.PREPARED,
        LiveActionState.PARTIAL,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
    ],
)
async def test_restart_enters_recovery_only_and_reaches_flat(
    tmp_path: Path,
    restart_state: LiveActionState,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _move_to(journal, restart_state)
    observed: list[LiveActionState] = []

    async def recover(action: LiveJournalAction) -> object:
        observed.append(action.state)
        await _force_exchange_verified_flat(journal, action)
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, poll_interval_seconds=0.01)
    health = await supervisor.reconcile_once()

    assert observed == [restart_state]
    assert health.mode == SupervisorMode.IDLE
    assert health.action_state == LiveActionState.FLAT
    assert health.recovery_required is False
    assert await journal.active() is None
    assert await supervisor.health() == health


@pytest.mark.asyncio
async def test_supervisor_detects_new_durable_intent_without_process_restart(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    recovered = asyncio.Event()

    async def recover(action: LiveJournalAction) -> object:
        await _force_exchange_verified_flat(journal, action)
        recovered.set()
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, poll_interval_seconds=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(supervisor.run(stop))
    try:
        for _ in range(100):
            if (await supervisor.health()).mode == SupervisorMode.IDLE:
                break
            await asyncio.sleep(0.01)
        await _prepare(journal)
        await asyncio.wait_for(recovered.wait(), timeout=2)
        assert await journal.active() is None
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_supervisor_persists_fail_closed_health_when_recovery_raises(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    action = await _move_to(journal, LiveActionState.PARTIAL)

    async def fail_recovery(active: LiveJournalAction) -> object:
        assert active.pair_action_id == action.pair_action_id
        raise ConnectionError("private transport unavailable")

    supervisor = LiveSafetySupervisor(journal, fail_recovery)
    health = await supervisor.reconcile_once()

    assert health.mode == SupervisorMode.BLOCKED
    assert health.recovery_required is True
    assert health.failure == "ConnectionError"
    assert health.active_pair_action_id == action.pair_action_id
    assert await journal.active() is not None
