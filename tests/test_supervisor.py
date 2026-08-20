from __future__ import annotations

import asyncio
from dataclasses import replace
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


async def _prepare_named(
    journal: LiveOrderJournal,
    pair_action_id: str,
    base: str,
) -> LiveJournalAction:
    await journal.initialise()
    route = DirectedRouteKey(base, Venue.BINANCE_USDM, Venue.OKX)
    long_request = replace(
        _request(Venue.BINANCE_USDM, Side.BUY, f"{base.lower()}-long"),
        client_order_id=venue_client_order_id(pair_action_id, "long"),
    )
    short_request = replace(
        _request(Venue.OKX, Side.SELL, f"{base.lower()}-short"),
        client_order_id=venue_client_order_id(pair_action_id, "short"),
    )
    return await journal.prepare(
        pair_action_id,
        route,
        "tranche-1",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"supervisor_intent": "MULTI_ACTION_RECOVERY"},
        "b" * 64,
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
    assert health.active_action_count == 0
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
    assert health.failure == f"{action.pair_action_id}:ConnectionError"
    assert health.active_pair_action_id == action.pair_action_id
    assert health.active_action_count == 1
    assert await journal.active() is not None


@pytest.mark.asyncio
async def test_one_route_recovery_failure_does_not_block_another_route_flatten(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    failed = await _prepare_named(journal, "action-btc", "BTC")
    recovered = await _prepare_named(journal, "action-eth", "ETH")
    recovered_flat = asyncio.Event()

    async def recover(action: LiveJournalAction) -> object:
        if action.pair_action_id == failed.pair_action_id:
            await asyncio.wait_for(recovered_flat.wait(), timeout=1)
            raise ConnectionError("BTC private transport unavailable")
        await _force_exchange_verified_flat(journal, action)
        recovered_flat.set()
        return object()

    supervisor = LiveSafetySupervisor(journal, recover)
    health = await supervisor.reconcile_once()

    assert recovered_flat.is_set()
    assert health.mode == SupervisorMode.BLOCKED
    assert health.recovery_required is True
    assert health.active_pair_action_id == failed.pair_action_id
    assert health.active_action_count == 1
    assert health.failure == "action-btc:ConnectionError"
    active = await journal.active_actions()
    assert tuple(action.pair_action_id for action in active) == (failed.pair_action_id,)
    recovered_action = await journal.load(recovered.pair_action_id)
    assert recovered_action is not None and recovered_action.state == LiveActionState.FLAT


@pytest.mark.asyncio
async def test_supervisor_stop_health_is_fail_closed_with_multiple_actions(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _prepare_named(journal, "action-btc", "BTC")
    await _prepare_named(journal, "action-eth", "ETH")

    async def should_not_run(_action: LiveJournalAction) -> object:
        raise AssertionError("pre-stopped supervisor must not begin recovery")

    supervisor = LiveSafetySupervisor(journal, should_not_run)
    stop = asyncio.Event()
    stop.set()
    await supervisor.run(stop)
    health = await supervisor.health()

    assert health.mode == SupervisorMode.STOPPED
    assert health.recovery_required is True
    assert health.active_pair_action_id == "action-btc"
    assert health.action_state is None
    assert health.active_action_count == 2


@pytest.mark.asyncio
async def test_hung_route_recovery_is_single_flight_while_other_route_reaches_flat(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    hung = await _prepare_named(journal, "action-btc", "BTC")
    await _prepare_named(journal, "action-eth", "ETH")
    hung_started = asyncio.Event()
    hung_release = asyncio.Event()
    hung_calls = 0

    async def recover(action: LiveJournalAction) -> object:
        nonlocal hung_calls
        if action.pair_action_id == hung.pair_action_id:
            hung_calls += 1
            hung_started.set()
            await hung_release.wait()
            return object()
        await _force_exchange_verified_flat(journal, action)
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, recovery_timeout_seconds=0.02)
    first = await supervisor.reconcile_once()
    await asyncio.wait_for(hung_started.wait(), timeout=1)
    second = await supervisor.reconcile_once()

    assert first.mode == second.mode == SupervisorMode.BLOCKED
    assert first.failure is not None and "action-btc:TimeoutError" in first.failure
    assert second.failure == "action-btc:TimeoutError"
    assert hung_calls == 1
    assert tuple(action.pair_action_id for action in await journal.active_actions()) == (
        hung.pair_action_id,
    )

    hung_release.set()
    await asyncio.sleep(0)
    third = await supervisor.reconcile_once()
    assert third.mode == SupervisorMode.BLOCKED
    assert third.failure is None
    assert hung_calls == 1


@pytest.mark.asyncio
async def test_supervisor_shutdown_reports_cancellation_resistant_recovery(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _prepare_named(journal, "action-btc", "BTC")
    started = asyncio.Event()
    release = asyncio.Event()

    async def resistant(_action: LiveJournalAction) -> object:
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        return object()

    supervisor = LiveSafetySupervisor(journal, resistant, recovery_timeout_seconds=0.02)
    health = await supervisor.reconcile_once()
    assert health.failure == "action-btc:TimeoutError"
    await asyncio.wait_for(started.wait(), timeout=1)
    stop = asyncio.Event()
    stop.set()

    with pytest.raises(RuntimeError, match="recovery tasks remain active"):
        await supervisor.run(stop)
    stopped = await supervisor.health()
    assert stopped.mode == SupervisorMode.STOPPED
    assert stopped.failure == "RecoveryShutdownTimeout"

    release.set()
    await asyncio.wait_for(
        asyncio.gather(*supervisor._recovery_tasks.values()),
        timeout=1,
    )
