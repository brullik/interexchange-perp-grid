from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from interexchange_perp_grid.live_journal import LiveActionState, LiveOrderJournal
from interexchange_perp_grid.private_transition_smoke import (
    run_private_transition_recovery_smoke,
)
from interexchange_perp_grid.supervisor_smoke import (
    RecoverySmokeTransition,
    run_supervisor_recovery_smoke,
)


@pytest.mark.asyncio
async def test_simulated_process_kill_then_restart_recovers_durable_partial(
    tmp_path: Path,
) -> None:
    state = tmp_path / "smoke.sqlite3"
    ready = tmp_path / "ready"
    killed_process = asyncio.create_task(
        run_supervisor_recovery_smoke(
            state,
            hold_after_active=True,
            ready_path=ready,
        )
    )
    for _ in range(100):
        if ready.is_file():
            break
        await asyncio.sleep(0.01)
    assert ready.is_file()
    assert len(ready.read_text(encoding="utf-8").splitlines()) == 10
    journal = LiveOrderJournal(state)
    assert len(await journal.active_actions()) == 10
    killed_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await killed_process

    result = await run_supervisor_recovery_smoke(state, hold_after_active=False)
    assert result["status"] == "PASS"
    assert result["process_restart_recovery"] is True
    assert result["expected_action_count"] == 10
    assert result["recovered_action_count"] == 10
    assert result["portfolio_stress_usdt"] == "50"
    assert result["production_exchange_transports_opened"] == 0
    assert await journal.active_actions() == ()


@pytest.mark.asyncio
async def test_second_restart_recovers_only_the_remaining_nine_actions(tmp_path: Path) -> None:
    state = tmp_path / "second-restart.sqlite3"
    ready = tmp_path / "second-ready"
    first_process = asyncio.create_task(
        run_supervisor_recovery_smoke(state, hold_after_active=True, ready_path=ready)
    )
    for _ in range(100):
        if ready.is_file():
            break
        await asyncio.sleep(0.01)
    journal = LiveOrderJournal(state)
    actions = await journal.active_actions()
    assert len(actions) == 10
    first = actions[0]
    await journal.transition(first.pair_action_id, LiveActionState.RECOVERING)
    await journal.transition(first.pair_action_id, LiveActionState.FLAT)
    first_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_process

    result = await run_supervisor_recovery_smoke(state, hold_after_active=False)

    assert result["status"] == "PASS"
    assert result["expected_action_count"] == 10
    assert result["recovered_action_count"] == 9
    assert result["portfolio_stress_usdt"] == "45"
    assert await journal.active_actions() == ()


@pytest.mark.asyncio
async def test_restart_smoke_rejects_nonfinite_durable_stress(tmp_path: Path) -> None:
    state = tmp_path / "nonfinite.sqlite3"
    ready = tmp_path / "nonfinite-ready"
    first_process = asyncio.create_task(
        run_supervisor_recovery_smoke(state, hold_after_active=True, ready_path=ready)
    )
    for _ in range(100):
        if ready.is_file():
            break
        await asyncio.sleep(0.01)
    first_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_process
    with sqlite3.connect(state) as database:
        database.execute(
            "UPDATE live_pair_actions SET risk_reservation_json = ? WHERE pair_action_id = ?",
            ('{"projected_stress_usdt":"NaN"}', "docker-supervisor-recovery-00"),
        )

    with pytest.raises(RuntimeError, match="route stress exceeds"):
        await run_supervisor_recovery_smoke(state, hold_after_active=False)

    journal = LiveOrderJournal(state)
    assert len(await journal.active_actions()) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transition_state",
    [
        LiveActionState.PREPARED,
        LiveActionState.SUBMITTING,
        LiveActionState.ACKNOWLEDGED,
        LiveActionState.PARTIAL,
        LiveActionState.FILLED,
        LiveActionState.REJECTED,
        LiveActionState.UNKNOWN,
        LiveActionState.RECOVERING,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
        LiveActionState.QUARANTINED,
    ],
)
async def test_restart_smoke_recovers_every_durable_active_transition(
    tmp_path: Path,
    transition_state: LiveActionState,
) -> None:
    state = tmp_path / f"transition-{transition_state.value}.sqlite3"
    ready = tmp_path / f"transition-{transition_state.value}.ready"
    killed_process = asyncio.create_task(
        run_supervisor_recovery_smoke(
            state,
            hold_after_active=True,
            ready_path=ready,
            action_count=1,
            transition_state=transition_state,
        )
    )
    for _ in range(100):
        if ready.is_file():
            break
        await asyncio.sleep(0.01)
    assert ready.read_text(encoding="utf-8").strip().endswith(f":{transition_state.value}")
    killed_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await killed_process

    result = await run_supervisor_recovery_smoke(
        state,
        hold_after_active=False,
        action_count=1,
        transition_state=transition_state,
    )

    assert result["status"] == "PASS"
    assert result["restarted_transition_state"] == transition_state.value
    assert await LiveOrderJournal(state).active_actions() == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("transition_state", tuple(RecoverySmokeTransition))
async def test_killed_process_recovers_through_private_transport_and_stable_flat(
    tmp_path: Path,
    transition_state: RecoverySmokeTransition,
) -> None:
    state = tmp_path / f"private-{transition_state.value}.sqlite3"
    private_state = tmp_path / f"private-{transition_state.value}"
    ready = tmp_path / f"private-{transition_state.value}.ready"
    killed_process = asyncio.create_task(
        run_private_transition_recovery_smoke(
            state,
            private_state,
            hold_after_active=True,
            transition_state=transition_state,
            ready_path=ready,
            action_count=1,
        )
    )
    for _ in range(100):
        if ready.is_file():
            break
        await asyncio.sleep(0.01)
    assert ready.read_text(encoding="utf-8").strip().endswith(f":{transition_state.value}")
    killed_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await killed_process

    result = await run_private_transition_recovery_smoke(
        state,
        private_state,
        hold_after_active=False,
        transition_state=transition_state,
        action_count=1,
    )

    assert result["status"] == "PASS"
    assert result["expected_action_count"] == 1
    assert result["recovered_action_count"] == 1
    assert result["flat_barrier_verified"] is True
    assert result["priority_scheduler_restart_proof"] is True
    assert result["new_entry_shed_while_p0_active"] is True
    assert result["production_exchange_transports_opened"] == 0


@pytest.mark.asyncio
async def test_private_priority_restart_recovers_maximum_ten_actions(tmp_path: Path) -> None:
    state = tmp_path / "private-priority-ten.sqlite3"
    private_state = tmp_path / "private-priority-ten"
    ready = tmp_path / "private-priority-ten.ready"
    killed_process = asyncio.create_task(
        run_private_transition_recovery_smoke(
            state,
            private_state,
            hold_after_active=True,
            transition_state=RecoverySmokeTransition.PARTIAL,
            ready_path=ready,
            action_count=10,
        )
    )
    for _ in range(100):
        if ready.is_file():
            break
        await asyncio.sleep(0.01)
    assert len(ready.read_text(encoding="utf-8").splitlines()) == 10
    killed_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await killed_process

    result = await run_private_transition_recovery_smoke(
        state,
        private_state,
        hold_after_active=False,
        transition_state=RecoverySmokeTransition.PARTIAL,
        action_count=10,
    )

    assert result["status"] == "PASS"
    assert result["expected_action_count"] == result["recovered_action_count"] == 10
    assert result["priority_scheduler_restart_proof"] is True
    assert result["new_entry_shed_while_p0_active"] is True
