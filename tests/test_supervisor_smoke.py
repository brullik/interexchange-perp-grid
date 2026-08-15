from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from interexchange_perp_grid.supervisor_smoke import run_supervisor_recovery_smoke


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
    killed_process.cancel()
    with pytest.raises(asyncio.CancelledError):
        await killed_process

    result = await run_supervisor_recovery_smoke(state, hold_after_active=False)
    assert result["status"] == "PASS"
    assert result["process_restart_recovery"] is True
    assert result["production_exchange_transports_opened"] == 0
