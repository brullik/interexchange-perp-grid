from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.service import BootstrapService
from interexchange_perp_grid.state import read_service_health

CONFIG = Path("config/defaults.yaml")


async def wait_for_health(path: Path, expected_starts: int) -> None:
    for _ in range(100):
        health = await read_service_health(path, max_age_seconds=5)
        if health.healthy and health.starts == expected_starts:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("service did not become healthy")


async def run_once(service: BootstrapService, path: Path, expected_starts: int) -> None:
    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run(stop_event))
    await wait_for_health(path, expected_starts)
    stop_event.set()
    await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_service_heartbeat_survives_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "service.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    service = BootstrapService(settings, heartbeat_interval_seconds=0.01, run_shadow=False)

    await run_once(service, state_path, expected_starts=1)
    stopped = await read_service_health(state_path, max_age_seconds=5)
    assert stopped.healthy is False
    assert stopped.reason == ReasonCode.SERVICE_NOT_RUNNING

    await run_once(service, state_path, expected_starts=2)
    restarted = await read_service_health(state_path, max_age_seconds=5)
    assert restarted.starts == 2
    assert restarted.status == "stopped"
