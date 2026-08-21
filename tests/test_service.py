from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import interexchange_perp_grid.service as service_module
from interexchange_perp_grid.autonomous_orchestrator import AutonomousOrchestrator
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.service import BootstrapService, run_for_duration
from interexchange_perp_grid.shadow import ContinuousShadowEvaluator
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


@pytest.mark.asyncio
async def test_service_owns_exactly_one_telegram_poller(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "service.sqlite3"
    settings = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(state_path),
            "IPEG_TELEGRAM_ENABLED": "true",
            "IPEG_TELEGRAM_OWNER_CHAT_ID": "123",
        },
    )
    telegram_started = asyncio.Event()
    poller_calls = 0

    async def fake_telegram(*args: object, **kwargs: object) -> None:
        nonlocal poller_calls
        del kwargs
        poller_calls += 1
        telegram_started.set()
        stop_event = args[2]
        assert isinstance(stop_event, asyncio.Event)
        await stop_event.wait()

    async def fake_shadow_run(self: object, stop_event: asyncio.Event) -> None:
        del self
        await stop_event.wait()

    monkeypatch.setattr(service_module, "run_telegram_bot", fake_telegram)
    monkeypatch.setattr(ContinuousShadowEvaluator, "run", fake_shadow_run)
    service = BootstrapService(
        settings,
        heartbeat_interval_seconds=0.01,
        supervisor_poll_interval_seconds=0.01,
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run(stop_event))
    try:
        await asyncio.wait_for(telegram_started.wait(), timeout=2)
        await wait_for_health(state_path, 1)
        assert poller_calls == 1
    finally:
        stop_event.set()
        await asyncio.wait_for(task, timeout=2)

    assert poller_calls == 1


@pytest.mark.asyncio
async def test_service_owns_one_persistent_autonomous_orchestrator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "service.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    started = asyncio.Event()
    calls = 0

    async def fake_orchestrator(self: object, stop_event: asyncio.Event) -> None:
        nonlocal calls
        del self
        calls += 1
        started.set()
        await stop_event.wait()

    monkeypatch.setattr(AutonomousOrchestrator, "run", fake_orchestrator)
    service = BootstrapService(
        settings,
        heartbeat_interval_seconds=0.01,
        run_shadow=False,
        supervisor_poll_interval_seconds=0.01,
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run(stop_event))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        await wait_for_health(state_path, 1)
        assert calls == 1
    finally:
        stop_event.set()
        await asyncio.wait_for(task, timeout=2)

    assert calls == 1


@pytest.mark.asyncio
async def test_bounded_service_runs_full_interval_and_stops_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")})
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def fake_run(self: object, stop_event: asyncio.Event) -> None:
        del self
        started.set()
        await stop_event.wait()
        stopped.set()

    monkeypatch.setattr(BootstrapService, "run", fake_run)

    await run_for_duration(settings, 0.02)

    assert started.is_set()
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_bounded_service_rejects_invalid_duration(tmp_path: Path) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")})

    with pytest.raises(ValueError, match="service duration"):
        await run_for_duration(settings, 0)
