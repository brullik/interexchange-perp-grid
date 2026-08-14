from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass
from pathlib import Path

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.observability import (
    SERVICE_HEARTBEATS,
    SERVICE_STARTS,
    SERVICE_UP,
    get_logger,
)
from interexchange_perp_grid.state import (
    initialise_state,
    record_service_heartbeat,
    record_service_started,
    record_service_stopped,
)


@dataclass(slots=True)
class BootstrapService:
    settings: Settings
    heartbeat_interval_seconds: float | None = None

    @property
    def state_path(self) -> Path:
        return Path(self.settings.storage.sqlite_path)

    async def run(self, stop_event: asyncio.Event) -> None:
        logger = get_logger()
        interval = self.heartbeat_interval_seconds or self.settings.app.heartbeat_interval_seconds
        await initialise_state(self.state_path)
        await record_service_started(self.state_path)
        SERVICE_STARTS.inc()
        SERVICE_UP.set(1)
        logger.info(
            "service_started",
            mode=self.settings.app.mode,
            state_path=str(self.state_path),
        )
        try:
            while not stop_event.is_set():
                await record_service_heartbeat(self.state_path)
                SERVICE_HEARTBEATS.inc()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                except TimeoutError:
                    continue
        finally:
            await record_service_stopped(self.state_path)
            SERVICE_UP.set(0)
            logger.info("service_stopped")


async def run_until_signal(settings: Settings) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for handled_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(handled_signal, stop_event.set)
            installed_signals.append(handled_signal)
        except (NotImplementedError, RuntimeError):
            continue
    try:
        await BootstrapService(settings).run(stop_event)
    finally:
        for handled_signal in installed_signals:
            loop.remove_signal_handler(handled_signal)
