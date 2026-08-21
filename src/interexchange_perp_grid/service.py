from __future__ import annotations

import asyncio
import contextlib
import signal
from dataclasses import dataclass
from pathlib import Path

from interexchange_perp_grid.autonomous_orchestrator import AutonomousOrchestrator
from interexchange_perp_grid.canary_runtime import (
    OnDemandLiveControlPlane,
    recover_active_actions,
)
from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.live_journal import LiveJournalAction, LiveOrderJournal
from interexchange_perp_grid.observability import (
    SERVICE_HEARTBEATS,
    SERVICE_STARTS,
    SERVICE_UP,
    get_logger,
)
from interexchange_perp_grid.priority_scheduler import PriorityWorkScheduler
from interexchange_perp_grid.shadow import ContinuousShadowEvaluator, ShadowRuntime
from interexchange_perp_grid.state import (
    initialise_state,
    record_service_heartbeat,
    record_service_started,
    record_service_stopped,
)
from interexchange_perp_grid.supervisor import LiveSafetySupervisor, RecoveryRunner
from interexchange_perp_grid.telegram_control import run_telegram_bot


@dataclass(slots=True)
class BootstrapService:
    settings: Settings
    heartbeat_interval_seconds: float | None = None
    run_shadow: bool = True
    recovery_runner: RecoveryRunner | None = None
    supervisor_poll_interval_seconds: float = 1.0

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
        background_tasks: list[asyncio.Task[None]] = []
        priority_scheduler = PriorityWorkScheduler(
            pending_limit=self.settings.shadow.overload_pending_limit,
            worker_count=6,
            shutdown_timeout_seconds=5.0,
        )
        await priority_scheduler.start()
        journal = LiveOrderJournal(self.state_path)
        await journal.initialise()
        recovery_runner = self.recovery_runner
        if recovery_runner is None:
            recovery_dispatch_lock = asyncio.Lock()

            async def default_recovery_runner(action: LiveJournalAction) -> object:
                async with recovery_dispatch_lock:
                    current = await journal.active_actions()
                    if not current:
                        return object()
                    if action.pair_action_id not in {item.pair_action_id for item in current}:
                        return object()
                    return await recover_active_actions(self.settings, journal, current)

            recovery_runner = default_recovery_runner

        supervisor = LiveSafetySupervisor(
            journal,
            recovery_runner,
            poll_interval_seconds=self.supervisor_poll_interval_seconds,
            priority_scheduler=priority_scheduler,
        )
        background_tasks.append(
            asyncio.create_task(supervisor.run(stop_event), name="live-safety-supervisor")
        )
        if self.settings.app.mode == "shadow":
            background_tasks.append(
                asyncio.create_task(
                    AutonomousOrchestrator(self.settings).run(stop_event),
                    name="autonomous-orchestrator",
                )
            )
        if self.run_shadow and self.settings.app.mode == "shadow":
            runtime = ShadowRuntime(self.settings)
            await runtime.start()
            background_tasks.append(
                asyncio.create_task(
                    ContinuousShadowEvaluator(
                        self.settings,
                        runtime=runtime,
                        critical_work_count=priority_scheduler.critical_work_count,
                    ).run(stop_event),
                    name="continuous-shadow-evaluator",
                )
            )
            if self.settings.telegram.enabled:
                background_tasks.append(
                    asyncio.create_task(
                        run_telegram_bot(
                            self.settings,
                            runtime,
                            stop_event,
                            OnDemandLiveControlPlane(self.settings),
                        ),
                        name="telegram-control",
                    )
                )
        try:
            while not stop_event.is_set():
                for task in background_tasks:
                    if task.done():
                        await task
                await record_service_heartbeat(self.state_path)
                SERVICE_HEARTBEATS.inc()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                except TimeoutError:
                    continue
        finally:
            stop_event.set()
            for task in background_tasks:
                if not task.done():
                    task.cancel()
            try:
                for task in background_tasks:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            finally:
                await priority_scheduler.close()
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
