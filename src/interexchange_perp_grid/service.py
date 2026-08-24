from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
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
from interexchange_perp_grid.qualification import QualificationPolicy
from interexchange_perp_grid.shadow import ContinuousShadowEvaluator, ShadowRuntime
from interexchange_perp_grid.state import (
    initialise_state,
    read_service_health,
    record_service_heartbeat,
    record_service_started,
    record_service_stopped,
)
from interexchange_perp_grid.supervisor import LiveSafetySupervisor, RecoveryRunner
from interexchange_perp_grid.telegram_control import run_telegram_bot

_ES_CONTINUOUS = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001
_ES_AWAYMODE_REQUIRED = 0x00000040


@dataclass(frozen=True, slots=True)
class BoundedServiceReceipt:
    schema_version: int
    status: str
    started_at: datetime
    ended_at: datetime
    requested_seconds: float
    observed_monotonic_seconds: float
    state_path: str
    service_starts: int

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or self.status != "PASS"
            or self.started_at.tzinfo is None
            or self.ended_at.tzinfo is None
            or self.ended_at < self.started_at
            or self.requested_seconds <= 0
            or self.observed_monotonic_seconds < self.requested_seconds
            or self.service_starts < 1
            or not self.state_path
        ):
            raise ValueError("bounded service receipt is invalid")


def write_bounded_service_receipt(path: Path, receipt: BoundedServiceReceipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(receipt), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_bounded_service_receipt(path: Path) -> BoundedServiceReceipt:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("bounded service receipt must be an object")
    return BoundedServiceReceipt(
        schema_version=int(payload["schema_version"]),
        status=str(payload["status"]),
        started_at=datetime.fromisoformat(str(payload["started_at"])),
        ended_at=datetime.fromisoformat(str(payload["ended_at"])),
        requested_seconds=float(payload["requested_seconds"]),
        observed_monotonic_seconds=float(payload["observed_monotonic_seconds"]),
        state_path=str(payload["state_path"]),
        service_starts=int(payload["service_starts"]),
    )


@contextmanager
def _prevent_windows_sleep() -> Iterator[None]:
    if sys.platform != "win32":
        yield
        return
    import ctypes

    kernel32 = ctypes.windll.kernel32
    previous = kernel32.SetThreadExecutionState(
        _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_AWAYMODE_REQUIRED
    )
    if previous == 0:
        raise RuntimeError("Windows sleep prevention could not be armed")
    try:
        yield
    finally:
        kernel32.SetThreadExecutionState(_ES_CONTINUOUS)


@dataclass(slots=True)
class BootstrapService:
    settings: Settings
    heartbeat_interval_seconds: float | None = None
    run_shadow: bool = True
    recovery_runner: RecoveryRunner | None = None
    supervisor_poll_interval_seconds: float = 1.0
    qualification_policy: QualificationPolicy | None = None

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
                    AutonomousOrchestrator(
                        self.settings,
                        qualification_policy=self.qualification_policy,
                    ).run(stop_event),
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


async def run_for_duration(settings: Settings, duration_seconds: float) -> BoundedServiceReceipt:
    if not 0 < duration_seconds <= 86_400:
        raise ValueError("service duration must be in (0, 86400]")
    with _prevent_windows_sleep():
        started_at = datetime.now(UTC)
        started_monotonic = time.monotonic()
        deadline_monotonic = started_monotonic + duration_seconds
        stop_event = asyncio.Event()
        service = asyncio.create_task(
            BootstrapService(settings).run(stop_event),
            name="bounded-bootstrap-service",
        )
        try:
            try:
                await asyncio.wait_for(asyncio.shield(service), timeout=duration_seconds)
            except TimeoutError:
                remaining = deadline_monotonic - time.monotonic()
                if remaining > 0:
                    await asyncio.sleep(remaining)
                stop_event.set()
                await service
                ended_at = datetime.now(UTC)
                observed_seconds = time.monotonic() - started_monotonic
                health = await read_service_health(
                    Path(settings.storage.sqlite_path),
                    settings.app.health_max_age_seconds,
                    ended_at,
                )
                if health.status != "stopped":
                    raise RuntimeError("bounded service did not persist a stopped state") from None
                return BoundedServiceReceipt(
                    schema_version=1,
                    status="PASS",
                    started_at=started_at,
                    ended_at=ended_at,
                    requested_seconds=duration_seconds,
                    observed_monotonic_seconds=observed_seconds,
                    state_path=str(Path(settings.storage.sqlite_path).resolve()),
                    service_starts=health.starts,
                )
            raise RuntimeError("service stopped before the requested duration")
        finally:
            stop_event.set()
            if not service.done():
                service.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await service
