from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.priority_scheduler import PriorityWorkScheduler, WorkPriority


class SupervisorMode(StrEnum):
    IDLE = "IDLE"
    RECOVERY_ONLY = "RECOVERY_ONLY"
    BLOCKED = "BLOCKED"
    STOPPED = "STOPPED"


@dataclass(frozen=True, slots=True)
class SupervisorHealth:
    mode: SupervisorMode
    active_pair_action_id: str | None
    action_state: LiveActionState | None
    active_action_count: int
    outcome: str
    recovery_required: bool
    failure: str | None
    heartbeat_at: datetime


RecoveryRunner = Callable[[LiveJournalAction], Awaitable[object]]


async def read_supervisor_health(path: Path) -> SupervisorHealth | None:
    return await asyncio.to_thread(_read_supervisor_health_sync, path)


def _read_supervisor_health_sync(path: Path) -> SupervisorHealth | None:
    if not path.is_file():
        return None
    with sqlite3.connect(path, timeout=30) as database:
        database.row_factory = sqlite3.Row
        table = database.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'live_safety_supervisor'"
        ).fetchone()
        if table is None:
            return None
        row = database.execute(
            "SELECT * FROM live_safety_supervisor WHERE singleton = 1"
        ).fetchone()
    return _health_from_row(row) if row is not None else None


def _health_from_row(row: sqlite3.Row) -> SupervisorHealth:
    state = str(row["action_state"]) if row["action_state"] is not None else None
    columns = tuple(row.keys())
    return SupervisorHealth(
        mode=SupervisorMode(str(row["mode"])),
        active_pair_action_id=(
            str(row["active_pair_action_id"]) if row["active_pair_action_id"] is not None else None
        ),
        action_state=LiveActionState(state) if state is not None else None,
        active_action_count=(
            int(row["active_action_count"])
            if "active_action_count" in columns
            else int(row["active_pair_action_id"] is not None)
        ),
        outcome=str(row["outcome"]),
        recovery_required=bool(row["recovery_required"]),
        failure=str(row["failure"]) if row["failure"] is not None else None,
        heartbeat_at=datetime.fromisoformat(str(row["heartbeat_at"])),
    )


class LiveSafetySupervisor:
    """Single long-running owner of all durable live actions and restart recovery."""

    def __init__(
        self,
        journal: LiveOrderJournal,
        recovery_runner: RecoveryRunner,
        *,
        poll_interval_seconds: float = 1.0,
        recovery_timeout_seconds: float = 5.0,
        priority_scheduler: PriorityWorkScheduler | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("supervisor poll interval must be positive")
        if recovery_timeout_seconds <= 0:
            raise ValueError("supervisor recovery timeout must be positive")
        self._journal = journal
        self._recovery_runner = recovery_runner
        self._poll_interval_seconds = poll_interval_seconds
        self._recovery_timeout_seconds = recovery_timeout_seconds
        self._priority_scheduler = priority_scheduler
        self._recovery_lock = asyncio.Lock()
        self._initialise_lock = asyncio.Lock()
        self._recovery_tasks: dict[str, asyncio.Task[object]] = {}
        self._recovery_work_keys: dict[str, str] = {}
        self._initialised = False

    async def initialise(self) -> None:
        async with self._initialise_lock:
            if self._initialised:
                return
            await self._journal.initialise()
            await asyncio.to_thread(self._initialise_status_sync)
            self._initialised = True

    async def reconcile_once(self) -> SupervisorHealth:
        """Recover every active action without qualification or owner-entry gates."""
        async with self._recovery_lock:
            await self.initialise()
            active = await self._journal.active_actions()
            active_ids = {action.pair_action_id for action in active}
            for pair_action_id, task in tuple(self._recovery_tasks.items()):
                if task.done() and pair_action_id not in active_ids:
                    self._consume_recovery_task(task)
                    self._recovery_tasks.pop(pair_action_id, None)
                    self._recovery_work_keys.pop(pair_action_id, None)
            if not active:
                if self._recovery_tasks:
                    return await self._record(
                        SupervisorMode.BLOCKED,
                        None,
                        LiveActionState.FLAT,
                        0,
                        "RECOVERY_TASKS_REMAIN_ACTIVE",
                        True,
                        "TimeoutError",
                    )
                return await self._record(
                    SupervisorMode.IDLE,
                    None,
                    None,
                    0,
                    "FLAT_NO_ACTIVE_ACTION",
                    False,
                    None,
                )
            representative = active[0]
            await self._record(
                SupervisorMode.RECOVERY_ONLY,
                representative.pair_action_id,
                representative.state if len(active) == 1 else None,
                len(active),
                f"RECOVERY_STARTED:{len(active)}",
                True,
                None,
            )
            tasks: dict[str, asyncio.Task[object]] = {}
            account_wide_recovery = len(active) > 1
            portfolio_recovery = _is_compatible_aggressive_portfolio(active)
            if portfolio_recovery:
                active_portfolio_tasks = {
                    task
                    for pair_action_id, task in self._recovery_tasks.items()
                    if pair_action_id in active_ids and not task.done()
                }
                portfolio_task_covers_snapshot = len(active_portfolio_tasks) == 1 and all(
                    self._recovery_tasks.get(pair_action_id) in active_portfolio_tasks
                    for pair_action_id in active_ids
                )
                if active_portfolio_tasks and not portfolio_task_covers_snapshot:
                    # A single-tranche monitor cannot absorb a newly durable tranche.
                    # Stop it cleanly, then rebuild one owner from the complete journal
                    # snapshot so every PREPARED action is submitted and route-wide
                    # safety monitoring is installed.
                    handoff_keys = {
                        self._recovery_work_keys[pair_action_id]
                        for pair_action_id in active_ids
                        if self._recovery_tasks.get(pair_action_id) in active_portfolio_tasks
                        and pair_action_id in self._recovery_work_keys
                    }
                    if self._priority_scheduler is not None:
                        stopped = tuple(
                            await asyncio.gather(
                                *(
                                    self._priority_scheduler.cancel_and_wait(
                                        key,
                                        timeout_seconds=self._recovery_timeout_seconds,
                                    )
                                    for key in handoff_keys
                                )
                            )
                        )
                        if not all(stopped):
                            return await self._record(
                                SupervisorMode.BLOCKED,
                                representative.pair_action_id,
                                None,
                                len(active),
                                "PORTFOLIO_HANDOFF_NOT_QUIESCENT",
                                True,
                                "RecoveryHandoffTimeout",
                            )
                    for task in active_portfolio_tasks:
                        task.cancel()
                    _done, pending_handoff = await asyncio.wait(
                        active_portfolio_tasks,
                        timeout=self._recovery_timeout_seconds,
                    )
                    if pending_handoff:
                        return await self._record(
                            SupervisorMode.BLOCKED,
                            representative.pair_action_id,
                            None,
                            len(active),
                            "PORTFOLIO_HANDOFF_NOT_QUIESCENT",
                            True,
                            "RecoveryHandoffTimeout",
                        )
                    for pair_action_id, task in tuple(self._recovery_tasks.items()):
                        if task in active_portfolio_tasks:
                            self._consume_recovery_task(task)
                            self._recovery_tasks.pop(pair_action_id, None)
                            self._recovery_work_keys.pop(pair_action_id, None)
            shared_portfolio_task = (
                next(
                    (
                        task
                        for pair_action_id, task in self._recovery_tasks.items()
                        if pair_action_id in active_ids and not task.done()
                    ),
                    None,
                )
                if portfolio_recovery
                else None
            )
            for action in active:
                recovery_task = self._recovery_tasks.get(action.pair_action_id)
                if recovery_task is None:
                    recovery_task = shared_portfolio_task
                    work_key = "live-recovery:portfolio"
                    if recovery_task is None:
                        work_key = (
                            "live-recovery:portfolio"
                            if portfolio_recovery
                            else "live-recovery:account"
                            if account_wide_recovery
                            else f"live-recovery:{action.pair_action_id}"
                        )
                        recovery_task = asyncio.create_task(
                            self._run_recovery(
                                action,
                                (
                                    WorkPriority.EMERGENCY_FLATTEN
                                    if account_wide_recovery
                                    else _recovery_priority(action)
                                ),
                                work_key,
                            ),
                            name=(
                                "live-recovery-portfolio"
                                if portfolio_recovery
                                else f"live-recovery-{action.pair_action_id}"
                            ),
                        )
                        if portfolio_recovery:
                            shared_portfolio_task = recovery_task
                    self._recovery_tasks[action.pair_action_id] = recovery_task
                    self._recovery_work_keys[action.pair_action_id] = work_key
                tasks[action.pair_action_id] = recovery_task
            _done, pending = await asyncio.wait(
                tasks.values(),
                timeout=self._recovery_timeout_seconds,
            )
            failures_list: list[str] = []
            for pair_action_id, task in tasks.items():
                if task in pending:
                    failures_list.append(f"{pair_action_id}:TimeoutError")
                    continue
                try:
                    task.result()
                except asyncio.CancelledError:
                    failures_list.append(f"{pair_action_id}:CancelledError")
                except Exception as error:
                    failures_list.append(f"{pair_action_id}:{type(error).__name__}")
                finally:
                    if self._recovery_tasks.get(pair_action_id) is task:
                        self._recovery_tasks.pop(pair_action_id, None)
                        self._recovery_work_keys.pop(pair_action_id, None)
            failures = tuple(failures_list)
            current = await self._journal.active_actions()
            if not current:
                return await self._record(
                    SupervisorMode.IDLE,
                    None,
                    LiveActionState.FLAT,
                    0,
                    "RECOVERY_EXCHANGE_VERIFIED_FLAT",
                    False,
                    None,
                )
            representative = current[0]
            return await self._record(
                SupervisorMode.BLOCKED,
                representative.pair_action_id,
                representative.state if len(current) == 1 else None,
                len(current),
                (
                    f"RECOVERY_FAILED_CLOSED:{len(current)}"
                    if failures
                    else f"RECOVERY_REMAINS_ACTIVE:{len(current)}"
                ),
                True,
                "|".join(failures) if failures else None,
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.initialise()
        try:
            while not stop_event.is_set():
                await self.reconcile_once()
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._poll_interval_seconds,
                    )
                except TimeoutError:
                    continue
        finally:
            recovery_tasks = tuple(self._recovery_tasks.values())
            for task in recovery_tasks:
                task.cancel()
            if recovery_tasks:
                done, pending = await asyncio.wait(
                    recovery_tasks,
                    timeout=self._recovery_timeout_seconds,
                )
                for task in done:
                    self._consume_recovery_task(task)
            else:
                pending = set()
            active = await self._journal.active_actions()
            representative = active[0] if active else None
            await self._record(
                SupervisorMode.STOPPED,
                representative.pair_action_id if representative is not None else None,
                (
                    representative.state
                    if representative is not None and len(active) == 1
                    else LiveActionState.FLAT
                    if not active
                    else None
                ),
                len(active),
                "SUPERVISOR_STOPPED",
                bool(active),
                "RecoveryShutdownTimeout" if pending else None,
            )
            if pending:
                raise RuntimeError("supervisor shutdown failed: recovery tasks remain active")

    async def health(self) -> SupervisorHealth:
        await self.initialise()
        return await asyncio.to_thread(self._read_status_sync)

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self._journal.path, timeout=30)
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA busy_timeout=30000")
        return database

    def _initialise_status_sync(self) -> None:
        with self._connect() as database:
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS live_safety_supervisor (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    mode TEXT NOT NULL,
                    active_pair_action_id TEXT,
                    action_state TEXT,
                    active_action_count INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL,
                    recovery_required INTEGER NOT NULL CHECK (recovery_required IN (0, 1)),
                    failure TEXT,
                    heartbeat_at TEXT NOT NULL
                )
                """
            )
            now = datetime.now(UTC).isoformat()
            database.execute(
                """
                INSERT OR IGNORE INTO live_safety_supervisor (
                    singleton, mode, active_pair_action_id, action_state, outcome,
                    recovery_required, failure, heartbeat_at
                ) VALUES (1, ?, NULL, NULL, 'INITIALISED', 0, NULL, ?)
                """,
                (SupervisorMode.IDLE.value, now),
            )
            columns = {
                str(row[1]) for row in database.execute("PRAGMA table_info(live_safety_supervisor)")
            }
            if "active_action_count" not in columns:
                database.execute(
                    "ALTER TABLE live_safety_supervisor "
                    "ADD COLUMN active_action_count INTEGER NOT NULL DEFAULT 0"
                )

    async def _record(
        self,
        mode: SupervisorMode,
        pair_action_id: str | None,
        action_state: LiveActionState | None,
        active_action_count: int,
        outcome: str,
        recovery_required: bool,
        failure: str | None,
    ) -> SupervisorHealth:
        health = SupervisorHealth(
            mode,
            pair_action_id,
            action_state,
            active_action_count,
            outcome,
            recovery_required,
            failure,
            datetime.now(UTC),
        )
        await asyncio.to_thread(self._write_status_sync, health)
        return health

    def _write_status_sync(self, health: SupervisorHealth) -> None:
        with self._connect() as database:
            database.execute(
                """
                UPDATE live_safety_supervisor
                SET mode = ?, active_pair_action_id = ?, action_state = ?,
                    active_action_count = ?, outcome = ?,
                    recovery_required = ?, failure = ?, heartbeat_at = ?
                WHERE singleton = 1
                """,
                (
                    health.mode.value,
                    health.active_pair_action_id,
                    health.action_state.value if health.action_state is not None else None,
                    health.active_action_count,
                    health.outcome,
                    int(health.recovery_required),
                    health.failure,
                    health.heartbeat_at.isoformat(),
                ),
            )

    def _read_status_sync(self) -> SupervisorHealth:
        with self._connect() as database:
            row = database.execute(
                "SELECT * FROM live_safety_supervisor WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("supervisor health is not initialised")
        return _health_from_row(row)

    async def _run_recovery(
        self,
        action: LiveJournalAction,
        priority: WorkPriority,
        work_key: str,
    ) -> object:
        if self._priority_scheduler is None:
            return await self._recovery_runner(action)
        return await self._priority_scheduler.run(
            priority,
            work_key,
            lambda: self._recovery_runner(action),
        )

    @staticmethod
    def _consume_recovery_task(task: asyncio.Task[object]) -> None:
        if task.cancelled():
            return
        try:
            task.exception()
        except asyncio.CancelledError:
            return


def _is_compatible_aggressive_portfolio(
    actions: tuple[LiveJournalAction, ...],
) -> bool:
    if not 2 <= len(actions) <= 5:
        return False
    first = actions[0]
    identity = (
        first.route,
        first.qualification_hash,
        first.risk_reservation.get("aggressive_binding_sha256"),
        first.risk_reservation.get("strategy_profile_sha256"),
    )
    levels: set[int] = set()
    for action in actions:
        reservation = action.risk_reservation
        try:
            level = int(str(reservation["level_index"]))
        except (KeyError, ValueError):
            return False
        if (
            reservation.get("strategy") != "AGGRESSIVE_SYMBIOSIS_V1"
            or reservation.get("stage") != "pilot_a"
            or (
                action.route,
                action.qualification_hash,
                reservation.get("aggressive_binding_sha256"),
                reservation.get("strategy_profile_sha256"),
            )
            != identity
            or not 1 <= level <= 5
            or level in levels
        ):
            return False
        levels.add(level)
    return True


def _recovery_priority(action: LiveJournalAction) -> WorkPriority:
    emergency_actions = {
        "EMERGENCY_FLATTEN",
        "KILL_CANCEL_FLATTEN",
        "CLOSE_ALL_LIVE",
    }
    risk_action = action.risk_reservation.get("action")
    if action.recovery_action in emergency_actions or (
        isinstance(risk_action, str) and risk_action in emergency_actions
    ):
        return WorkPriority.EMERGENCY_FLATTEN
    if (
        action.state == LiveActionState.PREPARED
        and action.risk_reservation.get("supervisor_intent") == "LIVE_CANARY"
        and action.risk_reservation.get("supervisor_queued") is True
    ):
        return WorkPriority.NEW_ENTRY
    if action.state in {
        LiveActionState.SUBMITTING,
        LiveActionState.ACKNOWLEDGED,
        LiveActionState.PARTIAL,
        LiveActionState.FILLED,
        LiveActionState.REJECTED,
        LiveActionState.UNKNOWN,
        LiveActionState.RECOVERING,
    }:
        return WorkPriority.UNMATCHED_HEDGE
    if action.state in {LiveActionState.HEDGED, LiveActionState.CLOSING}:
        return WorkPriority.NORMAL_CLOSE
    return WorkPriority.PRIVATE_RECONCILE
