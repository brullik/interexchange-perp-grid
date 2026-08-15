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
    return SupervisorHealth(
        mode=SupervisorMode(str(row["mode"])),
        active_pair_action_id=(
            str(row["active_pair_action_id"]) if row["active_pair_action_id"] is not None else None
        ),
        action_state=LiveActionState(state) if state is not None else None,
        outcome=str(row["outcome"]),
        recovery_required=bool(row["recovery_required"]),
        failure=str(row["failure"]) if row["failure"] is not None else None,
        heartbeat_at=datetime.fromisoformat(str(row["heartbeat_at"])),
    )


class LiveSafetySupervisor:
    """Single long-running owner of every durable live action and restart recovery."""

    def __init__(
        self,
        journal: LiveOrderJournal,
        recovery_runner: RecoveryRunner,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("supervisor poll interval must be positive")
        self._journal = journal
        self._recovery_runner = recovery_runner
        self._poll_interval_seconds = poll_interval_seconds
        self._recovery_lock = asyncio.Lock()
        self._initialise_lock = asyncio.Lock()
        self._initialised = False

    async def initialise(self) -> None:
        async with self._initialise_lock:
            if self._initialised:
                return
            await self._journal.initialise()
            await asyncio.to_thread(self._initialise_status_sync)
            self._initialised = True

    async def reconcile_once(self) -> SupervisorHealth:
        """Recover one active action without qualification or owner-entry gates."""
        async with self._recovery_lock:
            await self.initialise()
            active = await self._journal.active()
            if active is None:
                return await self._record(
                    SupervisorMode.IDLE,
                    None,
                    None,
                    "FLAT_NO_ACTIVE_ACTION",
                    False,
                    None,
                )
            await self._record(
                SupervisorMode.RECOVERY_ONLY,
                active.pair_action_id,
                active.state,
                "RECOVERY_STARTED",
                True,
                None,
            )
            try:
                await self._recovery_runner(active)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                current = await self._journal.active()
                return await self._record(
                    SupervisorMode.BLOCKED,
                    current.pair_action_id if current is not None else active.pair_action_id,
                    current.state if current is not None else active.state,
                    "RECOVERY_FAILED_CLOSED",
                    True,
                    type(error).__name__,
                )
            current = await self._journal.active()
            if current is None:
                return await self._record(
                    SupervisorMode.IDLE,
                    None,
                    LiveActionState.FLAT,
                    "RECOVERY_EXCHANGE_VERIFIED_FLAT",
                    False,
                    None,
                )
            return await self._record(
                SupervisorMode.BLOCKED,
                current.pair_action_id,
                current.state,
                "RECOVERY_REMAINS_ACTIVE",
                True,
                None,
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
            active = await self._journal.active()
            await self._record(
                SupervisorMode.STOPPED,
                active.pair_action_id if active is not None else None,
                active.state if active is not None else LiveActionState.FLAT,
                "SUPERVISOR_STOPPED",
                active is not None,
                None,
            )

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

    async def _record(
        self,
        mode: SupervisorMode,
        pair_action_id: str | None,
        action_state: LiveActionState | None,
        outcome: str,
        recovery_required: bool,
        failure: str | None,
    ) -> SupervisorHealth:
        health = SupervisorHealth(
            mode,
            pair_action_id,
            action_state,
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
                SET mode = ?, active_pair_action_id = ?, action_state = ?, outcome = ?,
                    recovery_required = ?, failure = ?, heartbeat_at = ?
                WHERE singleton = 1
                """,
                (
                    health.mode.value,
                    health.active_pair_action_id,
                    health.action_state.value if health.action_state is not None else None,
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
