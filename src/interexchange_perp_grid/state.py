from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from interexchange_perp_grid.reason_codes import ReasonCode

SCHEMA_VERSION = "1"
SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS service_runtime (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        status TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        starts INTEGER NOT NULL CHECK (starts >= 0)
    )
    """,
)


@dataclass(frozen=True, slots=True)
class ServiceHealth:
    healthy: bool
    reason: ReasonCode
    status: str | None
    heartbeat_at: datetime | None
    starts: int


def _connect(path: Path) -> sqlite3.Connection:
    database = sqlite3.connect(path, timeout=30)
    database.execute("PRAGMA busy_timeout=30000")
    database.execute("PRAGMA foreign_keys=ON")
    return database


def _initialise_state_sync(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(path) as database:
        journal_mode = database.execute("PRAGMA journal_mode=WAL").fetchone()
        if journal_mode is None or str(journal_mode[0]).lower() != "wal":
            raise RuntimeError("SQLite WAL mode is required")
        database.execute("PRAGMA synchronous=FULL")
        database.execute("BEGIN IMMEDIATE")
        for statement in SCHEMA_STATEMENTS:
            database.execute(statement)
        existing = database.execute(
            "SELECT value FROM metadata WHERE key = ?", ("schema_version",)
        ).fetchone()
        if existing is not None and existing[0] != SCHEMA_VERSION:
            raise RuntimeError(f"unsupported state schema version: {existing[0]}")
        database.execute(
            "INSERT OR IGNORE INTO metadata(key, value) VALUES (?, ?)",
            ("schema_version", SCHEMA_VERSION),
        )
        database.commit()


async def initialise_state(path: Path) -> None:
    await asyncio.to_thread(_initialise_state_sync, path)


def _record_service_started_sync(path: Path, now: datetime) -> None:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            """
            INSERT INTO service_runtime(singleton, status, heartbeat_at, starts)
            VALUES (1, 'running', ?, 1)
            ON CONFLICT(singleton) DO UPDATE SET
                status = 'running',
                heartbeat_at = excluded.heartbeat_at,
                starts = service_runtime.starts + 1
            """,
            (now.isoformat(),),
        )
        database.commit()


async def record_service_started(path: Path, now: datetime | None = None) -> None:
    await asyncio.to_thread(_record_service_started_sync, path, now or datetime.now(UTC))


def _record_service_status_sync(path: Path, status: str, now: datetime) -> None:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        cursor = database.execute(
            "UPDATE service_runtime SET status = ?, heartbeat_at = ? WHERE singleton = 1",
            (status, now.isoformat()),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("service runtime state is not initialised")
        database.commit()


async def record_service_heartbeat(path: Path, now: datetime | None = None) -> None:
    await asyncio.to_thread(
        _record_service_status_sync,
        path,
        "running",
        now or datetime.now(UTC),
    )


async def record_service_stopped(path: Path, now: datetime | None = None) -> None:
    await asyncio.to_thread(
        _record_service_status_sync,
        path,
        "stopped",
        now or datetime.now(UTC),
    )


def _read_service_health_sync(path: Path, max_age_seconds: int, now: datetime) -> ServiceHealth:
    if not path.is_file():
        return ServiceHealth(False, ReasonCode.SERVICE_STATE_MISSING, None, None, 0)
    with _connect(path) as database:
        row = database.execute(
            "SELECT status, heartbeat_at, starts FROM service_runtime WHERE singleton = 1"
        ).fetchone()
    if row is None:
        return ServiceHealth(False, ReasonCode.SERVICE_STATE_MISSING, None, None, 0)
    status, heartbeat_value, starts = str(row[0]), str(row[1]), int(row[2])
    heartbeat_at = datetime.fromisoformat(heartbeat_value)
    if status != "running":
        return ServiceHealth(False, ReasonCode.SERVICE_NOT_RUNNING, status, heartbeat_at, starts)
    if (now - heartbeat_at).total_seconds() > max_age_seconds:
        return ServiceHealth(
            False, ReasonCode.SERVICE_HEARTBEAT_STALE, status, heartbeat_at, starts
        )
    return ServiceHealth(True, ReasonCode.SERVICE_HEALTHY, status, heartbeat_at, starts)


async def read_service_health(
    path: Path,
    max_age_seconds: int,
    now: datetime | None = None,
) -> ServiceHealth:
    return await asyncio.to_thread(
        _read_service_health_sync,
        path,
        max_age_seconds,
        now or datetime.now(UTC),
    )
