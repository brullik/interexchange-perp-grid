from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from interexchange_perp_grid.domain import FundingSnapshot, Venue
from interexchange_perp_grid.execution import (
    Fill,
    OrderPurpose,
    PairActionState,
    Side,
    Tranche,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey, SignalDecision

SCHEMA_VERSION = "11"
STATE_TRANSITION_TERMINAL_TIMEOUT_SECONDS = 1.0
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
    """
    CREATE TABLE IF NOT EXISTS runtime_controls (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
        killed INTEGER NOT NULL CHECK (killed IN (0, 1)),
        reconciliation_state TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS simulated_tranches (
        tranche_id TEXT PRIMARY KEY,
        lifecycle_state TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS command_audit (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor TEXT NOT NULL,
        command TEXT NOT NULL,
        outcome TEXT NOT NULL,
        reason TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_snapshot (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        payload_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS live_confirmation (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        confirmed_until TEXT NOT NULL,
        confirmed_by TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_epochs (
        epoch_id TEXT PRIMARY KEY,
        route TEXT NOT NULL,
        release_sha TEXT NOT NULL,
        source_sha256 TEXT NOT NULL,
        config_sha256 TEXT NOT NULL,
        container_image_digest TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        status TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_funding_observations (
        epoch_id TEXT NOT NULL REFERENCES qualification_epochs(epoch_id),
        base TEXT NOT NULL,
        venue TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        rate TEXT NOT NULL,
        next_funding_timestamp_ms INTEGER NOT NULL,
        interval TEXT NOT NULL,
        PRIMARY KEY (epoch_id, base, venue, next_funding_timestamp_ms, observed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_signal_observations (
        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        epoch_id TEXT NOT NULL REFERENCES qualification_epochs(epoch_id),
        route TEXT NOT NULL,
        accepted INTEGER NOT NULL CHECK (accepted IN (0, 1)),
        reason TEXT NOT NULL,
        expected_net_pnl_usdt TEXT NOT NULL,
        observed_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_strategy_parameters (
        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        epoch_id TEXT NOT NULL REFERENCES qualification_epochs(epoch_id),
        route TEXT NOT NULL,
        size_bucket_base_quantity TEXT NOT NULL,
        calibration_version INTEGER NOT NULL,
        adaptive_entry_threshold_bps TEXT NOT NULL,
        target_exit_spread_bps TEXT NOT NULL,
        minimum_profit_usdt TEXT NOT NULL,
        stressed_cost_multiplier TEXT NOT NULL,
        expected_holding_seconds INTEGER NOT NULL,
        maximum_holding_seconds INTEGER NOT NULL,
        observed_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_pnl_observations (
        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        epoch_id TEXT NOT NULL REFERENCES qualification_epochs(epoch_id),
        route TEXT NOT NULL,
        simulated_net_pnl_usdt TEXT NOT NULL,
        observed_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_runtime_errors (
        error_id INTEGER PRIMARY KEY AUTOINCREMENT,
        epoch_id TEXT NOT NULL REFERENCES qualification_epochs(epoch_id),
        error_type TEXT NOT NULL,
        observed_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS private_event_watermarks (
        venue TEXT PRIMARY KEY,
        event_watermark INTEGER NOT NULL CHECK (event_watermark >= 0),
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS route_calibration_observations (
        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        route TEXT NOT NULL,
        size_bucket_multiplier TEXT NOT NULL,
        epoch_id TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        reason TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        UNIQUE(route, size_bucket_multiplier, epoch_id, observed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS route_calibration_parameters (
        route TEXT NOT NULL,
        size_bucket_multiplier TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
        transient_blocked INTEGER NOT NULL DEFAULT 0 CHECK (transient_blocked IN (0, 1)),
        PRIMARY KEY(route, size_bucket_multiplier)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS route_calibration_episodes (
        route TEXT NOT NULL,
        size_bucket_multiplier TEXT NOT NULL,
        epoch_id TEXT NOT NULL,
        spread_bucket_index INTEGER NOT NULL CHECK (spread_bucket_index BETWEEN 0 AND 4),
        entry_spread_bps TEXT NOT NULL,
        convergence_target_bps TEXT NOT NULL,
        peak_spread_bps TEXT NOT NULL,
        started_at TEXT NOT NULL,
        PRIMARY KEY(route, size_bucket_multiplier, epoch_id, spread_bucket_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS route_calibration_segments (
        route TEXT NOT NULL,
        size_bucket_multiplier TEXT NOT NULL,
        epoch_id TEXT NOT NULL,
        ready_sample_count INTEGER NOT NULL CHECK (ready_sample_count >= 0),
        segment_started_at TEXT,
        last_observed_at TEXT NOT NULL,
        last_reason TEXT NOT NULL,
        PRIMARY KEY(route, size_bucket_multiplier, epoch_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS route_calibration_runtime (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        epoch_id TEXT NOT NULL,
        policy_fingerprint TEXT NOT NULL,
        last_observed_at TEXT
    )
    """,
)

_SCHEMA_INDEX_STATEMENTS = (
    """
    CREATE INDEX IF NOT EXISTS route_calibration_observations_key_time_v8
    ON route_calibration_observations(
        route, size_bucket_multiplier, epoch_id, observed_at
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS route_calibration_observations_key_observed_v9
    ON route_calibration_observations(
        route, size_bucket_multiplier, observed_at
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS route_calibration_observations_observed_v9
    ON route_calibration_observations(observed_at)
    """,
)

_EPOCH_OBSERVATION_TABLES = (
    "qualification_funding_observations",
    "qualification_signal_observations",
    "qualification_strategy_parameters",
    "qualification_pnl_observations",
    "qualification_runtime_errors",
)


class QualificationEpochStatus(StrEnum):
    RUNNING = "RUNNING"
    FINALIZED = "FINALIZED"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class QualificationEpoch:
    epoch_id: str
    route: DirectedRouteKey
    release_sha: str
    source_sha256: str
    config_sha256: str
    container_image_digest: str
    started_at: datetime
    ended_at: datetime | None
    status: QualificationEpochStatus


@dataclass(frozen=True, slots=True)
class ServiceHealth:
    healthy: bool
    reason: ReasonCode
    status: str | None
    heartbeat_at: datetime | None
    starts: int


@dataclass(frozen=True, slots=True)
class RuntimeControls:
    paused: bool
    killed: bool
    reconciliation_state: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class CommandAudit:
    audit_id: int
    actor: str
    command: str
    outcome: str
    reason: ReasonCode
    created_at: datetime


class StateTransitionDeadlineError(RuntimeError):
    """A daemon SQLite transition did not reach a knowable terminal state."""


class _DaemonStateWorker:
    def __init__(self, operation: Callable[[], None], *, name: str) -> None:
        self._operation = operation
        self._done = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    @property
    def done(self) -> bool:
        return self._done.is_set()

    def _run(self) -> None:
        try:
            self._operation()
        except BaseException as error:
            self._error = error
        finally:
            self._done.set()

    def result(self) -> None:
        if not self.done:
            raise RuntimeError("state transition worker is not terminal")
        if self._error is not None:
            raise self._error


async def _await_daemon_state_worker(
    worker: _DaemonStateWorker,
    *,
    deadline_monotonic: float,
) -> None:
    loop = asyncio.get_running_loop()
    while not worker.done:
        remaining = deadline_monotonic - loop.time()
        if remaining <= 0:
            raise StateTransitionDeadlineError(
                "SQLite state transition did not finish before its terminal deadline"
            )
        try:
            await asyncio.sleep(min(0.005, remaining))
        except asyncio.CancelledError:
            # The native thread cannot be cancelled.  Continue only until the
            # fixed terminal deadline, then surface an explicit indeterminate
            # transition while the daemon worker cannot block process exit.
            continue
    worker.result()


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
        if existing is not None and existing[0] not in {
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            SCHEMA_VERSION,
        }:
            raise RuntimeError(f"unsupported state schema version: {existing[0]}")
        calibration_columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(route_calibration_observations)")
        }
        if (
            existing is not None
            and existing[0] == "7"
            and "size_bucket_base_quantity" in calibration_columns
        ):
            for table in (
                "route_calibration_observations",
                "route_calibration_parameters",
                "route_calibration_episodes",
            ):
                database.execute(f"ALTER TABLE {table} RENAME TO {table}_legacy_v7")
            for statement in SCHEMA_STATEMENTS:
                database.execute(statement)
        episode_columns = {
            str(row[1]) for row in database.execute("PRAGMA table_info(route_calibration_episodes)")
        }
        if "spread_bucket_index" not in episode_columns:
            # Schema v10 allowed only one open episode per route/size.  Preserve
            # its timeout/adverse evidence across the migration when the
            # persisted entry levels identify its bucket.  If that mapping is
            # unavailable, retain the row in bucket zero but deactivate the
            # parameter so it cannot be exposed until a fresh calibration.
            database.execute(
                "ALTER TABLE route_calibration_episodes "
                "RENAME TO route_calibration_episodes_legacy_v10"
            )
            database.execute(
                """
                CREATE TABLE route_calibration_episodes (
                    route TEXT NOT NULL,
                    size_bucket_multiplier TEXT NOT NULL,
                    epoch_id TEXT NOT NULL,
                    spread_bucket_index INTEGER NOT NULL
                        CHECK (spread_bucket_index BETWEEN 0 AND 4),
                    entry_spread_bps TEXT NOT NULL,
                    convergence_target_bps TEXT NOT NULL,
                    peak_spread_bps TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    PRIMARY KEY(
                        route, size_bucket_multiplier, epoch_id, spread_bucket_index
                    )
                )
                """
            )
            legacy_episodes = database.execute(
                """
                SELECT route, size_bucket_multiplier, epoch_id, entry_spread_bps,
                       convergence_target_bps, peak_spread_bps, started_at
                FROM route_calibration_episodes_legacy_v10
                """
            ).fetchall()
            for episode in legacy_episodes:
                route_value = str(episode[0])
                size_value = str(episode[1])
                entry_spread = Decimal(str(episode[3]))
                bucket_index = 0
                mapped = False
                parameter_row = database.execute(
                    """
                    SELECT payload_json FROM route_calibration_parameters
                    WHERE route = ? AND size_bucket_multiplier = ?
                    """,
                    (route_value, size_value),
                ).fetchone()
                if parameter_row is not None:
                    try:
                        payload = json.loads(str(parameter_row[0]))
                        levels = tuple(Decimal(str(value)) for value in payload["entry_levels_bps"])
                        if len(levels) == 5 and entry_spread >= levels[0]:
                            bucket_index = max(
                                index
                                for index, lower_bound in enumerate(levels)
                                if entry_spread >= lower_bound
                            )
                            mapped = True
                    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                        mapped = False
                database.execute(
                    """
                    INSERT INTO route_calibration_episodes(
                        route, size_bucket_multiplier, epoch_id, spread_bucket_index,
                        entry_spread_bps, convergence_target_bps, peak_spread_bps, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        route_value,
                        size_value,
                        str(episode[2]),
                        bucket_index,
                        str(episode[3]),
                        str(episode[4]),
                        str(episode[5]),
                        str(episode[6]),
                    ),
                )
                if not mapped:
                    database.execute(
                        """
                        UPDATE route_calibration_parameters
                        SET active = 0, transient_blocked = 1
                        WHERE route = ? AND size_bucket_multiplier = ?
                        """,
                        (route_value, size_value),
                    )
            database.execute("DROP TABLE route_calibration_episodes_legacy_v10")
        calibration_columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(route_calibration_observations)")
        }
        if "reason" not in calibration_columns:
            database.execute(
                """
                ALTER TABLE route_calibration_observations
                ADD COLUMN reason TEXT NOT NULL DEFAULT 'CALIBRATION_INSUFFICIENT'
                """
            )
        parameter_columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(route_calibration_parameters)")
        }
        if "active" not in parameter_columns:
            database.execute(
                """
                ALTER TABLE route_calibration_parameters
                ADD COLUMN active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
                """
            )
        if "transient_blocked" not in parameter_columns:
            database.execute(
                """
                ALTER TABLE route_calibration_parameters
                ADD COLUMN transient_blocked INTEGER NOT NULL DEFAULT 0
                    CHECK (transient_blocked IN (0, 1))
                """
            )
        for statement in _SCHEMA_INDEX_STATEMENTS:
            database.execute(statement)
        for table in _EPOCH_OBSERVATION_TABLES:
            columns = {str(row[1]) for row in database.execute(f"PRAGMA table_info({table})")}
            if "epoch_id" not in columns:
                database.execute(
                    f"ALTER TABLE {table} ADD COLUMN epoch_id TEXT "
                    "REFERENCES qualification_epochs(epoch_id)"
                )
        database.execute(
            """
            INSERT INTO metadata(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("schema_version", SCHEMA_VERSION),
        )
        database.execute(
            """
            INSERT OR IGNORE INTO runtime_controls(
                singleton, paused, killed, reconciliation_state, updated_at
            ) VALUES (1, 0, 0, 'PENDING', ?)
            """,
            (datetime.now(UTC).isoformat(),),
        )
        database.commit()


async def initialise_state(path: Path) -> None:
    await asyncio.to_thread(_initialise_state_sync, path)


def _read_private_event_watermark_sync(path: Path, venue: Venue) -> int:
    with _connect(path) as database:
        row = database.execute(
            "SELECT event_watermark FROM private_event_watermarks WHERE venue = ?",
            (venue.value,),
        ).fetchone()
    return int(row[0]) if row is not None else 0


async def read_private_event_watermark(path: Path, venue: Venue) -> int:
    return await asyncio.to_thread(_read_private_event_watermark_sync, path, venue)


def _save_private_event_watermark_sync(path: Path, venue: Venue, watermark: int) -> None:
    if watermark < 0:
        raise ValueError("private event watermark must be non-negative")
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        row = database.execute(
            "SELECT event_watermark FROM private_event_watermarks WHERE venue = ?",
            (venue.value,),
        ).fetchone()
        if row is not None and watermark < int(row[0]):
            raise ValueError("private event watermark cannot regress")
        database.execute(
            """
            INSERT INTO private_event_watermarks(venue, event_watermark, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(venue) DO UPDATE SET
                event_watermark = excluded.event_watermark,
                updated_at = excluded.updated_at
            """,
            (venue.value, watermark, datetime.now(UTC).isoformat()),
        )
        database.commit()


async def save_private_event_watermark(path: Path, venue: Venue, watermark: int) -> None:
    await asyncio.to_thread(_save_private_event_watermark_sync, path, venue, watermark)


def _route_from_value(value: str) -> DirectedRouteKey:
    base, venues = value.split(":", 1)
    long_venue, short_venue = venues.split(">", 1)
    return DirectedRouteKey(base, Venue(long_venue), Venue(short_venue))


def _epoch_from_row(row: tuple[object, ...]) -> QualificationEpoch:
    return QualificationEpoch(
        epoch_id=str(row[0]),
        route=_route_from_value(str(row[1])),
        release_sha=str(row[2]),
        source_sha256=str(row[3]),
        config_sha256=str(row[4]),
        container_image_digest=str(row[5]),
        started_at=datetime.fromisoformat(str(row[6])),
        ended_at=datetime.fromisoformat(str(row[7])) if row[7] is not None else None,
        status=QualificationEpochStatus(str(row[8])),
    )


def _validate_epoch_identity(
    release_sha: str,
    source_sha256: str,
    config_sha256: str,
    container_image_digest: str,
) -> None:
    if len(release_sha) != 40 or any(value not in "0123456789abcdef" for value in release_sha):
        raise ValueError("qualification epoch release SHA is invalid")
    for name, value in (("source", source_sha256), ("config", config_sha256)):
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError(f"qualification epoch {name} SHA-256 is invalid")
    if not container_image_digest.startswith("sha256:") or len(container_image_digest) != 71:
        raise ValueError("qualification epoch image digest is invalid")


def _start_qualification_epoch_sync(
    path: Path,
    route: DirectedRouteKey,
    release_sha: str,
    source_sha256: str,
    config_sha256: str,
    container_image_digest: str,
    now: datetime,
) -> QualificationEpoch:
    _validate_epoch_identity(
        release_sha,
        source_sha256,
        config_sha256,
        container_image_digest,
    )
    identity = (
        route.value,
        release_sha,
        source_sha256,
        config_sha256,
        container_image_digest,
    )
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        active = database.execute(
            """
            SELECT epoch_id, route, release_sha, source_sha256, config_sha256,
                   container_image_digest, started_at, ended_at, status
            FROM qualification_epochs WHERE status = ? ORDER BY started_at DESC LIMIT 1
            """,
            (QualificationEpochStatus.RUNNING.value,),
        ).fetchone()
        if active is not None:
            parsed = _epoch_from_row(active)
            observed_identity = (
                parsed.route.value,
                parsed.release_sha,
                parsed.source_sha256,
                parsed.config_sha256,
                parsed.container_image_digest,
            )
            if observed_identity == identity:
                database.commit()
                return parsed
            database.execute(
                "UPDATE qualification_epochs SET status = ?, ended_at = ? WHERE epoch_id = ?",
                (QualificationEpochStatus.CLOSED.value, now.isoformat(), parsed.epoch_id),
            )
        finalized = database.execute(
            """
            SELECT epoch_id, route, release_sha, source_sha256, config_sha256,
                   container_image_digest, started_at, ended_at, status
            FROM qualification_epochs
            WHERE route = ? AND release_sha = ? AND source_sha256 = ? AND config_sha256 = ?
              AND container_image_digest = ? AND status = ?
            ORDER BY started_at DESC LIMIT 1
            """,
            (*identity, QualificationEpochStatus.FINALIZED.value),
        ).fetchone()
        if finalized is not None:
            database.commit()
            return _epoch_from_row(finalized)
        epoch_id = uuid4().hex
        database.execute(
            """
            INSERT INTO qualification_epochs (
                epoch_id, route, release_sha, source_sha256, config_sha256,
                container_image_digest, started_at, ended_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
            """,
            (
                epoch_id,
                *identity,
                now.isoformat(),
                QualificationEpochStatus.RUNNING.value,
            ),
        )
        database.commit()
    result = _read_qualification_epoch_sync(path, epoch_id)
    if result is None:
        raise RuntimeError("qualification epoch disappeared after start")
    return result


async def start_qualification_epoch(
    path: Path,
    route: DirectedRouteKey,
    release_sha: str,
    source_sha256: str,
    config_sha256: str,
    container_image_digest: str,
    now: datetime | None = None,
) -> QualificationEpoch:
    return await asyncio.to_thread(
        _start_qualification_epoch_sync,
        path,
        route,
        release_sha,
        source_sha256,
        config_sha256,
        container_image_digest,
        now or datetime.now(UTC),
    )


def _read_qualification_epoch_sync(
    path: Path,
    epoch_id: str | None = None,
) -> QualificationEpoch | None:
    with _connect(path) as database:
        if epoch_id is None:
            row = database.execute(
                """
                SELECT epoch_id, route, release_sha, source_sha256, config_sha256,
                       container_image_digest, started_at, ended_at, status
                FROM qualification_epochs ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
        else:
            row = database.execute(
                """
                SELECT epoch_id, route, release_sha, source_sha256, config_sha256,
                       container_image_digest, started_at, ended_at, status
                FROM qualification_epochs WHERE epoch_id = ?
                """,
                (epoch_id,),
            ).fetchone()
    return _epoch_from_row(row) if row is not None else None


async def read_qualification_epoch(
    path: Path,
    epoch_id: str | None = None,
) -> QualificationEpoch | None:
    return await asyncio.to_thread(_read_qualification_epoch_sync, path, epoch_id)


def _read_active_qualification_epoch_sync(path: Path) -> QualificationEpoch | None:
    with _connect(path) as database:
        row = database.execute(
            """
            SELECT epoch_id, route, release_sha, source_sha256, config_sha256,
                   container_image_digest, started_at, ended_at, status
            FROM qualification_epochs WHERE status = ? ORDER BY started_at DESC LIMIT 1
            """,
            (QualificationEpochStatus.RUNNING.value,),
        ).fetchone()
    return _epoch_from_row(row) if row is not None else None


async def read_active_qualification_epoch(path: Path) -> QualificationEpoch | None:
    return await asyncio.to_thread(_read_active_qualification_epoch_sync, path)


def _finalize_qualification_epoch_sync(
    path: Path,
    epoch_id: str,
    now: datetime,
) -> QualificationEpoch:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        row = database.execute(
            "SELECT status FROM qualification_epochs WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        if row is None:
            database.rollback()
            raise KeyError(epoch_id)
        status = QualificationEpochStatus(str(row[0]))
        if status == QualificationEpochStatus.CLOSED:
            database.rollback()
            raise RuntimeError("closed qualification epoch cannot be finalized")
        if status == QualificationEpochStatus.RUNNING:
            database.execute(
                "UPDATE qualification_epochs SET status = ?, ended_at = ? WHERE epoch_id = ?",
                (QualificationEpochStatus.FINALIZED.value, now.isoformat(), epoch_id),
            )
        database.commit()
    result = _read_qualification_epoch_sync(path, epoch_id)
    if result is None:
        raise RuntimeError("qualification epoch disappeared after finalize")
    return result


async def finalize_qualification_epoch(
    path: Path,
    epoch_id: str,
    now: datetime | None = None,
) -> QualificationEpoch:
    return await asyncio.to_thread(
        _finalize_qualification_epoch_sync,
        path,
        epoch_id,
        now or datetime.now(UTC),
    )


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
        try:
            row = database.execute(
                "SELECT status, heartbeat_at, starts FROM service_runtime WHERE singleton = 1"
            ).fetchone()
        except sqlite3.OperationalError:
            return ServiceHealth(False, ReasonCode.SERVICE_STATE_MISSING, None, None, 0)
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


def _read_runtime_controls_sync(path: Path) -> RuntimeControls:
    with _connect(path) as database:
        row = database.execute(
            """
            SELECT paused, killed, reconciliation_state, updated_at
            FROM runtime_controls WHERE singleton = 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("runtime controls are not initialised")
    return RuntimeControls(
        paused=bool(row[0]),
        killed=bool(row[1]),
        reconciliation_state=str(row[2]),
        updated_at=datetime.fromisoformat(str(row[3])),
    )


async def read_runtime_controls(path: Path) -> RuntimeControls:
    return await asyncio.to_thread(_read_runtime_controls_sync, path)


def _update_runtime_controls_sync(
    path: Path,
    paused: bool | None,
    killed: bool | None,
    reconciliation_state: str | None,
    now: datetime,
) -> RuntimeControls:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        current = database.execute(
            "SELECT paused, killed, reconciliation_state FROM runtime_controls WHERE singleton = 1"
        ).fetchone()
        if current is None:
            raise RuntimeError("runtime controls are not initialised")
        next_paused = bool(current[0]) if paused is None else paused
        next_killed = bool(current[1]) if killed is None else killed
        next_reconciliation = (
            str(current[2]) if reconciliation_state is None else reconciliation_state
        )
        if next_reconciliation not in {"PENDING", "CONSISTENT", "INCONSISTENT"}:
            raise ValueError("invalid reconciliation state")
        database.execute(
            """
            UPDATE runtime_controls
            SET paused = ?, killed = ?, reconciliation_state = ?, updated_at = ?
            WHERE singleton = 1
            """,
            (int(next_paused), int(next_killed), next_reconciliation, now.isoformat()),
        )
        database.commit()
    return RuntimeControls(next_paused, next_killed, next_reconciliation, now)


async def update_runtime_controls(
    path: Path,
    *,
    paused: bool | None = None,
    killed: bool | None = None,
    reconciliation_state: str | None = None,
    now: datetime | None = None,
) -> RuntimeControls:
    return await asyncio.to_thread(
        _update_runtime_controls_sync,
        path,
        paused,
        killed,
        reconciliation_state,
        now or datetime.now(UTC),
    )


def _record_command_audit_sync(
    path: Path,
    actor: str,
    command: str,
    outcome: str,
    reason: ReasonCode,
    now: datetime,
) -> None:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            """
            INSERT INTO command_audit(actor, command, outcome, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (actor, command, outcome, reason.value, now.isoformat()),
        )
        database.commit()


async def record_command_audit(
    path: Path,
    actor: str,
    command: str,
    outcome: str,
    reason: ReasonCode,
    now: datetime | None = None,
) -> None:
    if not actor.strip() or not command.strip() or not outcome.strip():
        raise ValueError("audit fields must be non-empty")
    await asyncio.to_thread(
        _record_command_audit_sync,
        path,
        actor,
        command,
        outcome,
        reason,
        now or datetime.now(UTC),
    )


def _read_command_audit_sync(path: Path) -> tuple[CommandAudit, ...]:
    with _connect(path) as database:
        rows = database.execute(
            """
            SELECT audit_id, actor, command, outcome, reason, created_at
            FROM command_audit ORDER BY audit_id
            """
        ).fetchall()
    return tuple(
        CommandAudit(
            audit_id=int(row[0]),
            actor=str(row[1]),
            command=str(row[2]),
            outcome=str(row[3]),
            reason=ReasonCode(str(row[4])),
            created_at=datetime.fromisoformat(str(row[5])),
        )
        for row in rows
    )


async def read_command_audit(path: Path) -> tuple[CommandAudit, ...]:
    return await asyncio.to_thread(_read_command_audit_sync, path)


def _fill_to_payload(fill: Fill) -> dict[str, str]:
    return {
        "client_order_id": fill.client_order_id,
        "venue": fill.venue.value,
        "side": fill.side.value,
        "purpose": fill.purpose.value,
        "quantity": str(fill.quantity),
        "price": str(fill.price),
        "fee_usdt": str(fill.fee_usdt),
    }


def _fill_from_payload(payload: dict[str, Any]) -> Fill:
    from decimal import Decimal

    return Fill(
        client_order_id=str(payload["client_order_id"]),
        venue=Venue(str(payload["venue"])),
        side=Side(str(payload["side"])),
        purpose=OrderPurpose(str(payload["purpose"])),
        quantity=Decimal(str(payload["quantity"])),
        price=Decimal(str(payload["price"])),
        fee_usdt=Decimal(str(payload["fee_usdt"])),
    )


def _tranche_to_payload(tranche: Tranche) -> dict[str, Any]:
    return {
        "tranche_id": tranche.tranche_id,
        "base": tranche.route.base,
        "long_venue": tranche.route.long_venue.value,
        "short_venue": tranche.route.short_venue.value,
        "requested_quantity": str(tranche.requested_quantity),
        "target_close_spread": str(tranche.target_close_spread),
        "stop_spread": str(tranche.stop_spread),
        "projected_stress_usdt": str(tranche.projected_stress_usdt),
        "state": tranche.state.value,
        "reason": tranche.reason.value if tranche.reason is not None else None,
        "entry_long_fills": [_fill_to_payload(fill) for fill in tranche.entry_long_fills],
        "entry_short_fills": [_fill_to_payload(fill) for fill in tranche.entry_short_fills],
        "close_long_fills": [_fill_to_payload(fill) for fill in tranche.close_long_fills],
        "close_short_fills": [_fill_to_payload(fill) for fill in tranche.close_short_fills],
        "emergency_fills": [_fill_to_payload(fill) for fill in tranche.emergency_fills],
        "funding_usdt": str(tranche.funding_usdt),
        "processed_order_ids": sorted(tranche.processed_order_ids),
    }


def _tranche_from_payload(payload: dict[str, Any]) -> Tranche:
    from decimal import Decimal

    reason_value = payload.get("reason")
    return Tranche(
        tranche_id=str(payload["tranche_id"]),
        route=DirectedRouteKey(
            str(payload["base"]),
            Venue(str(payload["long_venue"])),
            Venue(str(payload["short_venue"])),
        ),
        requested_quantity=Decimal(str(payload["requested_quantity"])),
        target_close_spread=Decimal(str(payload["target_close_spread"])),
        stop_spread=Decimal(str(payload["stop_spread"])),
        projected_stress_usdt=Decimal(str(payload["projected_stress_usdt"])),
        state=PairActionState(str(payload["state"])),
        reason=ReasonCode(str(reason_value)) if reason_value is not None else None,
        entry_long_fills=[_fill_from_payload(item) for item in payload["entry_long_fills"]],
        entry_short_fills=[_fill_from_payload(item) for item in payload["entry_short_fills"]],
        close_long_fills=[_fill_from_payload(item) for item in payload["close_long_fills"]],
        close_short_fills=[_fill_from_payload(item) for item in payload["close_short_fills"]],
        emergency_fills=[_fill_from_payload(item) for item in payload["emergency_fills"]],
        funding_usdt=Decimal(str(payload["funding_usdt"])),
        processed_order_ids={str(item) for item in payload["processed_order_ids"]},
    )


def _save_tranche_sync(
    path: Path,
    tranche: Tranche,
    now: datetime,
    deadline_monotonic: float | None,
) -> None:
    if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
        raise TimeoutError("simulated tranche persistence deadline expired")
    payload = json.dumps(_tranche_to_payload(tranche), sort_keys=True, separators=(",", ":"))
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            """
            INSERT INTO simulated_tranches(tranche_id, lifecycle_state, payload_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tranche_id) DO UPDATE SET
                lifecycle_state = excluded.lifecycle_state,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (tranche.tranche_id, tranche.state.value, payload, now.isoformat()),
        )
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            database.rollback()
            raise TimeoutError("simulated tranche persistence deadline expired")
        database.commit()


async def save_tranche(
    path: Path,
    tranche: Tranche,
    now: datetime | None = None,
    *,
    deadline_monotonic: float | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    if deadline_monotonic is not None and loop.time() >= deadline_monotonic:
        raise TimeoutError("simulated tranche persistence deadline expired")
    terminal_deadline = min(
        deadline_monotonic or float("inf"),
        loop.time() + STATE_TRANSITION_TERMINAL_TIMEOUT_SECONDS,
    )
    worker = _DaemonStateWorker(
        lambda: _save_tranche_sync(path, tranche, now or datetime.now(UTC), deadline_monotonic),
        name=f"state-save-tranche-{tranche.tranche_id}",
    )
    await _await_daemon_state_worker(worker, deadline_monotonic=terminal_deadline)


def _delete_tranche_sync(path: Path, tranche_id: str) -> None:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            "DELETE FROM simulated_tranches WHERE tranche_id = ?",
            (tranche_id,),
        )
        database.commit()


async def delete_tranche(
    path: Path,
    tranche_id: str,
    *,
    timeout_seconds: float = STATE_TRANSITION_TERMINAL_TIMEOUT_SECONDS,
) -> None:
    if timeout_seconds <= 0:
        raise ValueError("tranche deletion timeout must be positive")
    worker = _DaemonStateWorker(
        lambda: _delete_tranche_sync(path, tranche_id),
        name=f"state-delete-tranche-{tranche_id}",
    )
    await _await_daemon_state_worker(
        worker,
        deadline_monotonic=asyncio.get_running_loop().time() + timeout_seconds,
    )


def _load_tranches_sync(path: Path) -> tuple[Tranche, ...]:
    with _connect(path) as database:
        rows = database.execute(
            "SELECT payload_json FROM simulated_tranches ORDER BY tranche_id"
        ).fetchall()
    return tuple(_tranche_from_payload(json.loads(str(row[0]))) for row in rows)


async def load_tranches(path: Path) -> tuple[Tranche, ...]:
    return await asyncio.to_thread(_load_tranches_sync, path)


def _save_shadow_snapshot_sync(path: Path, payload: dict[str, Any], now: datetime) -> None:
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            """
            INSERT INTO shadow_snapshot(singleton, payload_json, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (rendered, now.isoformat()),
        )
        database.commit()


async def save_shadow_snapshot(
    path: Path,
    payload: dict[str, Any],
    now: datetime | None = None,
) -> None:
    await asyncio.to_thread(
        _save_shadow_snapshot_sync,
        path,
        payload,
        now or datetime.now(UTC),
    )


def _read_shadow_snapshot_sync(path: Path) -> dict[str, Any] | None:
    with _connect(path) as database:
        row = database.execute(
            "SELECT payload_json FROM shadow_snapshot WHERE singleton = 1"
        ).fetchone()
    if row is None:
        return None
    parsed = json.loads(str(row[0]))
    if not isinstance(parsed, dict):
        raise RuntimeError("shadow snapshot payload must be an object")
    return parsed


async def read_shadow_snapshot(path: Path) -> dict[str, Any] | None:
    return await asyncio.to_thread(_read_shadow_snapshot_sync, path)


def _record_live_confirmation_sync(
    path: Path,
    actor: str,
    confirmed_until: datetime,
    now: datetime,
) -> None:
    if confirmed_until <= now:
        raise ValueError("live confirmation expiry must be in the future")
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            """
            INSERT INTO live_confirmation(singleton, confirmed_until, confirmed_by, created_at)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET
                confirmed_until = excluded.confirmed_until,
                confirmed_by = excluded.confirmed_by,
                created_at = excluded.created_at
            """,
            (confirmed_until.isoformat(), actor, now.isoformat()),
        )
        database.commit()


async def record_live_confirmation(
    path: Path,
    actor: str,
    confirmed_until: datetime,
    now: datetime | None = None,
) -> None:
    if not actor.strip():
        raise ValueError("live confirmation actor must be non-empty")
    await asyncio.to_thread(
        _record_live_confirmation_sync,
        path,
        actor,
        confirmed_until,
        now or datetime.now(UTC),
    )


def _live_confirmation_valid_sync(path: Path, now: datetime) -> bool:
    with _connect(path) as database:
        row = database.execute(
            "SELECT confirmed_until FROM live_confirmation WHERE singleton = 1"
        ).fetchone()
    return row is not None and datetime.fromisoformat(str(row[0])) >= now


async def live_confirmation_valid(path: Path, now: datetime | None = None) -> bool:
    return await asyncio.to_thread(
        _live_confirmation_valid_sync,
        path,
        now or datetime.now(UTC),
    )


@dataclass(frozen=True, slots=True)
class StoredQualificationStatistics:
    funding_rows: tuple[tuple[Venue, datetime, str, int, str], ...]
    accepted_signals: int
    rejected_signals: int
    latest_simulated_net_pnl_usdt: str
    maximum_adverse_excursion_usdt: str
    unhandled_exception_count: int
    strategy: dict[str, str | int] | None


def _record_qualification_scan_sync(
    path: Path,
    epoch_id: str,
    base: str,
    funding: tuple[FundingSnapshot, ...],
    decisions: tuple[SignalDecision, ...],
    tranches: tuple[Tranche, ...],
    stressed_cost_multiplier: str,
    expected_holding_seconds: int,
    maximum_holding_seconds: int,
    now: datetime,
) -> None:
    with _connect(path) as database:
        database.execute("BEGIN IMMEDIATE")
        epoch_row = database.execute(
            "SELECT route, status FROM qualification_epochs WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        if (
            epoch_row is None
            or QualificationEpochStatus(str(epoch_row[1])) != QualificationEpochStatus.RUNNING
        ):
            database.rollback()
            raise RuntimeError("qualification observations require a running exact epoch")
        epoch_route = _route_from_value(str(epoch_row[0]))
        if base.upper() != epoch_route.base.upper():
            database.rollback()
            raise ValueError("qualification scan base does not match epoch route")
        allowed_venues = {epoch_route.long_venue, epoch_route.short_venue}
        for snapshot in funding:
            if (
                snapshot.rate is None
                or snapshot.next_funding_timestamp_ms is None
                or snapshot.interval is None
                or snapshot.venue not in allowed_venues
            ):
                continue
            database.execute(
                """
                INSERT OR IGNORE INTO qualification_funding_observations (
                    epoch_id, base, venue, observed_at, rate,
                    next_funding_timestamp_ms, interval
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    base.upper(),
                    snapshot.venue.value,
                    now.isoformat(),
                    str(snapshot.rate),
                    snapshot.next_funding_timestamp_ms,
                    snapshot.interval,
                ),
            )
        for decision in decisions:
            if decision.route != epoch_route:
                continue
            database.execute(
                """
                INSERT INTO qualification_signal_observations (
                    epoch_id, route, accepted, reason, expected_net_pnl_usdt, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    decision.route.value,
                    int(decision.accepted),
                    decision.reason.value,
                    str(decision.cost.expected_net_pnl_usdt),
                    now.isoformat(),
                ),
            )
            database.execute(
                """
                INSERT INTO qualification_strategy_parameters (
                    epoch_id, route, size_bucket_base_quantity, calibration_version,
                    adaptive_entry_threshold_bps, target_exit_spread_bps,
                    minimum_profit_usdt, stressed_cost_multiplier,
                    expected_holding_seconds, maximum_holding_seconds, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    decision.route.value,
                    str(decision.inputs["size_bucket_base_quantity"]),
                    decision.calibration_version,
                    str(decision.inputs["adaptive_entry_threshold_bps"]),
                    str(decision.inputs["target_exit_spread_bps"]),
                    str(decision.inputs["minimum_profit_usdt"]),
                    stressed_cost_multiplier,
                    expected_holding_seconds,
                    maximum_holding_seconds,
                    now.isoformat(),
                ),
            )
        pnl = sum(
            (tranche.pnl().net_pnl_usdt for tranche in tranches if tranche.route == epoch_route),
            Decimal(0),
        )
        database.execute(
            """
            INSERT INTO qualification_pnl_observations (
                epoch_id, route, simulated_net_pnl_usdt, observed_at
            ) VALUES (?, ?, ?, ?)
            """,
            (epoch_id, epoch_route.value, str(pnl), now.isoformat()),
        )
        database.commit()


async def record_qualification_scan(
    path: Path,
    epoch_id: str,
    base: str,
    funding: tuple[FundingSnapshot, ...],
    decisions: tuple[SignalDecision, ...],
    tranches: tuple[Tranche, ...],
    stressed_cost_multiplier: Decimal,
    expected_holding_seconds: int,
    maximum_holding_seconds: int,
    now: datetime | None = None,
) -> None:
    await asyncio.to_thread(
        _record_qualification_scan_sync,
        path,
        epoch_id,
        base,
        funding,
        decisions,
        tranches,
        str(stressed_cost_multiplier),
        expected_holding_seconds,
        maximum_holding_seconds,
        now or datetime.now(UTC),
    )


def _record_qualification_exception_sync(
    path: Path,
    epoch_id: str,
    error_type: str,
    now: datetime,
) -> None:
    with _connect(path) as database:
        epoch = database.execute(
            "SELECT status FROM qualification_epochs WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        if (
            epoch is None
            or QualificationEpochStatus(str(epoch[0])) != QualificationEpochStatus.RUNNING
        ):
            raise RuntimeError("qualification exception requires a running exact epoch")
        database.execute(
            """
            INSERT INTO qualification_runtime_errors(epoch_id, error_type, observed_at)
            VALUES (?, ?, ?)
            """,
            (epoch_id, error_type, now.isoformat()),
        )
        database.commit()


async def record_qualification_exception(
    path: Path,
    epoch_id: str,
    error_type: str,
    now: datetime | None = None,
) -> None:
    if not error_type.strip():
        raise ValueError("qualification exception type must be non-empty")
    await asyncio.to_thread(
        _record_qualification_exception_sync,
        path,
        epoch_id,
        error_type,
        now or datetime.now(UTC),
    )


def _read_qualification_statistics_sync(
    path: Path,
    epoch_id: str,
) -> StoredQualificationStatistics:
    with _connect(path) as database:
        epoch_row = database.execute(
            "SELECT route FROM qualification_epochs WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        if epoch_row is None:
            raise KeyError(epoch_id)
        route = _route_from_value(str(epoch_row[0]))
        funding_rows = database.execute(
            """
            SELECT venue, observed_at, rate, next_funding_timestamp_ms, interval
            FROM qualification_funding_observations
            WHERE epoch_id = ? AND base = ? AND venue IN (?, ?)
            ORDER BY observed_at, venue
            """,
            (
                epoch_id,
                route.base.upper(),
                route.long_venue.value,
                route.short_venue.value,
            ),
        ).fetchall()
        signal_row = database.execute(
            """
            SELECT
                coalesce(sum(CASE WHEN accepted = 1 THEN 1 ELSE 0 END), 0),
                coalesce(sum(CASE WHEN accepted = 0 THEN 1 ELSE 0 END), 0)
            FROM qualification_signal_observations
            WHERE epoch_id = ? AND route = ?
            """,
            (epoch_id, route.value),
        ).fetchone()
        pnl_rows = database.execute(
            """
            SELECT simulated_net_pnl_usdt
            FROM qualification_pnl_observations
            WHERE epoch_id = ? AND route = ?
            ORDER BY observed_at
            """,
            (epoch_id, route.value),
        ).fetchall()
        error_row = database.execute(
            """
            SELECT count(*) FROM qualification_runtime_errors
            WHERE epoch_id = ?
            """,
            (epoch_id,),
        ).fetchone()
        strategy_row = database.execute(
            """
            SELECT size_bucket_base_quantity, calibration_version,
                   adaptive_entry_threshold_bps, target_exit_spread_bps,
                   minimum_profit_usdt, stressed_cost_multiplier,
                   expected_holding_seconds, maximum_holding_seconds
            FROM qualification_strategy_parameters
            WHERE epoch_id = ? AND route = ?
            ORDER BY observation_id DESC LIMIT 1
            """,
            (epoch_id, route.value),
        ).fetchone()
    pnl_values = tuple(Decimal(str(row[0])) for row in pnl_rows)
    latest_pnl = pnl_values[-1] if pnl_values else Decimal(0)
    adverse = abs(min((Decimal(0), *pnl_values)))
    strategy: dict[str, str | int] | None = (
        {
            "size_bucket_base_quantity": str(strategy_row[0]),
            "calibration_version": int(strategy_row[1]),
            "adaptive_entry_threshold_bps": str(strategy_row[2]),
            "target_exit_spread_bps": str(strategy_row[3]),
            "minimum_profit_usdt": str(strategy_row[4]),
            "stressed_cost_multiplier": str(strategy_row[5]),
            "expected_holding_seconds": int(strategy_row[6]),
            "maximum_holding_seconds": int(strategy_row[7]),
        }
        if strategy_row is not None
        else None
    )
    return StoredQualificationStatistics(
        funding_rows=tuple(
            (
                Venue(str(row[0])),
                datetime.fromisoformat(str(row[1])),
                str(row[2]),
                int(row[3]),
                str(row[4]),
            )
            for row in funding_rows
        ),
        accepted_signals=int(signal_row[0]) if signal_row else 0,
        rejected_signals=int(signal_row[1]) if signal_row else 0,
        latest_simulated_net_pnl_usdt=str(latest_pnl),
        maximum_adverse_excursion_usdt=str(adverse),
        unhandled_exception_count=int(error_row[0]) if error_row else 0,
        strategy=strategy,
    )


async def read_qualification_statistics(
    path: Path,
    epoch_id: str,
) -> StoredQualificationStatistics:
    return await asyncio.to_thread(
        _read_qualification_statistics_sync,
        path,
        epoch_id,
    )
