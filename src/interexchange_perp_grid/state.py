from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

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

SCHEMA_VERSION = "4"
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
    CREATE TABLE IF NOT EXISTS qualification_funding_observations (
        base TEXT NOT NULL,
        venue TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        rate TEXT NOT NULL,
        next_funding_timestamp_ms INTEGER NOT NULL,
        interval TEXT NOT NULL,
        PRIMARY KEY (base, venue, next_funding_timestamp_ms, observed_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_signal_observations (
        observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        route TEXT NOT NULL,
        simulated_net_pnl_usdt TEXT NOT NULL,
        observed_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS qualification_runtime_errors (
        error_id INTEGER PRIMARY KEY AUTOINCREMENT,
        error_type TEXT NOT NULL,
        observed_at TEXT NOT NULL
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
        if existing is not None and existing[0] not in {"1", "2", "3", SCHEMA_VERSION}:
            raise RuntimeError(f"unsupported state schema version: {existing[0]}")
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


def _save_tranche_sync(path: Path, tranche: Tranche, now: datetime) -> None:
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
        database.commit()


async def save_tranche(path: Path, tranche: Tranche, now: datetime | None = None) -> None:
    await asyncio.to_thread(_save_tranche_sync, path, tranche, now or datetime.now(UTC))


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
        for snapshot in funding:
            if (
                snapshot.rate is None
                or snapshot.next_funding_timestamp_ms is None
                or snapshot.interval is None
            ):
                continue
            database.execute(
                """
                INSERT OR IGNORE INTO qualification_funding_observations (
                    base, venue, observed_at, rate, next_funding_timestamp_ms, interval
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    base.upper(),
                    snapshot.venue.value,
                    now.isoformat(),
                    str(snapshot.rate),
                    snapshot.next_funding_timestamp_ms,
                    snapshot.interval,
                ),
            )
        for decision in decisions:
            database.execute(
                """
                INSERT INTO qualification_signal_observations (
                    route, accepted, reason, expected_net_pnl_usdt, observed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
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
                    route, size_bucket_base_quantity, calibration_version,
                    adaptive_entry_threshold_bps, target_exit_spread_bps,
                    minimum_profit_usdt, stressed_cost_multiplier,
                    expected_holding_seconds, maximum_holding_seconds, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
        routes = {decision.route for decision in decisions} | {
            tranche.route for tranche in tranches
        }
        for route in routes:
            pnl = sum(
                (tranche.pnl().net_pnl_usdt for tranche in tranches if tranche.route == route),
                Decimal(0),
            )
            database.execute(
                """
                INSERT INTO qualification_pnl_observations (
                    route, simulated_net_pnl_usdt, observed_at
                ) VALUES (?, ?, ?)
                """,
                (route.value, str(pnl), now.isoformat()),
            )
        database.commit()


async def record_qualification_scan(
    path: Path,
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
    error_type: str,
    now: datetime,
) -> None:
    with _connect(path) as database:
        database.execute(
            """
            INSERT INTO qualification_runtime_errors(error_type, observed_at)
            VALUES (?, ?)
            """,
            (error_type, now.isoformat()),
        )
        database.commit()


async def record_qualification_exception(
    path: Path,
    error_type: str,
    now: datetime | None = None,
) -> None:
    if not error_type.strip():
        raise ValueError("qualification exception type must be non-empty")
    await asyncio.to_thread(
        _record_qualification_exception_sync,
        path,
        error_type,
        now or datetime.now(UTC),
    )


def _read_qualification_statistics_sync(
    path: Path,
    route: DirectedRouteKey,
    since: datetime,
) -> StoredQualificationStatistics:
    with _connect(path) as database:
        funding_rows = database.execute(
            """
            SELECT venue, observed_at, rate, next_funding_timestamp_ms, interval
            FROM qualification_funding_observations
            WHERE base = ? AND venue IN (?, ?) AND observed_at >= ?
            ORDER BY observed_at, venue
            """,
            (
                route.base.upper(),
                route.long_venue.value,
                route.short_venue.value,
                since.isoformat(),
            ),
        ).fetchall()
        signal_row = database.execute(
            """
            SELECT
                coalesce(sum(CASE WHEN accepted = 1 THEN 1 ELSE 0 END), 0),
                coalesce(sum(CASE WHEN accepted = 0 THEN 1 ELSE 0 END), 0)
            FROM qualification_signal_observations
            WHERE route = ? AND observed_at >= ?
            """,
            (route.value, since.isoformat()),
        ).fetchone()
        pnl_rows = database.execute(
            """
            SELECT simulated_net_pnl_usdt
            FROM qualification_pnl_observations
            WHERE route = ? AND observed_at >= ?
            ORDER BY observed_at
            """,
            (route.value, since.isoformat()),
        ).fetchall()
        error_row = database.execute(
            """
            SELECT count(*) FROM qualification_runtime_errors
            WHERE observed_at >= ?
            """,
            (since.isoformat(),),
        ).fetchone()
        strategy_row = database.execute(
            """
            SELECT size_bucket_base_quantity, calibration_version,
                   adaptive_entry_threshold_bps, target_exit_spread_bps,
                   minimum_profit_usdt, stressed_cost_multiplier,
                   expected_holding_seconds, maximum_holding_seconds
            FROM qualification_strategy_parameters
            WHERE route = ? AND observed_at >= ?
            ORDER BY observation_id DESC LIMIT 1
            """,
            (route.value, since.isoformat()),
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
    route: DirectedRouteKey,
    since: datetime,
) -> StoredQualificationStatistics:
    return await asyncio.to_thread(
        _read_qualification_statistics_sync,
        path,
        route,
        since,
    )
