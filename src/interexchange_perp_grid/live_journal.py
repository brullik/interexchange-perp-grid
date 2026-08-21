from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from interexchange_perp_grid.client_ids import parse_bot_client_order_id
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import (
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

_PROCESS_INCARCATION = secrets.token_hex(32)


class LiveActionState(StrEnum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    RECOVERING = "RECOVERING"
    HEDGED = "HEDGED"
    CLOSING = "CLOSING"
    FLAT = "FLAT"
    QUARANTINED = "QUARANTINED"


def is_completed_normal_paired_cycle(action: LiveJournalAction) -> bool:
    """Return true only for a canonical normal paired open-and-close cycle."""
    if (
        action.state != LiveActionState.FLAT
        or action.recovery_action is not None
        or len(action.legs) != 4
    ):
        return False
    roles: list[str] = []
    for leg in action.legs:
        parsed = parse_bot_client_order_id(leg.client_order_id)
        if (
            parsed is None
            or leg.status != PrivateOrderStatus.FILLED
            or leg.filled_base_quantity != leg.intended_base_quantity
            or leg.filled_base_quantity <= 0
        ):
            return False
        roles.append(parsed.role_code)
    return sorted(roles) == ["clo", "clo", "lon", "sho"]


def completed_normal_actions_sha256(actions: tuple[LiveJournalAction, ...]) -> str:
    eligible = tuple(action for action in actions if is_completed_normal_paired_cycle(action))
    return hashlib.sha256(
        json.dumps(
            tuple(asdict(action) for action in eligible),
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


_TRANSITIONS: dict[LiveActionState, frozenset[LiveActionState]] = {
    LiveActionState.PREPARED: frozenset({LiveActionState.SUBMITTING, LiveActionState.QUARANTINED}),
    LiveActionState.SUBMITTING: frozenset(
        {
            LiveActionState.ACKNOWLEDGED,
            LiveActionState.PARTIAL,
            LiveActionState.FILLED,
            LiveActionState.REJECTED,
            LiveActionState.UNKNOWN,
            LiveActionState.RECOVERING,
            LiveActionState.QUARANTINED,
        }
    ),
    LiveActionState.ACKNOWLEDGED: frozenset(
        {
            LiveActionState.PARTIAL,
            LiveActionState.FILLED,
            LiveActionState.REJECTED,
            LiveActionState.UNKNOWN,
            LiveActionState.RECOVERING,
            LiveActionState.CLOSING,
            LiveActionState.QUARANTINED,
        }
    ),
    LiveActionState.PARTIAL: frozenset(
        {
            LiveActionState.FILLED,
            LiveActionState.REJECTED,
            LiveActionState.UNKNOWN,
            LiveActionState.RECOVERING,
            LiveActionState.HEDGED,
            LiveActionState.CLOSING,
            LiveActionState.QUARANTINED,
        }
    ),
    LiveActionState.FILLED: frozenset(
        {
            LiveActionState.RECOVERING,
            LiveActionState.HEDGED,
            LiveActionState.CLOSING,
            LiveActionState.QUARANTINED,
        }
    ),
    LiveActionState.REJECTED: frozenset(
        {LiveActionState.RECOVERING, LiveActionState.FLAT, LiveActionState.QUARANTINED}
    ),
    LiveActionState.UNKNOWN: frozenset({LiveActionState.RECOVERING, LiveActionState.QUARANTINED}),
    LiveActionState.RECOVERING: frozenset(
        {
            LiveActionState.HEDGED,
            LiveActionState.CLOSING,
            LiveActionState.FLAT,
            LiveActionState.QUARANTINED,
        }
    ),
    LiveActionState.HEDGED: frozenset({LiveActionState.CLOSING, LiveActionState.QUARANTINED}),
    LiveActionState.CLOSING: frozenset(
        {LiveActionState.FLAT, LiveActionState.RECOVERING, LiveActionState.QUARANTINED}
    ),
    LiveActionState.FLAT: frozenset({LiveActionState.QUARANTINED}),
    LiveActionState.QUARANTINED: frozenset({LiveActionState.RECOVERING, LiveActionState.FLAT}),
}

_ORDER_STATUS_TRANSITIONS: dict[PrivateOrderStatus | None, frozenset[PrivateOrderStatus]] = {
    None: frozenset(PrivateOrderStatus),
    PrivateOrderStatus.OPEN: frozenset(
        {
            PrivateOrderStatus.OPEN,
            PrivateOrderStatus.PARTIAL,
            PrivateOrderStatus.FILLED,
            PrivateOrderStatus.CANCELLED,
            PrivateOrderStatus.REJECTED,
            PrivateOrderStatus.UNKNOWN,
        }
    ),
    PrivateOrderStatus.PARTIAL: frozenset(
        {
            PrivateOrderStatus.PARTIAL,
            PrivateOrderStatus.FILLED,
            PrivateOrderStatus.CANCELLED,
            PrivateOrderStatus.UNKNOWN,
        }
    ),
    PrivateOrderStatus.UNKNOWN: frozenset(PrivateOrderStatus),
    PrivateOrderStatus.FILLED: frozenset({PrivateOrderStatus.FILLED}),
    PrivateOrderStatus.CANCELLED: frozenset({PrivateOrderStatus.CANCELLED}),
    PrivateOrderStatus.REJECTED: frozenset({PrivateOrderStatus.REJECTED}),
}


class JournalEventQuarantinedError(RuntimeError):
    pass


MAX_ACTIVE_LIVE_ACTIONS = 10


@dataclass(frozen=True, slots=True)
class JournalLeg:
    client_order_id: str
    venue: Venue
    symbol: str
    side: Side
    request_payload_hash: str
    intended_base_quantity: Decimal
    protected_price: Decimal | None
    submit_attempted: bool
    order_id: str | None
    status: PrivateOrderStatus | None
    filled_base_quantity: Decimal


@dataclass(frozen=True, slots=True)
class LiveJournalAction:
    pair_action_id: str
    route: DirectedRouteKey
    tranche_id: str
    state: LiveActionState
    risk_reservation: dict[str, Any]
    qualification_hash: str
    residual_delta: Decimal
    recovery_action: str | None
    created_at: datetime
    updated_at: datetime
    legs: tuple[JournalLeg, ...]


@dataclass(frozen=True, slots=True)
class FlatBarrierCommitResult:
    committed: bool
    action: LiveJournalAction | None
    event_watermark: int


@dataclass(frozen=True, slots=True)
class FlatBarrierBatchCommitResult:
    committed: bool
    actions: tuple[LiveJournalAction, ...]
    event_watermark: int


def request_payload_hash(request: VenueOrderRequest) -> str:
    encoded = json.dumps(asdict(request), default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class LiveOrderJournal:
    """SQLite WAL journal; all order intent is durable before network submission."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def initialise(self) -> None:
        await asyncio.to_thread(self._initialise_sync)

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.path, isolation_level=None)
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA foreign_keys=ON")
        database.execute("PRAGMA busy_timeout=5000")
        database.execute("PRAGMA synchronous=FULL")
        return database

    def _initialise_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as database:
            mode = database.execute("PRAGMA journal_mode=WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RuntimeError("live journal requires SQLite WAL")
            database.execute("PRAGMA synchronous=FULL")
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS live_pair_actions (
                    pair_action_id TEXT PRIMARY KEY,
                    route_base TEXT NOT NULL,
                    long_venue TEXT NOT NULL,
                    short_venue TEXT NOT NULL,
                    tranche_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    risk_reservation_json TEXT NOT NULL,
                    qualification_hash TEXT NOT NULL,
                    residual_delta TEXT NOT NULL,
                    recovery_action TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_order_legs (
                    client_order_id TEXT PRIMARY KEY,
                    pair_action_id TEXT NOT NULL REFERENCES live_pair_actions(pair_action_id),
                    venue TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    request_payload_hash TEXT NOT NULL,
                    intended_base_quantity TEXT NOT NULL,
                    protected_price TEXT,
                    submit_attempted INTEGER NOT NULL DEFAULT 0,
                    order_id TEXT,
                    status TEXT,
                    filled_base_quantity TEXT NOT NULL DEFAULT '0',
                    last_event_at TEXT
                );
                CREATE TABLE IF NOT EXISTS live_action_transitions (
                    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_action_id TEXT NOT NULL REFERENCES live_pair_actions(pair_action_id),
                    from_state TEXT,
                    to_state TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_order_events (
                    client_order_id TEXT NOT NULL REFERENCES live_order_legs(client_order_id),
                    event_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    filled_base_quantity TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (client_order_id, event_key)
                );
                CREATE TABLE IF NOT EXISTS live_journal_audit_events (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_action_id TEXT NOT NULL REFERENCES live_pair_actions(pair_action_id),
                    client_order_id TEXT,
                    code TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_action_leases (
                    lease_key TEXT PRIMARY KEY,
                    lease_kind TEXT NOT NULL CHECK (lease_kind IN ('BASE', 'ROUTE')),
                    pair_action_id TEXT NOT NULL REFERENCES live_pair_actions(pair_action_id)
                        ON DELETE CASCADE,
                    acquired_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_live_action_leases_pair
                    ON live_action_leases(pair_action_id);
                CREATE TABLE IF NOT EXISTS live_control_leases (
                    lease_key TEXT PRIMARY KEY,
                    owner_token TEXT NOT NULL,
                    owner_pid INTEGER NOT NULL,
                    owner_incarnation TEXT NOT NULL,
                    owner_process_identity TEXT NOT NULL,
                    acquired_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_entry_controls (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    risk_stage_completion_frozen INTEGER NOT NULL DEFAULT 0
                        CHECK (risk_stage_completion_frozen IN (0, 1))
                );
                """
            )
            database.execute(
                "INSERT OR IGNORE INTO live_entry_controls("
                "singleton, risk_stage_completion_frozen) VALUES (1, 0)"
            )
            columns = {
                str(row[1]) for row in database.execute("PRAGMA table_info(live_order_legs)")
            }
            if "last_event_at" not in columns:
                database.execute("ALTER TABLE live_order_legs ADD COLUMN last_event_at TEXT")
            control_columns = {
                str(row[1]) for row in database.execute("PRAGMA table_info(live_control_leases)")
            }
            if "owner_pid" not in control_columns:
                database.execute(
                    "ALTER TABLE live_control_leases ADD COLUMN owner_pid "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "owner_incarnation" not in control_columns:
                database.execute(
                    "ALTER TABLE live_control_leases ADD COLUMN owner_incarnation "
                    "TEXT NOT NULL DEFAULT ''"
                )
            if "owner_process_identity" not in control_columns:
                database.execute(
                    "ALTER TABLE live_control_leases ADD COLUMN owner_process_identity "
                    "TEXT NOT NULL DEFAULT ''"
                )
            database.execute("BEGIN IMMEDIATE")
            try:
                active_rows = database.execute(
                    """
                SELECT pair_action_id, route_base, long_venue, short_venue, created_at,
                       recovery_action
                    FROM live_pair_actions WHERE state <> ? ORDER BY created_at, pair_action_id
                    """,
                    (LiveActionState.FLAT.value,),
                ).fetchall()
                if len(active_rows) > MAX_ACTIVE_LIVE_ACTIONS:
                    raise RuntimeError("legacy active live actions exceed the maximum limit")
                for row in active_rows:
                    route = DirectedRouteKey(
                        str(row["route_base"]),
                        Venue(str(row["long_venue"])),
                        Venue(str(row["short_venue"])),
                    )
                    self._acquire_leases_in_transaction(
                        database,
                        str(row["pair_action_id"]),
                        route,
                        str(row["created_at"]),
                        idempotent=True,
                        emergency_exclusive=str(row["recovery_action"] or "")
                        == "EMERGENCY_FLATTEN",
                    )
                database.commit()
            except Exception:
                database.rollback()
                raise

    async def prepare(
        self,
        pair_action_id: str,
        route: DirectedRouteKey,
        tranche_id: str,
        long_request: VenueOrderRequest,
        short_request: VenueOrderRequest,
        intended_base_quantities: dict[Venue, Decimal],
        protected_prices: dict[Venue, Decimal],
        risk_reservation: dict[str, Any],
        qualification_hash: str,
        now: datetime | None = None,
    ) -> LiveJournalAction:
        return await asyncio.to_thread(
            self._prepare_sync,
            pair_action_id,
            route,
            tranche_id,
            long_request,
            short_request,
            intended_base_quantities,
            protected_prices,
            risk_reservation,
            qualification_hash,
            now or datetime.now(UTC),
        )

    def _prepare_sync(
        self,
        pair_action_id: str,
        route: DirectedRouteKey,
        tranche_id: str,
        long_request: VenueOrderRequest,
        short_request: VenueOrderRequest,
        intended_base_quantities: dict[Venue, Decimal],
        protected_prices: dict[Venue, Decimal],
        risk_reservation: dict[str, Any],
        qualification_hash: str,
        now: datetime,
    ) -> LiveJournalAction:
        if not pair_action_id.strip() or not tranche_id.strip():
            raise ValueError("pair action and tranche IDs must be non-empty")
        self._require_canonical_route_base(route)
        requests = (long_request, short_request)
        if {request.venue for request in requests} != {
            route.long_venue,
            route.short_venue,
        }:
            raise ValueError("prepared requests must match the exact route")
        if len({request.client_order_id for request in requests}) != 2:
            raise ValueError("pair legs require distinct client order IDs")
        if len(qualification_hash) != 64:
            raise ValueError("qualification hash must be a SHA-256 hex digest")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            stage_freeze = database.execute(
                "SELECT risk_stage_completion_frozen FROM live_entry_controls WHERE singleton = 1"
            ).fetchone()
            if stage_freeze is not None and bool(stage_freeze["risk_stage_completion_frozen"]):
                database.rollback()
                raise RuntimeError("risk-stage completion freeze blocks new live entry")
            conflicting = database.execute(
                "SELECT pair_action_id FROM live_action_leases WHERE lease_key IN (?, ?) LIMIT 1",
                (f"base:{route.base}", f"route:{route.value}"),
            ).fetchone()
            if conflicting is not None:
                database.rollback()
                raise RuntimeError(
                    f"live route lease is already held by {conflicting['pair_action_id']}"
                )
            active_count = int(
                database.execute(
                    "SELECT count(*) FROM live_pair_actions WHERE state <> ?",
                    (LiveActionState.FLAT.value,),
                ).fetchone()[0]
            )
            if active_count >= MAX_ACTIVE_LIVE_ACTIONS:
                database.rollback()
                raise RuntimeError("maximum active live action limit reached")
            created = now.isoformat()
            database.execute(
                """
                INSERT INTO live_pair_actions (
                    pair_action_id, route_base, long_venue, short_venue, tranche_id,
                    state, risk_reservation_json, qualification_hash, residual_delta,
                    recovery_action, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', NULL, ?, ?)
                """,
                (
                    pair_action_id,
                    route.base,
                    route.long_venue.value,
                    route.short_venue.value,
                    tranche_id,
                    LiveActionState.PREPARED.value,
                    json.dumps(risk_reservation, default=str, sort_keys=True),
                    qualification_hash,
                    created,
                    created,
                ),
            )
            self._acquire_leases_in_transaction(
                database,
                pair_action_id,
                route,
                created,
            )
            for request in requests:
                quantity = intended_base_quantities[request.venue]
                protected = protected_prices[request.venue]
                if quantity <= 0 or protected <= 0:
                    database.rollback()
                    raise ValueError("prepared quantity and protected price must be positive")
                database.execute(
                    """
                    INSERT INTO live_order_legs (
                        client_order_id, pair_action_id, venue, symbol, side,
                        request_payload_hash, intended_base_quantity, protected_price
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.client_order_id,
                        pair_action_id,
                        request.venue.value,
                        request.symbol,
                        request.side.value,
                        request_payload_hash(request),
                        str(quantity),
                        str(protected),
                    ),
                )
            database.execute(
                """
                INSERT INTO live_action_transitions (
                    pair_action_id, from_state, to_state, details_json, observed_at
                ) VALUES (?, NULL, ?, '{}', ?)
                """,
                (pair_action_id, LiveActionState.PREPARED.value, created),
            )
            database.commit()
        action = self._load_sync(pair_action_id)
        if action is None:
            raise RuntimeError("prepared live action was not persisted")
        return action

    async def mark_submit_attempted(
        self,
        pair_action_id: str,
        client_order_ids: tuple[str, ...],
        now: datetime | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._mark_submit_attempted_sync,
            pair_action_id,
            client_order_ids,
            now or datetime.now(UTC),
        )

    def _mark_submit_attempted_sync(
        self,
        pair_action_id: str,
        client_order_ids: tuple[str, ...],
        now: datetime,
    ) -> None:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            action = database.execute(
                "SELECT state FROM live_pair_actions WHERE pair_action_id = ?",
                (pair_action_id,),
            ).fetchone()
            if action is None or LiveActionState(str(action["state"])) != LiveActionState.PREPARED:
                database.rollback()
                raise RuntimeError("only a PREPARED action may start submission")
            rows = database.execute(
                """
                SELECT client_order_id, submit_attempted
                FROM live_order_legs
                WHERE pair_action_id = ?
                """,
                (pair_action_id,),
            ).fetchall()
            expected = {str(row["client_order_id"]) for row in rows}
            if expected != set(client_order_ids) or any(
                int(row["submit_attempted"]) for row in rows
            ):
                database.rollback()
                raise RuntimeError(
                    "client order ID was already submitted or does not match the pair"
                )
            database.execute(
                "UPDATE live_order_legs SET submit_attempted = 1 WHERE pair_action_id = ?",
                (pair_action_id,),
            )
            self._transition_in_transaction(
                database,
                pair_action_id,
                LiveActionState.SUBMITTING,
                {"client_order_ids": client_order_ids},
                now,
            )
            database.commit()

    async def prepare_emergency(
        self,
        pair_action_id: str,
        route: DirectedRouteKey,
        tranche_id: str,
        requests: tuple[VenueOrderRequest, ...],
        intended_base_quantities: dict[str, Decimal],
        risk_reservation: dict[str, Any],
        qualification_hash: str,
        now: datetime | None = None,
    ) -> LiveJournalAction:
        return await asyncio.to_thread(
            self._prepare_emergency_sync,
            pair_action_id,
            route,
            tranche_id,
            requests,
            intended_base_quantities,
            risk_reservation,
            qualification_hash,
            now or datetime.now(UTC),
        )

    def _prepare_emergency_sync(
        self,
        pair_action_id: str,
        route: DirectedRouteKey,
        tranche_id: str,
        requests: tuple[VenueOrderRequest, ...],
        intended_base_quantities: dict[str, Decimal],
        risk_reservation: dict[str, Any],
        qualification_hash: str,
        now: datetime,
    ) -> LiveJournalAction:
        if not requests or len({request.client_order_id for request in requests}) != len(requests):
            raise ValueError("emergency journal requires distinct non-empty order requests")
        self._require_canonical_route_base(route)
        if any(request.order_type != "market" for request in requests):
            raise ValueError("standalone emergency journal only accepts emergency market requests")
        if len(qualification_hash) != 64:
            raise ValueError("qualification hash must be a SHA-256 hex digest")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            active = database.execute(
                "SELECT pair_action_id FROM live_pair_actions WHERE state <> ? LIMIT 1",
                (LiveActionState.FLAT.value,),
            ).fetchone()
            if active is not None:
                database.rollback()
                raise RuntimeError("active live action must be recovered instead of replaced")
            observed = now.isoformat()
            database.execute(
                """
                INSERT INTO live_pair_actions (
                    pair_action_id, route_base, long_venue, short_venue, tranche_id,
                    state, risk_reservation_json, qualification_hash, residual_delta,
                    recovery_action, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', 'EMERGENCY_FLATTEN', ?, ?)
                """,
                (
                    pair_action_id,
                    route.base,
                    route.long_venue.value,
                    route.short_venue.value,
                    tranche_id,
                    LiveActionState.PREPARED.value,
                    json.dumps(risk_reservation, default=str, sort_keys=True),
                    qualification_hash,
                    observed,
                    observed,
                ),
            )
            self._acquire_leases_in_transaction(
                database,
                pair_action_id,
                route,
                observed,
                emergency_exclusive=True,
            )
            for request in requests:
                quantity = intended_base_quantities[request.client_order_id]
                if quantity <= 0:
                    database.rollback()
                    raise ValueError("emergency intended quantity must be positive")
                database.execute(
                    """
                    INSERT INTO live_order_legs (
                        client_order_id, pair_action_id, venue, symbol, side,
                        request_payload_hash, intended_base_quantity, protected_price
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        request.client_order_id,
                        pair_action_id,
                        request.venue.value,
                        request.symbol,
                        request.side.value,
                        request_payload_hash(request),
                        str(quantity),
                    ),
                )
            database.execute(
                """
                INSERT INTO live_action_transitions (
                    pair_action_id, from_state, to_state, details_json, observed_at
                ) VALUES (?, NULL, ?, ?, ?)
                """,
                (
                    pair_action_id,
                    LiveActionState.PREPARED.value,
                    json.dumps({"emergency": True, "orders": len(requests)}),
                    observed,
                ),
            )
            database.commit()
        action = self._load_sync(pair_action_id)
        if action is None:
            raise RuntimeError("emergency action was not durably prepared")
        return action

    async def append_order_leg(
        self,
        pair_action_id: str,
        request: VenueOrderRequest,
        intended_base_quantity: Decimal,
        protected_price: Decimal | None,
    ) -> None:
        await asyncio.to_thread(
            self._append_order_leg_sync,
            pair_action_id,
            request,
            intended_base_quantity,
            protected_price,
        )

    def _append_order_leg_sync(
        self,
        pair_action_id: str,
        request: VenueOrderRequest,
        intended_base_quantity: Decimal,
        protected_price: Decimal | None,
    ) -> None:
        if intended_base_quantity <= 0:
            raise ValueError("journaled recovery/close quantity must be positive")
        if request.order_type == "limit" and (protected_price is None or protected_price <= 0):
            raise ValueError("journaled protected limit price must be positive")
        if request.order_type == "market" and protected_price is not None:
            raise ValueError("unbounded emergency order cannot carry a protected price")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            row = database.execute(
                "SELECT state FROM live_pair_actions WHERE pair_action_id = ?",
                (pair_action_id,),
            ).fetchone()
            if row is None:
                database.rollback()
                raise KeyError(pair_action_id)
            state = LiveActionState(str(row["state"]))
            if state not in {
                LiveActionState.RECOVERING,
                LiveActionState.HEDGED,
                LiveActionState.CLOSING,
                LiveActionState.QUARANTINED,
            }:
                database.rollback()
                raise RuntimeError(
                    "additional legs require recovery, hedged, closing, or quarantine"
                )
            database.execute(
                """
                INSERT INTO live_order_legs (
                    client_order_id, pair_action_id, venue, symbol, side,
                    request_payload_hash, intended_base_quantity, protected_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.client_order_id,
                    pair_action_id,
                    request.venue.value,
                    request.symbol,
                    request.side.value,
                    request_payload_hash(request),
                    str(intended_base_quantity),
                    str(protected_price) if protected_price is not None else None,
                ),
            )
            database.commit()

    async def mark_leg_submit_attempted(
        self,
        pair_action_id: str,
        client_order_id: str,
    ) -> None:
        await asyncio.to_thread(
            self._mark_leg_submit_attempted_sync,
            pair_action_id,
            client_order_id,
        )

    def _mark_leg_submit_attempted_sync(
        self,
        pair_action_id: str,
        client_order_id: str,
    ) -> None:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            row = database.execute(
                """
                SELECT submit_attempted FROM live_order_legs
                WHERE pair_action_id = ? AND client_order_id = ?
                """,
                (pair_action_id, client_order_id),
            ).fetchone()
            if row is None or bool(row["submit_attempted"]):
                database.rollback()
                raise RuntimeError("additional client order ID is missing or already attempted")
            database.execute(
                "UPDATE live_order_legs SET submit_attempted = 1 WHERE client_order_id = ?",
                (client_order_id,),
            )
            database.commit()

    async def transition(
        self,
        pair_action_id: str,
        state: LiveActionState,
        details: dict[str, Any] | None = None,
        *,
        residual_delta: Decimal | None = None,
        recovery_action: str | None = None,
        now: datetime | None = None,
    ) -> LiveJournalAction:
        return await asyncio.to_thread(
            self._transition_sync,
            pair_action_id,
            state,
            details or {},
            residual_delta,
            recovery_action,
            now or datetime.now(UTC),
        )

    def _transition_sync(
        self,
        pair_action_id: str,
        state: LiveActionState,
        details: dict[str, Any],
        residual_delta: Decimal | None,
        recovery_action: str | None,
        now: datetime,
    ) -> LiveJournalAction:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            self._transition_in_transaction(
                database,
                pair_action_id,
                state,
                details,
                now,
                residual_delta,
                recovery_action,
            )
            database.commit()
        action = self._load_sync(pair_action_id)
        if action is None:
            raise RuntimeError("transitioned action disappeared")
        return action

    async def commit_flat_barrier(
        self,
        pair_action_id: str | None,
        expected_event_watermark: int,
        details: dict[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> FlatBarrierCommitResult:
        """Atomically validate the event watermark and commit a terminal FLAT state."""
        batch = await self.commit_flat_barrier_many(
            (pair_action_id,) if pair_action_id is not None else (),
            expected_event_watermark,
            details or {},
            now=now,
        )
        action = next(
            (action for action in batch.actions if action.pair_action_id == pair_action_id),
            None,
        )
        return FlatBarrierCommitResult(batch.committed, action, batch.event_watermark)

    async def commit_flat_barrier_many(
        self,
        pair_action_ids: tuple[str, ...],
        expected_event_watermark: int,
        details: dict[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> FlatBarrierBatchCommitResult:
        """Atomically commit all named actions after one stable-FLAT watermark."""
        if len(set(pair_action_ids)) != len(pair_action_ids):
            raise ValueError("flat barrier action IDs must be unique")
        return await asyncio.to_thread(
            self._commit_flat_barrier_many_sync,
            pair_action_ids,
            expected_event_watermark,
            details or {},
            now or datetime.now(UTC),
        )

    def _commit_flat_barrier_many_sync(
        self,
        pair_action_ids: tuple[str, ...],
        expected_event_watermark: int,
        details: dict[str, Any],
        now: datetime,
    ) -> FlatBarrierBatchCommitResult:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            observed_watermark = self._event_watermark_in_transaction(database)
            active_rows = database.execute(
                "SELECT pair_action_id FROM live_pair_actions WHERE state <> ? "
                "ORDER BY created_at, pair_action_id",
                (LiveActionState.FLAT.value,),
            ).fetchall()
            active_ids = tuple(str(row["pair_action_id"]) for row in active_rows)
            requested_states: dict[str, LiveActionState] = {}
            for pair_action_id in pair_action_ids:
                requested = database.execute(
                    "SELECT state FROM live_pair_actions WHERE pair_action_id = ?",
                    (pair_action_id,),
                ).fetchone()
                if requested is None:
                    database.rollback()
                    raise KeyError(pair_action_id)
                requested_states[pair_action_id] = LiveActionState(str(requested["state"]))
            expected_active_ids = tuple(
                pair_action_id
                for pair_action_id in pair_action_ids
                if requested_states[pair_action_id] != LiveActionState.FLAT
            )
            active_set_matches = set(active_ids) == set(expected_active_ids)
            action_ids = (
                pair_action_ids
                if active_set_matches
                else active_ids
                + tuple(
                    pair_action_id
                    for pair_action_id in pair_action_ids
                    if pair_action_id not in active_ids
                )
            )
            for pair_action_id in action_ids:
                row = database.execute(
                    "SELECT state FROM live_pair_actions WHERE pair_action_id = ?",
                    (pair_action_id,),
                ).fetchone()
                if row is None:
                    database.rollback()
                    raise KeyError(pair_action_id)
                state = LiveActionState(str(row["state"]))
                if not active_set_matches:
                    if state not in {LiveActionState.FLAT, LiveActionState.QUARANTINED}:
                        self._transition_in_transaction(
                            database,
                            pair_action_id,
                            LiveActionState.QUARANTINED,
                            {
                                **details,
                                "reason": "ACTIVE_ACTION_SET_CHANGED",
                                "expected_pair_action_ids": pair_action_ids,
                                "observed_pair_action_ids": active_ids,
                            },
                            now,
                            recovery_action="ACTIVE_ACTION_SET_CHANGED",
                        )
                elif observed_watermark != expected_event_watermark:
                    if state not in {LiveActionState.FLAT, LiveActionState.QUARANTINED}:
                        self._transition_in_transaction(
                            database,
                            pair_action_id,
                            LiveActionState.QUARANTINED,
                            {
                                **details,
                                "reason": "FLAT_BARRIER_EVENT_RACE",
                                "expected_event_watermark": expected_event_watermark,
                                "observed_event_watermark": observed_watermark,
                            },
                            now,
                            recovery_action="FLAT_BARRIER_EVENT_RACE",
                        )
                elif state != LiveActionState.FLAT:
                    self._transition_in_transaction(
                        database,
                        pair_action_id,
                        LiveActionState.FLAT,
                        {**details, "verified": True, "event_watermark": observed_watermark},
                        now,
                        residual_delta=Decimal(0),
                    )
            actions = tuple(
                action
                for pair_action_id in action_ids
                if (action := self._load_in_transaction(database, pair_action_id)) is not None
            )
            final_watermark = self._event_watermark_in_transaction(database)
            committed = (
                observed_watermark == expected_event_watermark == final_watermark
                and active_set_matches
                and all(action.state == LiveActionState.FLAT for action in actions)
                and len(actions) == len(pair_action_ids)
            )
            database.commit()
        return FlatBarrierBatchCommitResult(committed, actions, final_watermark)

    def _transition_in_transaction(
        self,
        database: sqlite3.Connection,
        pair_action_id: str,
        state: LiveActionState,
        details: dict[str, Any],
        now: datetime,
        residual_delta: Decimal | None = None,
        recovery_action: str | None = None,
    ) -> None:
        row = database.execute(
            "SELECT state, residual_delta, recovery_action, route_base, long_venue, "
            "short_venue FROM live_pair_actions "
            "WHERE pair_action_id = ?",
            (pair_action_id,),
        ).fetchone()
        if row is None:
            raise KeyError(pair_action_id)
        previous = LiveActionState(str(row["state"]))
        if state not in _TRANSITIONS[previous]:
            raise ValueError(f"invalid live action transition {previous.value}->{state.value}")
        if previous == LiveActionState.FLAT and state != LiveActionState.FLAT:
            active_count = int(
                database.execute(
                    "SELECT count(*) FROM live_pair_actions WHERE state <> ?",
                    (LiveActionState.FLAT.value,),
                ).fetchone()[0]
            )
            if active_count >= MAX_ACTIVE_LIVE_ACTIONS:
                raise RuntimeError("maximum active live action limit reached")
            route = DirectedRouteKey(
                str(row["route_base"]),
                Venue(str(row["long_venue"])),
                Venue(str(row["short_venue"])),
            )
            self._acquire_leases_in_transaction(
                database,
                pair_action_id,
                route,
                now.isoformat(),
                emergency_exclusive=str(row["recovery_action"] or "") == "EMERGENCY_FLATTEN",
            )
        next_delta = (
            str(residual_delta) if residual_delta is not None else str(row["residual_delta"])
        )
        next_recovery = recovery_action if recovery_action is not None else row["recovery_action"]
        observed = now.isoformat()
        database.execute(
            """
            UPDATE live_pair_actions
            SET state = ?, residual_delta = ?, recovery_action = ?, updated_at = ?
            WHERE pair_action_id = ?
            """,
            (state.value, next_delta, next_recovery, observed, pair_action_id),
        )
        if state == LiveActionState.FLAT:
            database.execute(
                "DELETE FROM live_action_leases WHERE pair_action_id = ?",
                (pair_action_id,),
            )
        database.execute(
            """
            INSERT INTO live_action_transitions (
                pair_action_id, from_state, to_state, details_json, observed_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                pair_action_id,
                previous.value,
                state.value,
                json.dumps(details, default=str, sort_keys=True),
                observed,
            ),
        )

    async def record_order_event(
        self,
        pair_action_id: str,
        order: PrivateOrder,
        event_key: str,
    ) -> bool:
        return await asyncio.to_thread(
            self._record_order_event_sync,
            pair_action_id,
            order,
            event_key,
        )

    def _record_order_event_sync(
        self,
        pair_action_id: str,
        order: PrivateOrder,
        event_key: str,
    ) -> bool:
        if not event_key.strip():
            raise ValueError("private order event key must be non-empty")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            leg = database.execute(
                """
                SELECT leg.venue, leg.symbol, leg.side, leg.intended_base_quantity,
                       leg.filled_base_quantity, leg.order_id, leg.status, leg.last_event_at,
                       action.state AS action_state
                FROM live_order_legs AS leg
                JOIN live_pair_actions AS action
                  ON action.pair_action_id = leg.pair_action_id
                WHERE leg.pair_action_id = ? AND leg.client_order_id = ?
                """,
                (pair_action_id, order.client_order_id),
            ).fetchone()
            if leg is None:
                database.rollback()
                raise KeyError(order.client_order_id)
            payload_json = json.dumps(asdict(order), default=str, sort_keys=True)
            existing_event = database.execute(
                """
                SELECT payload_json FROM live_order_events
                WHERE client_order_id = ? AND event_key = ?
                """,
                (order.client_order_id, event_key),
            ).fetchone()
            if existing_event is not None:
                if str(existing_event["payload_json"]) == payload_json:
                    database.commit()
                    return False
                self._quarantine_event_in_transaction(
                    database,
                    pair_action_id,
                    order.client_order_id,
                    "EVENT_KEY_PAYLOAD_CONFLICT",
                    {"event_key": event_key},
                    order.observed_at,
                )
                database.commit()
                raise JournalEventQuarantinedError("EVENT_KEY_PAYLOAD_CONFLICT")

            previous_status = (
                PrivateOrderStatus(str(leg["status"])) if leg["status"] is not None else None
            )
            previous_filled = Decimal(str(leg["filled_base_quantity"]))
            intended = Decimal(str(leg["intended_base_quantity"]))
            violations: list[str] = []
            if Venue(str(leg["venue"])) != order.venue:
                violations.append("VENUE_MISMATCH")
            if str(leg["symbol"]) != order.symbol:
                violations.append("SYMBOL_MISMATCH")
            if Side(str(leg["side"])) != order.side:
                violations.append("SIDE_MISMATCH")
            if intended != order.requested_base_quantity:
                violations.append("REQUESTED_QUANTITY_MISMATCH")
            if (
                leg["order_id"] is not None
                and order.order_id is not None
                and str(leg["order_id"]) != order.order_id
            ):
                violations.append("EXCHANGE_ORDER_ID_CONFLICT")
            if order.filled_base_quantity < previous_filled:
                violations.append("FILL_REGRESSION")
            if order.filled_base_quantity > intended:
                violations.append("FILL_EXCEEDS_INTENT")
            if order.status not in _ORDER_STATUS_TRANSITIONS[previous_status]:
                violations.append("STATUS_REGRESSION")
            if LiveActionState(str(leg["action_state"])) == LiveActionState.FLAT:
                violations.append("EVENT_AFTER_FLAT")
            if leg["last_event_at"] is not None and order.observed_at < datetime.fromisoformat(
                str(leg["last_event_at"])
            ):
                violations.append("EVENT_TIME_REGRESSION")
            if violations:
                self._quarantine_event_in_transaction(
                    database,
                    pair_action_id,
                    order.client_order_id,
                    "|".join(violations),
                    {"event_key": event_key, "order": asdict(order)},
                    order.observed_at,
                )
                database.commit()
                raise JournalEventQuarantinedError("|".join(violations))

            database.execute(
                """
                INSERT INTO live_order_events (
                    client_order_id, event_key, status, filled_base_quantity,
                    payload_json, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    order.client_order_id,
                    event_key,
                    order.status.value,
                    str(order.filled_base_quantity),
                    payload_json,
                    order.observed_at.isoformat(),
                ),
            )
            database.execute(
                """
                UPDATE live_order_legs
                SET order_id = COALESCE(order_id, ?), status = ?, filled_base_quantity = ?,
                    last_event_at = ?
                WHERE client_order_id = ?
                """,
                (
                    order.order_id,
                    order.status.value,
                    str(order.filled_base_quantity),
                    order.observed_at.isoformat(),
                    order.client_order_id,
                ),
            )
            database.commit()
        return True

    def _quarantine_event_in_transaction(
        self,
        database: sqlite3.Connection,
        pair_action_id: str,
        client_order_id: str,
        code: str,
        payload: dict[str, Any],
        observed_at: datetime,
    ) -> None:
        observed = observed_at.isoformat()
        database.execute(
            """
            INSERT INTO live_journal_audit_events (
                pair_action_id, client_order_id, code, payload_json, observed_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                pair_action_id,
                client_order_id,
                code,
                json.dumps(payload, default=str, sort_keys=True),
                observed,
            ),
        )
        row = database.execute(
            "SELECT state FROM live_pair_actions WHERE pair_action_id = ?",
            (pair_action_id,),
        ).fetchone()
        if row is None:
            raise KeyError(pair_action_id)
        previous = LiveActionState(str(row["state"]))
        if previous not in {LiveActionState.FLAT, LiveActionState.QUARANTINED}:
            database.execute(
                """
                UPDATE live_pair_actions
                SET state = ?, recovery_action = ?, updated_at = ?
                WHERE pair_action_id = ?
                """,
                (
                    LiveActionState.QUARANTINED.value,
                    "JOURNAL_EVENT_IDENTITY_FAILURE",
                    observed,
                    pair_action_id,
                ),
            )
            database.execute(
                """
                INSERT INTO live_action_transitions (
                    pair_action_id, from_state, to_state, details_json, observed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    pair_action_id,
                    previous.value,
                    LiveActionState.QUARANTINED.value,
                    json.dumps({"code": code, "client_order_id": client_order_id}),
                    observed,
                ),
            )

    async def event_watermark(self) -> int:
        return await asyncio.to_thread(self._event_watermark_sync)

    def _event_watermark_sync(self) -> int:
        with self._connect() as database:
            return self._event_watermark_in_transaction(database)

    @staticmethod
    def _event_watermark_in_transaction(database: sqlite3.Connection) -> int:
        order_events = database.execute("SELECT count(*) FROM live_order_events").fetchone()
        audit_events = database.execute("SELECT count(*) FROM live_journal_audit_events").fetchone()
        return int(order_events[0]) + int(audit_events[0])

    async def load(self, pair_action_id: str) -> LiveJournalAction | None:
        return await asyncio.to_thread(self._load_sync, pair_action_id)

    async def active(self) -> LiveJournalAction | None:
        return await asyncio.to_thread(self._active_sync)

    async def active_actions(self) -> tuple[LiveJournalAction, ...]:
        return await asyncio.to_thread(self._active_actions_sync)

    async def completed_actions_since(
        self,
        started_at: datetime,
        qualification_hash: str,
    ) -> tuple[LiveJournalAction, ...]:
        return await asyncio.to_thread(
            self._completed_actions_since_sync,
            started_at,
            qualification_hash,
        )

    async def acquire_account_flatten_lease(self) -> str | None:
        await self.initialise()
        return await asyncio.to_thread(self._acquire_account_flatten_lease_sync)

    def _acquire_account_flatten_lease_sync(self) -> str | None:
        owner_token = secrets.token_hex(32)
        owner_pid = os.getpid()
        owner_process_identity = _process_identity(owner_pid)
        if owner_process_identity is None:
            raise RuntimeError("cannot establish account-flatten process identity")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            existing = database.execute(
                "SELECT owner_token, owner_pid, owner_incarnation, owner_process_identity "
                "FROM live_control_leases "
                "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
            ).fetchone()
            if existing is not None:
                existing_pid = int(existing["owner_pid"])
                existing_incarnation = str(existing["owner_incarnation"])
                existing_process_identity = str(existing["owner_process_identity"])
                if not existing_process_identity:
                    database.rollback()
                    return None
                same_live_process = (
                    existing_pid == owner_pid and existing_incarnation == _PROCESS_INCARCATION
                )
                if same_live_process:
                    database.commit()
                    return str(existing["owner_token"])
                other_live_process = (
                    bool(existing_process_identity)
                    and _process_identity(existing_pid) == existing_process_identity
                )
                if other_live_process:
                    database.rollback()
                    return None
                database.execute(
                    """
                    UPDATE live_control_leases
                    SET owner_token = ?, owner_pid = ?, owner_incarnation = ?,
                        owner_process_identity = ?, acquired_at = ?
                    WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'
                    """,
                    (
                        owner_token,
                        owner_pid,
                        _PROCESS_INCARCATION,
                        owner_process_identity,
                        datetime.now(UTC).isoformat(),
                    ),
                )
                database.commit()
                return owner_token
            database.execute(
                """
                INSERT INTO live_control_leases (
                    lease_key, owner_token, owner_pid, owner_incarnation,
                    owner_process_identity, acquired_at
                ) VALUES ('ACCOUNT_WIDE_FLATTEN', ?, ?, ?, ?, ?)
                """,
                (
                    owner_token,
                    owner_pid,
                    _PROCESS_INCARCATION,
                    owner_process_identity,
                    datetime.now(UTC).isoformat(),
                ),
            )
            database.commit()
        return owner_token

    async def release_account_flatten_lease(self, owner_token: str) -> None:
        await asyncio.to_thread(self._release_account_flatten_lease_sync, owner_token)

    def _release_account_flatten_lease_sync(self, owner_token: str) -> None:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            deleted = database.execute(
                "DELETE FROM live_control_leases "
                "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN' AND owner_token = ?",
                (owner_token,),
            ).rowcount
            if deleted != 1:
                database.rollback()
                raise RuntimeError("account-wide flatten lease ownership was lost")
            database.commit()

    async def known_client_order_ids(self) -> set[str]:
        return await asyncio.to_thread(self._known_client_order_ids_sync)

    def _known_client_order_ids_sync(self) -> set[str]:
        with self._connect() as database:
            rows = database.execute("SELECT client_order_id FROM live_order_legs").fetchall()
        return {str(row["client_order_id"]) for row in rows}

    def _active_sync(self) -> LiveJournalAction | None:
        active = self._active_actions_sync()
        if len(active) > 1:
            raise RuntimeError("multiple live actions require active_actions()")
        return active[0] if active else None

    def _active_actions_sync(self) -> tuple[LiveJournalAction, ...]:
        with self._connect() as database:
            database.execute("BEGIN")
            rows = database.execute(
                """
                SELECT pair_action_id FROM live_pair_actions
                WHERE state <> ?
                ORDER BY created_at, pair_action_id
                """,
                (LiveActionState.FLAT.value,),
            ).fetchall()
            actions = tuple(
                self._load_in_transaction(database, str(row["pair_action_id"])) for row in rows
            )
        return tuple(action for action in actions if action is not None)

    def _completed_actions_since_sync(
        self,
        started_at: datetime,
        qualification_hash: str,
    ) -> tuple[LiveJournalAction, ...]:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("completed action boundary must be timezone-aware")
        with self._connect() as database:
            database.execute("BEGIN")
            rows = database.execute(
                """
                SELECT pair_action_id FROM live_pair_actions
                WHERE state = ? AND created_at >= ? AND qualification_hash = ?
                ORDER BY created_at, pair_action_id
                """,
                (LiveActionState.FLAT.value, started_at.isoformat(), qualification_hash),
            ).fetchall()
            actions = tuple(
                self._load_in_transaction(database, str(row["pair_action_id"])) for row in rows
            )
        return tuple(action for action in actions if action is not None)

    def completed_normal_snapshot_in_transaction(
        self,
        database: sqlite3.Connection,
        started_at: datetime,
        qualification_hash: str,
    ) -> tuple[tuple[str, ...], str]:
        rows = database.execute(
            "SELECT pair_action_id FROM live_pair_actions "
            "WHERE state = ? AND created_at >= ? AND qualification_hash = ? "
            "ORDER BY pair_action_id",
            (LiveActionState.FLAT.value, started_at.isoformat(), qualification_hash),
        ).fetchall()
        actions = tuple(
            action
            for row in rows
            if (action := self._load_in_transaction(database, str(row["pair_action_id"])))
            is not None
        )
        return (
            tuple(action.pair_action_id for action in actions),
            completed_normal_actions_sha256(actions),
        )

    @staticmethod
    def _require_canonical_route_base(route: DirectedRouteKey) -> None:
        if route.base != route.base.strip().upper():
            raise ValueError("route base must be canonical uppercase")

    @staticmethod
    def _acquire_leases_in_transaction(
        database: sqlite3.Connection,
        pair_action_id: str,
        route: DirectedRouteKey,
        acquired_at: str,
        *,
        idempotent: bool = False,
        emergency_exclusive: bool = False,
    ) -> None:
        flatten_owner = database.execute(
            "SELECT owner_token FROM live_control_leases WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
        ).fetchone()
        if flatten_owner is not None and not emergency_exclusive and not idempotent:
            raise RuntimeError("account-wide flatten is in progress")
        emergency_owner = database.execute(
            "SELECT pair_action_id FROM live_action_leases WHERE lease_key = 'global:emergency'"
        ).fetchone()
        if emergency_owner is not None and str(emergency_owner["pair_action_id"]) != pair_action_id:
            raise RuntimeError("global emergency live action lease is already held")
        if emergency_exclusive:
            other_active = database.execute(
                "SELECT pair_action_id FROM live_pair_actions "
                "WHERE state <> ? AND pair_action_id <> ? LIMIT 1",
                (LiveActionState.FLAT.value, pair_action_id),
            ).fetchone()
            if other_active is not None:
                raise RuntimeError(
                    "global emergency live action requires exclusive active ownership"
                )
        leases = [
            (f"base:{route.base.strip().upper()}", "BASE"),
            (f"route:{route.value}", "ROUTE"),
        ]
        if emergency_exclusive:
            leases.insert(0, ("global:emergency", "ROUTE"))
        for lease_key, lease_kind in leases:
            if idempotent:
                database.execute(
                    """
                    INSERT OR IGNORE INTO live_action_leases (
                        lease_key, lease_kind, pair_action_id, acquired_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (lease_key, lease_kind, pair_action_id, acquired_at),
                )
            else:
                database.execute(
                    """
                    INSERT OR IGNORE INTO live_action_leases (
                        lease_key, lease_kind, pair_action_id, acquired_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (lease_key, lease_kind, pair_action_id, acquired_at),
                )
            owner = database.execute(
                "SELECT pair_action_id FROM live_action_leases WHERE lease_key = ?",
                (lease_key,),
            ).fetchone()
            if owner is None or str(owner["pair_action_id"]) != pair_action_id:
                raise RuntimeError(f"live action lease conflict: {lease_key}")

    def _load_sync(self, pair_action_id: str) -> LiveJournalAction | None:
        with self._connect() as database:
            return self._load_in_transaction(database, pair_action_id)

    def _load_in_transaction(
        self,
        database: sqlite3.Connection,
        pair_action_id: str,
    ) -> LiveJournalAction | None:
        row = database.execute(
            "SELECT * FROM live_pair_actions WHERE pair_action_id = ?",
            (pair_action_id,),
        ).fetchone()
        if row is None:
            return None
        legs = database.execute(
            """
            SELECT * FROM live_order_legs
            WHERE pair_action_id = ?
            ORDER BY venue, client_order_id
            """,
            (pair_action_id,),
        ).fetchall()
        return LiveJournalAction(
            pair_action_id=str(row["pair_action_id"]),
            route=DirectedRouteKey(
                str(row["route_base"]),
                Venue(str(row["long_venue"])),
                Venue(str(row["short_venue"])),
            ),
            tranche_id=str(row["tranche_id"]),
            state=LiveActionState(str(row["state"])),
            risk_reservation=json.loads(str(row["risk_reservation_json"])),
            qualification_hash=str(row["qualification_hash"]),
            residual_delta=Decimal(str(row["residual_delta"])),
            recovery_action=(
                str(row["recovery_action"]) if row["recovery_action"] is not None else None
            ),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            legs=tuple(
                JournalLeg(
                    client_order_id=str(leg["client_order_id"]),
                    venue=Venue(str(leg["venue"])),
                    symbol=str(leg["symbol"]),
                    side=Side(str(leg["side"])),
                    request_payload_hash=str(leg["request_payload_hash"]),
                    intended_base_quantity=Decimal(str(leg["intended_base_quantity"])),
                    protected_price=(
                        Decimal(str(leg["protected_price"]))
                        if leg["protected_price"] is not None
                        else None
                    ),
                    submit_attempted=bool(leg["submit_attempted"]),
                    order_id=str(leg["order_id"]) if leg["order_id"] is not None else None,
                    status=(
                        PrivateOrderStatus(str(leg["status"]))
                        if leg["status"] is not None
                        else None
                    ),
                    filled_base_quantity=Decimal(str(leg["filled_base_quantity"])),
                )
                for leg in legs
            ),
        )


def _process_identity(process_id: int) -> str | None:
    if process_id <= 0:
        return None
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return None
    except PermissionError:
        pass
    except OSError as error:
        if getattr(error, "winerror", None) in {87, 1168}:
            return None
    if os.name == "nt":
        return _windows_process_identity(process_id)
    try:
        stat = (Path("/proc") / str(process_id) / "stat").read_text(encoding="utf-8")
        fields = stat[stat.rfind(")") + 2 :].split()
        return f"proc:{process_id}:{fields[19]}"
    except (OSError, IndexError):
        return None


def _windows_process_identity(process_id: int) -> str | None:
    import ctypes
    from ctypes import wintypes

    loader = getattr(ctypes, "windll", None)
    if loader is None:
        return None
    kernel32 = loader.kernel32
    process = kernel32.OpenProcess(0x1000, False, process_id)
    if not process:
        return None
    try:
        created = wintypes.FILETIME()
        exited = wintypes.FILETIME()
        kernel = wintypes.FILETIME()
        user = wintypes.FILETIME()
        if not kernel32.GetProcessTimes(
            process,
            ctypes.byref(created),
            ctypes.byref(exited),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return None
        ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
        return f"win:{process_id}:{ticks}"
    finally:
        kernel32.CloseHandle(process)
