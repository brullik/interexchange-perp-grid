from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_domain import (
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.strategy import DirectedRouteKey


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
                """
            )
            columns = {
                str(row[1]) for row in database.execute("PRAGMA table_info(live_order_legs)")
            }
            if "last_event_at" not in columns:
                database.execute("ALTER TABLE live_order_legs ADD COLUMN last_event_at TEXT")

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
            active = database.execute(
                "SELECT pair_action_id, state FROM live_pair_actions WHERE state <> ? LIMIT 1",
                (LiveActionState.FLAT.value,),
            ).fetchone()
            if active is not None:
                database.rollback()
                active_identity = f"{active['pair_action_id']}:{active['state']}"
                raise RuntimeError(f"unreconciled live action blocks entry: {active_identity}")
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
            "SELECT state, residual_delta, recovery_action FROM live_pair_actions "
            "WHERE pair_action_id = ?",
            (pair_action_id,),
        ).fetchone()
        if row is None:
            raise KeyError(pair_action_id)
        previous = LiveActionState(str(row["state"]))
        if state not in _TRANSITIONS[previous]:
            raise ValueError(f"invalid live action transition {previous.value}->{state.value}")
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
        if previous != LiveActionState.QUARANTINED:
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
            order_events = database.execute("SELECT count(*) FROM live_order_events").fetchone()
            audit_events = database.execute(
                "SELECT count(*) FROM live_journal_audit_events"
            ).fetchone()
        return int(order_events[0]) + int(audit_events[0])

    async def load(self, pair_action_id: str) -> LiveJournalAction | None:
        return await asyncio.to_thread(self._load_sync, pair_action_id)

    async def active(self) -> LiveJournalAction | None:
        return await asyncio.to_thread(self._active_sync)

    async def known_client_order_ids(self) -> set[str]:
        return await asyncio.to_thread(self._known_client_order_ids_sync)

    def _known_client_order_ids_sync(self) -> set[str]:
        with self._connect() as database:
            rows = database.execute("SELECT client_order_id FROM live_order_legs").fetchall()
        return {str(row["client_order_id"]) for row in rows}

    def _active_sync(self) -> LiveJournalAction | None:
        with self._connect() as database:
            row = database.execute(
                """
                SELECT pair_action_id FROM live_pair_actions
                WHERE state <> ?
                ORDER BY created_at LIMIT 1
                """,
                (LiveActionState.FLAT.value,),
            ).fetchone()
        return self._load_sync(str(row["pair_action_id"])) if row is not None else None

    def _load_sync(self, pair_action_id: str) -> LiveJournalAction | None:
        with self._connect() as database:
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
