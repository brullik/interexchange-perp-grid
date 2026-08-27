from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, DecimalException
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
_DEPLOYMENT_UPGRADE_LEASE_PREFIX = "deployment-upgrade-"


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


MAX_ACTIVE_LIVE_ROUTES = 10
MAX_TRANCHES_PER_LIVE_ROUTE = 5
MAX_ACTIVE_LIVE_ACTIONS = MAX_ACTIVE_LIVE_ROUTES * MAX_TRANCHES_PER_LIVE_ROUTE
MAX_LIVE_ROUTE_STRESS_USDT = Decimal("5")
MAX_LIVE_PORTFOLIO_STRESS_USDT = Decimal("50")


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
    activation_hash: str | None = None


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


@dataclass(frozen=True, slots=True)
class DeploymentUpgradeGate:
    entry_frozen: bool
    active_action_count: int
    updated_at: datetime


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
                    emergency_exclusive INTEGER NOT NULL DEFAULT 0
                        CHECK (emergency_exclusive IN (0, 1)),
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
                CREATE TABLE IF NOT EXISTS fast_live_preflight_consumption (
                    preflight_sha256 TEXT PRIMARY KEY,
                    activation_hash TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    consumed_by_pair_action_id TEXT NOT NULL UNIQUE,
                    consumed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fast_live_preflight_issuance (
                    preflight_sha256 TEXT PRIMARY KEY,
                    expires_at TEXT NOT NULL,
                    issued_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_deployment_controls (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    upgrade_entry_frozen INTEGER NOT NULL DEFAULT 0
                        CHECK (upgrade_entry_frozen IN (0, 1)),
                    previous_risk_stage_completion_frozen INTEGER NOT NULL DEFAULT 0
                        CHECK (previous_risk_stage_completion_frozen IN (0, 1)),
                    owner_token TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                """
            )
            database.execute(
                "INSERT OR IGNORE INTO live_entry_controls("
                "singleton, risk_stage_completion_frozen) VALUES (1, 0)"
            )
            database.execute(
                "INSERT OR IGNORE INTO live_deployment_controls("
                "singleton, upgrade_entry_frozen, previous_risk_stage_completion_frozen, "
                "updated_at) VALUES (1, 0, 0, ?)",
                (datetime.now(UTC).isoformat(),),
            )
            deployment_columns = {
                str(row[1])
                for row in database.execute("PRAGMA table_info(live_deployment_controls)")
            }
            if "previous_risk_stage_completion_frozen" not in deployment_columns:
                database.execute(
                    "ALTER TABLE live_deployment_controls ADD COLUMN "
                    "previous_risk_stage_completion_frozen INTEGER NOT NULL DEFAULT 0"
                )
            if "owner_token" not in deployment_columns:
                database.execute(
                    "ALTER TABLE live_deployment_controls ADD COLUMN "
                    "owner_token TEXT NOT NULL DEFAULT ''"
                )
            columns = {
                str(row[1]) for row in database.execute("PRAGMA table_info(live_order_legs)")
            }
            if "last_event_at" not in columns:
                database.execute("ALTER TABLE live_order_legs ADD COLUMN last_event_at TEXT")
            action_columns = {
                str(row[1]) for row in database.execute("PRAGMA table_info(live_pair_actions)")
            }
            if "emergency_exclusive" not in action_columns:
                database.execute(
                    "ALTER TABLE live_pair_actions ADD COLUMN emergency_exclusive "
                    "INTEGER NOT NULL DEFAULT 0"
                )
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
                initial_transitions = database.execute(
                    "SELECT pair_action_id, details_json FROM live_action_transitions "
                    "WHERE from_state IS NULL"
                ).fetchall()
                authoritative_emergency_ids: set[str] = set()
                for transition in initial_transitions:
                    try:
                        details = json.loads(str(transition["details_json"]))
                    except (TypeError, ValueError):
                        continue
                    if isinstance(details, dict) and details.get("emergency") is True:
                        pair_action_id = str(transition["pair_action_id"])
                        authoritative_emergency_ids.add(pair_action_id)
                        database.execute(
                            "UPDATE live_pair_actions SET emergency_exclusive = 1 "
                            "WHERE pair_action_id = ?",
                            (pair_action_id,),
                        )
                active_rows = database.execute(
                    """
                SELECT pair_action_id, route_base, long_venue, short_venue, tranche_id,
                       risk_reservation_json, created_at, recovery_action,
                       emergency_exclusive
                    FROM live_pair_actions WHERE state <> ? ORDER BY created_at, pair_action_id
                    """,
                    (LiveActionState.FLAT.value,),
                ).fetchall()
                if any(
                    bool(row["emergency_exclusive"])
                    != (str(row["pair_action_id"]) in authoritative_emergency_ids)
                    for row in active_rows
                ):
                    raise RuntimeError("legacy emergency live action identity is inconsistent")
                self._validate_active_portfolio_rows(active_rows)
                database.execute("DELETE FROM live_action_leases")
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
                        emergency_exclusive=bool(row["emergency_exclusive"]),
                    )
                database.commit()
            except Exception:
                database.rollback()
                raise

    async def arm_deployment_upgrade(
        self,
        owner_token: str,
        now: datetime | None = None,
    ) -> DeploymentUpgradeGate:
        return await asyncio.to_thread(
            self._set_deployment_upgrade_gate_sync,
            True,
            owner_token,
            now or datetime.now(UTC),
        )

    async def arm_legacy_deployment_upgrade(
        self,
        owner_token: str,
        now: datetime | None = None,
    ) -> DeploymentUpgradeGate:
        """Arm only controls understood by the currently deployed legacy image."""
        return await asyncio.to_thread(
            self._arm_legacy_deployment_upgrade_sync,
            owner_token,
            now or datetime.now(UTC),
        )

    def _arm_legacy_deployment_upgrade_sync(
        self,
        owner_token: str,
        now: datetime,
    ) -> DeploymentUpgradeGate:
        self._validate_deployment_upgrade_owner(owner_token)
        observed = now.isoformat()
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            existing_lease = database.execute(
                "SELECT owner_token FROM live_control_leases "
                "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
            ).fetchone()
            if existing_lease is None:
                database.execute(
                    "INSERT INTO live_control_leases("
                    "lease_key, owner_token, owner_pid, owner_incarnation, "
                    "owner_process_identity, acquired_at) "
                    "VALUES ('ACCOUNT_WIDE_FLATTEN', ?, 0, ?, ?, ?)",
                    (owner_token, owner_token, owner_token, observed),
                )
            elif str(existing_lease["owner_token"]) != owner_token:
                database.rollback()
                raise RuntimeError("account-wide recovery already owns the live entry gate")
            active_action_count = int(
                database.execute(
                    "SELECT COUNT(*) FROM live_pair_actions WHERE state <> ?",
                    (LiveActionState.FLAT.value,),
                ).fetchone()[0]
            )
            database.commit()
        return DeploymentUpgradeGate(True, active_action_count, now)

    async def release_deployment_upgrade(
        self,
        owner_token: str,
        now: datetime | None = None,
    ) -> DeploymentUpgradeGate:
        return await asyncio.to_thread(
            self._set_deployment_upgrade_gate_sync,
            False,
            owner_token,
            now or datetime.now(UTC),
        )

    def _set_deployment_upgrade_gate_sync(
        self,
        frozen: bool,
        owner_token: str,
        now: datetime,
    ) -> DeploymentUpgradeGate:
        self._validate_deployment_upgrade_owner(owner_token)
        observed = now.isoformat()
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            gate = database.execute(
                "SELECT upgrade_entry_frozen, owner_token "
                "FROM live_deployment_controls WHERE singleton = 1"
            ).fetchone()
            if gate is None:
                database.rollback()
                raise RuntimeError("deployment upgrade gate state is unavailable")
            active_action_count = int(
                database.execute(
                    "SELECT COUNT(*) FROM live_pair_actions WHERE state <> ?",
                    (LiveActionState.FLAT.value,),
                ).fetchone()[0]
            )
            currently_frozen = bool(gate["upgrade_entry_frozen"])
            if frozen and not currently_frozen:
                existing_lease = database.execute(
                    "SELECT owner_token FROM live_control_leases "
                    "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
                ).fetchone()
                if existing_lease is None:
                    database.execute(
                        "INSERT INTO live_control_leases("
                        "lease_key, owner_token, owner_pid, owner_incarnation, "
                        "owner_process_identity, acquired_at) "
                        "VALUES ('ACCOUNT_WIDE_FLATTEN', ?, 0, ?, ?, ?)",
                        (owner_token, owner_token, owner_token, observed),
                    )
                elif str(existing_lease["owner_token"]) != owner_token:
                    database.rollback()
                    raise RuntimeError("account-wide recovery already owns the live entry gate")
                database.execute(
                    "UPDATE live_deployment_controls "
                    "SET upgrade_entry_frozen = 1, owner_token = ?, updated_at = ? "
                    "WHERE singleton = 1",
                    (owner_token, observed),
                )
            elif frozen:
                lease = database.execute(
                    "SELECT owner_token FROM live_control_leases "
                    "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
                ).fetchone()
                if (
                    str(gate["owner_token"]) != owner_token
                    or lease is None
                    or str(lease["owner_token"]) != owner_token
                ):
                    database.rollback()
                    raise RuntimeError("deployment upgrade legacy gate ownership is unavailable")
            elif not frozen:
                if active_action_count:
                    database.rollback()
                    raise RuntimeError(
                        "deployment upgrade release requires zero active live actions"
                    )
                lease = database.execute(
                    "SELECT owner_token FROM live_control_leases "
                    "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
                ).fetchone()
                if (
                    not currently_frozen
                    or str(gate["owner_token"]) != owner_token
                    or lease is None
                    or str(lease["owner_token"]) != owner_token
                ):
                    database.rollback()
                    raise RuntimeError("deployment upgrade gate ownership is unavailable")
                database.execute(
                    "DELETE FROM live_control_leases "
                    "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN' AND owner_token = ?",
                    (owner_token,),
                )
                database.execute(
                    "UPDATE live_deployment_controls "
                    "SET upgrade_entry_frozen = 0, owner_token = '', "
                    "previous_risk_stage_completion_frozen = 0, updated_at = ? "
                    "WHERE singleton = 1",
                    (observed,),
                )
            database.commit()
        return DeploymentUpgradeGate(frozen, active_action_count, now)

    @staticmethod
    def _validate_deployment_upgrade_owner(owner_token: str) -> None:
        if (
            not owner_token.startswith(_DEPLOYMENT_UPGRADE_LEASE_PREFIX)
            or len(owner_token) != len(_DEPLOYMENT_UPGRADE_LEASE_PREFIX) + 40
            or any(character not in "0123456789abcdef" for character in owner_token[-40:])
        ):
            raise ValueError("deployment upgrade owner token must bind one full release SHA")

    @staticmethod
    def _require_deployment_entry_open(database: sqlite3.Connection) -> None:
        deployment_freeze = database.execute(
            "SELECT upgrade_entry_frozen FROM live_deployment_controls WHERE singleton = 1"
        ).fetchone()
        if deployment_freeze is None or bool(deployment_freeze["upgrade_entry_frozen"]):
            raise RuntimeError("deployment upgrade freeze blocks new live action")

    async def issue_fast_live_preflight(
        self,
        preflight_sha256: str,
        expires_at: datetime,
        *,
        now: datetime | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._issue_fast_live_preflight_sync,
            preflight_sha256,
            expires_at,
            now or datetime.now(UTC),
        )

    def _issue_fast_live_preflight_sync(
        self,
        preflight_sha256: str,
        expires_at: datetime,
        now: datetime,
    ) -> None:
        if (
            len(preflight_sha256) != 64
            or any(character not in "0123456789abcdef" for character in preflight_sha256)
            or now.tzinfo is None
            or now.utcoffset() is None
            or expires_at.tzinfo is None
            or expires_at.utcoffset() is None
            or expires_at != now + timedelta(seconds=600)
        ):
            raise ValueError("fast-live preflight issuance identity is invalid")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            consumed = database.execute(
                "SELECT 1 FROM fast_live_preflight_consumption WHERE preflight_sha256 = ?",
                (preflight_sha256,),
            ).fetchone()
            if consumed is not None:
                database.rollback()
                raise RuntimeError("consumed fast-live preflight cannot be reissued")
            try:
                database.execute(
                    "INSERT INTO fast_live_preflight_issuance("
                    "preflight_sha256, expires_at, issued_at) VALUES (?, ?, ?)",
                    (preflight_sha256, expires_at.isoformat(), now.isoformat()),
                )
            except sqlite3.IntegrityError as error:
                database.rollback()
                raise RuntimeError("fast-live preflight was already issued") from error
            database.commit()

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
        *,
        activation_hash: str | None = None,
        fast_live_preflight_sha256: str | None = None,
        fast_live_preflight_expires_at: datetime | None = None,
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
            activation_hash,
            fast_live_preflight_sha256,
            fast_live_preflight_expires_at,
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
        activation_hash: str | None,
        fast_live_preflight_sha256: str | None,
        fast_live_preflight_expires_at: datetime | None,
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
        if len(qualification_hash) != 64 or any(
            character not in "0123456789abcdef" for character in qualification_hash
        ):
            raise ValueError("qualification hash must be a SHA-256 hex digest")
        fast_live_fields = (
            activation_hash,
            fast_live_preflight_sha256,
            fast_live_preflight_expires_at,
        )
        if any(value is not None for value in fast_live_fields):
            if (
                activation_hash is None
                or fast_live_preflight_sha256 is None
                or fast_live_preflight_expires_at is None
                or len(activation_hash) != 64
                or len(fast_live_preflight_sha256) != 64
                or activation_hash != fast_live_preflight_sha256
                or any(
                    character not in "0123456789abcdef"
                    for character in activation_hash + fast_live_preflight_sha256
                )
                or fast_live_preflight_expires_at.tzinfo is None
                or fast_live_preflight_expires_at.utcoffset() is None
                or now.tzinfo is None
                or now.utcoffset() is None
                or not isinstance(risk_reservation, dict)
                or risk_reservation.get("activation_hash") != activation_hash
                or risk_reservation.get("fast_live_preflight_expires_at")
                != fast_live_preflight_expires_at.isoformat()
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(risk_reservation.get("consumption_data_generation_sha256", "")),
                )
                is None
            ):
                raise ValueError("fast-live activation identity is incomplete")
            if now < datetime.fromtimestamp(0, UTC) or now > fast_live_preflight_expires_at:
                raise RuntimeError("fast-live preflight expired before journal prepare")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            stage_freeze = database.execute(
                "SELECT risk_stage_completion_frozen FROM live_entry_controls WHERE singleton = 1"
            ).fetchone()
            if stage_freeze is not None and bool(stage_freeze["risk_stage_completion_frozen"]):
                database.rollback()
                raise RuntimeError("risk-stage completion freeze blocks new live entry")
            try:
                self._require_deployment_entry_open(database)
            except RuntimeError:
                database.rollback()
                raise
            self._validate_new_tranche_in_transaction(
                database,
                route,
                tranche_id,
                risk_reservation,
            )
            created = now.isoformat()
            if fast_live_preflight_sha256 is not None:
                assert activation_hash is not None
                assert fast_live_preflight_expires_at is not None
                issuance = database.execute(
                    "SELECT expires_at, issued_at FROM fast_live_preflight_issuance "
                    "WHERE preflight_sha256 = ?",
                    (fast_live_preflight_sha256,),
                ).fetchone()
                if (
                    issuance is None
                    or str(issuance["expires_at"]) != fast_live_preflight_expires_at.isoformat()
                    or datetime.fromisoformat(str(issuance["issued_at"])) > now
                ):
                    database.rollback()
                    raise RuntimeError("fast-live preflight was not durably issued")
                try:
                    database.execute(
                        "INSERT INTO fast_live_preflight_consumption("
                        "preflight_sha256, activation_hash, expires_at, "
                        "consumed_by_pair_action_id, consumed_at) VALUES (?, ?, ?, ?, ?)",
                        (
                            fast_live_preflight_sha256,
                            activation_hash,
                            fast_live_preflight_expires_at.isoformat(),
                            pair_action_id,
                            created,
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    database.rollback()
                    raise RuntimeError("fast-live preflight was already consumed") from error
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
            prepare_details = (
                {
                    "fast_live_preflight_sha256": fast_live_preflight_sha256,
                    "consumption_data_generation_sha256": risk_reservation[
                        "consumption_data_generation_sha256"
                    ],
                }
                if fast_live_preflight_sha256 is not None
                else {}
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
                    json.dumps(prepare_details, default=str, sort_keys=True),
                    created,
                ),
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
            try:
                self._require_deployment_entry_open(database)
            except RuntimeError:
                database.rollback()
                raise
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
                    recovery_action, emergency_exclusive, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', 'EMERGENCY_FLATTEN', 1, ?, ?)
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

    async def commit_reconciled_action(
        self,
        pair_action_id: str,
        expected_active_pair_action_ids: tuple[str, ...],
        expected_event_watermark: int,
        reconciliation_position_audit_sha256: str,
        details: dict[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> FlatBarrierCommitResult:
        """Release one closed tranche only while the aggregate active set remains unchanged."""
        if pair_action_id not in expected_active_pair_action_ids:
            raise ValueError("reconciled action must belong to the expected active set")
        if len(set(expected_active_pair_action_ids)) != len(expected_active_pair_action_ids):
            raise ValueError("expected active action IDs must be unique")
        if len(reconciliation_position_audit_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in reconciliation_position_audit_sha256
        ):
            raise ValueError("reconciliation position audit must be a lowercase SHA-256")
        return await asyncio.to_thread(
            self._commit_reconciled_action_sync,
            pair_action_id,
            expected_active_pair_action_ids,
            expected_event_watermark,
            {
                **(details or {}),
                "reconciliation_position_audit_sha256": (reconciliation_position_audit_sha256),
            },
            now or datetime.now(UTC),
        )

    def _commit_reconciled_action_sync(
        self,
        pair_action_id: str,
        expected_active_pair_action_ids: tuple[str, ...],
        expected_event_watermark: int,
        details: dict[str, Any],
        now: datetime,
    ) -> FlatBarrierCommitResult:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            observed_watermark = self._event_watermark_in_transaction(database)
            active_ids = tuple(
                str(row["pair_action_id"])
                for row in database.execute(
                    "SELECT pair_action_id FROM live_pair_actions WHERE state <> ? "
                    "ORDER BY created_at, pair_action_id",
                    (LiveActionState.FLAT.value,),
                ).fetchall()
            )
            action = self._load_in_transaction(database, pair_action_id)
            if action is None:
                database.rollback()
                raise KeyError(pair_action_id)
            if (
                active_ids != expected_active_pair_action_ids
                or observed_watermark != expected_event_watermark
                or action.state not in {LiveActionState.CLOSING, LiveActionState.RECOVERING}
            ):
                database.rollback()
                return FlatBarrierCommitResult(False, action, observed_watermark)
            self._transition_in_transaction(
                database,
                pair_action_id,
                LiveActionState.FLAT,
                {**details, "portfolio_reconciled": True},
                now,
                residual_delta=Decimal(0),
            )
            committed = self._load_in_transaction(database, pair_action_id)
            database.commit()
        return FlatBarrierCommitResult(True, committed, observed_watermark)

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
            "short_venue, tranche_id, risk_reservation_json, emergency_exclusive "
            "FROM live_pair_actions "
            "WHERE pair_action_id = ?",
            (pair_action_id,),
        ).fetchone()
        if row is None:
            raise KeyError(pair_action_id)
        previous = LiveActionState(str(row["state"]))
        if state not in _TRANSITIONS[previous]:
            raise ValueError(f"invalid live action transition {previous.value}->{state.value}")
        if previous == LiveActionState.FLAT and state != LiveActionState.FLAT:
            self._require_deployment_entry_open(database)
            route = DirectedRouteKey(
                str(row["route_base"]),
                Venue(str(row["long_venue"])),
                Venue(str(row["short_venue"])),
            )
            self._validate_new_tranche_in_transaction(
                database,
                route,
                str(row["tranche_id"]),
                None
                if bool(row["emergency_exclusive"])
                else json.loads(str(row["risk_reservation_json"])),
            )
            self._acquire_leases_in_transaction(
                database,
                pair_action_id,
                route,
                now.isoformat(),
                emergency_exclusive=bool(row["emergency_exclusive"]),
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

    async def latest_order_events(self, pair_action_id: str) -> tuple[PrivateOrder, ...]:
        return await asyncio.to_thread(self._latest_order_events_sync, pair_action_id)

    def _latest_order_events_sync(self, pair_action_id: str) -> tuple[PrivateOrder, ...]:
        with self._connect() as database:
            rows = database.execute(
                """
                SELECT event.client_order_id, event.payload_json, event.observed_at,
                       event.rowid AS event_rowid
                FROM live_order_events AS event
                JOIN live_order_legs AS leg
                  ON leg.client_order_id = event.client_order_id
                WHERE leg.pair_action_id = ?
                ORDER BY event.observed_at, event_rowid
                """,
                (pair_action_id,),
            ).fetchall()
        latest: dict[str, PrivateOrder] = {}
        for row in rows:
            latest[str(row["client_order_id"])] = _private_order_from_json(str(row["payload_json"]))
        return tuple(latest[key] for key in sorted(latest))

    async def update_final_opening_reserves(
        self,
        pair_action_id: str,
        components: dict[str, Decimal],
        *,
        data_generation_sha256: str,
        now: datetime | None = None,
    ) -> LiveJournalAction:
        """Persist component-wise conservative V2 reserves before any transport submit."""
        allowed = {
            "initial_adverse_funding_reserve_usdt",
            "initial_remaining_close_fees_usdt",
            "initial_measured_book_impact_usdt",
            "initial_total_reserves_usdt",
        }
        if (
            set(components) != allowed
            or any(not value.is_finite() or value < 0 for value in components.values())
            or re.fullmatch(r"[0-9a-f]{64}", data_generation_sha256) is None
        ):
            raise ValueError("final opening reserve components are invalid")
        return await asyncio.to_thread(
            self._update_final_opening_reserves_sync,
            pair_action_id,
            components,
            data_generation_sha256,
            now or datetime.now(UTC),
        )

    def _update_final_opening_reserves_sync(
        self,
        pair_action_id: str,
        components: dict[str, Decimal],
        data_generation_sha256: str,
        now: datetime,
    ) -> LiveJournalAction:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            action = self._load_in_transaction(database, pair_action_id)
            if (
                action is None
                or action.state not in {LiveActionState.PREPARED, LiveActionState.SUBMITTING}
                or action.risk_reservation.get("strategy") != "AGGRESSIVE_FAST_LIVE_V2"
            ):
                database.rollback()
                raise RuntimeError("final opening reserves require one pre-submit V2 action")
            observed_events = database.execute(
                "SELECT COUNT(*) FROM live_order_events AS event "
                "JOIN live_order_legs AS leg ON leg.client_order_id = event.client_order_id "
                "WHERE leg.pair_action_id = ?",
                (pair_action_id,),
            ).fetchone()
            if observed_events is None or int(observed_events[0]) != 0:
                database.rollback()
                raise RuntimeError("final opening reserves cannot change after an order event")
            reservation = dict(action.risk_reservation)
            for key, current in components.items():
                previous = Decimal(str(reservation[key]))
                if not previous.is_finite() or previous < 0:
                    database.rollback()
                    raise RuntimeError("stored opening reserve component is invalid")
                reservation[key] = str(max(previous, current))
            raw_history = reservation.get("opening_data_generation_sha256_history", [])
            if not isinstance(raw_history, list) or any(
                re.fullmatch(r"[0-9a-f]{64}", str(value)) is None for value in raw_history
            ):
                database.rollback()
                raise RuntimeError("stored opening data-generation history is invalid")
            history = [str(value) for value in raw_history]
            history.append(data_generation_sha256)
            reservation["opening_data_generation_sha256_history"] = history
            reservation["latest_opening_data_generation_sha256"] = data_generation_sha256
            database.execute(
                "UPDATE live_pair_actions SET risk_reservation_json = ?, updated_at = ? "
                "WHERE pair_action_id = ?",
                (
                    json.dumps(reservation, default=str, sort_keys=True),
                    now.isoformat(),
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
                    action.state.value,
                    action.state.value,
                    json.dumps(
                        {
                            "final_opening_reserves": components,
                            "data_generation_sha256": data_generation_sha256,
                        },
                        default=str,
                        sort_keys=True,
                    ),
                    now.isoformat(),
                ),
            )
            database.commit()
        updated = self._load_sync(pair_action_id)
        if updated is None:
            raise RuntimeError("updated V2 action disappeared")
        return updated

    async def update_actual_risk(
        self,
        pair_action_id: str,
        expected_event_watermark: int,
        actual_risk: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> LiveJournalAction:
        return await asyncio.to_thread(
            self._update_actual_risk_sync,
            pair_action_id,
            expected_event_watermark,
            actual_risk,
            now or datetime.now(UTC),
        )

    def _update_actual_risk_sync(
        self,
        pair_action_id: str,
        expected_event_watermark: int,
        actual_risk: dict[str, Any],
        now: datetime,
    ) -> LiveJournalAction:
        required = {
            "incremental_stress_usdt",
            "route_total_usdt",
            "portfolio_total_usdt",
            "actual_entry_spread_bps",
            "fill_event_watermark",
        }
        breakdown_fields = {
            "actual_open_fees_usdt",
            "remaining_close_fees_usdt",
            "initial_measured_book_impact_usdt",
            "adverse_funding_usdt",
            "other_reserves_usdt",
        }
        supplied = set(actual_risk)
        if not required.issubset(supplied) or not supplied.issubset(required | breakdown_fields):
            raise ValueError("actual risk snapshot fields are invalid")
        incremental = Decimal(str(actual_risk["incremental_stress_usdt"]))
        actual_spread = Decimal(str(actual_risk["actual_entry_spread_bps"]))
        if not incremental.is_finite() or incremental <= 0 or not actual_spread.is_finite():
            raise ValueError("actual risk snapshot values are invalid")
        if int(str(actual_risk["fill_event_watermark"])) != expected_event_watermark:
            raise ValueError("actual risk watermark is inconsistent")
        breakdown = {
            key: Decimal(str(actual_risk[key])) for key in breakdown_fields if key in actual_risk
        }
        if any(not value.is_finite() or value < 0 for value in breakdown.values()):
            raise ValueError("actual risk cost breakdown is invalid")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            observed_watermark = self._event_watermark_in_transaction(database)
            action = self._load_in_transaction(database, pair_action_id)
            if action is None:
                database.rollback()
                raise KeyError(pair_action_id)
            if observed_watermark != expected_event_watermark:
                database.rollback()
                raise RuntimeError("actual fill risk event watermark changed")
            if action.state == LiveActionState.FLAT:
                database.rollback()
                raise RuntimeError("actual fill risk cannot update a flat action")
            active_rows = database.execute(
                "SELECT pair_action_id, route_base, long_venue, short_venue, "
                "risk_reservation_json, emergency_exclusive FROM live_pair_actions "
                "WHERE state <> ? AND pair_action_id <> ?",
                (LiveActionState.FLAT.value, pair_action_id),
            ).fetchall()
            route_total = incremental
            portfolio_total = incremental
            for row in active_rows:
                if bool(row["emergency_exclusive"]):
                    continue
                peer_stress = self._admission_stress(json.loads(str(row["risk_reservation_json"])))
                portfolio_total += peer_stress
                if (
                    str(row["route_base"]) == action.route.base
                    and str(row["long_venue"]) == action.route.long_venue.value
                    and str(row["short_venue"]) == action.route.short_venue.value
                ):
                    route_total += peer_stress
            authoritative_risk = {
                "incremental_stress_usdt": str(incremental),
                "route_total_usdt": str(route_total),
                "portfolio_total_usdt": str(portfolio_total),
                "actual_entry_spread_bps": str(actual_spread),
                "fill_event_watermark": expected_event_watermark,
                **{key: str(value) for key, value in breakdown.items()},
            }
            reservation = dict(action.risk_reservation)
            reservation["actual_fill_risk"] = authoritative_risk
            database.execute(
                "UPDATE live_pair_actions SET risk_reservation_json = ?, updated_at = ? "
                "WHERE pair_action_id = ?",
                (
                    json.dumps(reservation, default=str, sort_keys=True),
                    now.isoformat(),
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
                    action.state.value,
                    action.state.value,
                    json.dumps(
                        {"actual_fill_risk": authoritative_risk},
                        default=str,
                        sort_keys=True,
                    ),
                    now.isoformat(),
                ),
            )
            updated = self._load_in_transaction(database, pair_action_id)
            database.commit()
        if updated is None:
            raise RuntimeError("actual fill risk update lost the durable action")
        return updated

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

    async def completed_fast_live_actions(self) -> tuple[LiveJournalAction, ...]:
        """Return completed V2 actions without consulting legacy qualification lineage."""
        return await asyncio.to_thread(self._completed_fast_live_actions_sync)

    async def actions_updated_after(
        self,
        boundary: datetime,
        qualification_hash: str,
    ) -> tuple[LiveJournalAction, ...]:
        return await asyncio.to_thread(
            self._actions_updated_after_sync,
            boundary,
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

    def _completed_fast_live_actions_sync(self) -> tuple[LiveJournalAction, ...]:
        with self._connect() as database:
            database.execute("BEGIN")
            rows = database.execute(
                "SELECT pair_action_id, risk_reservation_json FROM live_pair_actions "
                "WHERE state = ? ORDER BY created_at, pair_action_id",
                (LiveActionState.FLAT.value,),
            ).fetchall()
            actions = tuple(
                action
                for row in rows
                if json.loads(str(row["risk_reservation_json"])).get("strategy")
                == "AGGRESSIVE_FAST_LIVE_V2"
                and (action := self._load_in_transaction(database, str(row["pair_action_id"])))
                is not None
            )
        return actions

    def _actions_updated_after_sync(
        self,
        boundary: datetime,
        qualification_hash: str,
    ) -> tuple[LiveJournalAction, ...]:
        if boundary.tzinfo is None or boundary.utcoffset() is None:
            raise ValueError("journal tail boundary must be timezone-aware")
        with self._connect() as database:
            database.execute("BEGIN")
            rows = database.execute(
                """
                SELECT pair_action_id FROM live_pair_actions
                WHERE updated_at > ? AND qualification_hash = ?
                ORDER BY updated_at, pair_action_id
                """,
                (boundary.isoformat(), qualification_hash),
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

    def completed_fast_live_normal_snapshot_in_transaction(
        self,
        database: sqlite3.Connection,
        started_at: datetime,
    ) -> tuple[tuple[str, ...], str]:
        """Snapshot genuine V2 cycles without legacy qualification filtering."""
        rows = database.execute(
            "SELECT pair_action_id, risk_reservation_json FROM live_pair_actions "
            "WHERE state = ? AND created_at >= ? ORDER BY pair_action_id",
            (LiveActionState.FLAT.value, started_at.isoformat()),
        ).fetchall()
        actions = tuple(
            action
            for row in rows
            if json.loads(str(row["risk_reservation_json"])).get("strategy")
            == "AGGRESSIVE_FAST_LIVE_V2"
            and (action := self._load_in_transaction(database, str(row["pair_action_id"])))
            is not None
            and is_completed_normal_paired_cycle(action)
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
    def _validate_active_portfolio_rows(rows: list[sqlite3.Row]) -> None:
        if len(rows) > MAX_ACTIVE_LIVE_ACTIONS:
            raise RuntimeError("legacy active live actions exceed the maximum limit")
        routes_by_base: dict[str, tuple[str, str]] = {}
        tranche_ids_by_route: dict[tuple[str, str, str], set[str]] = {}
        stress_by_route: dict[tuple[str, str, str], Decimal] = {}
        portfolio_stress = Decimal(0)
        emergency_count = 0
        for row in rows:
            base = str(row["route_base"])
            venues = (str(row["long_venue"]), str(row["short_venue"]))
            existing = routes_by_base.setdefault(base, venues)
            if existing != venues:
                raise RuntimeError("legacy active live actions contain multiple routes per base")
            route_identity = (base, *venues)
            tranche_ids = tranche_ids_by_route.setdefault(route_identity, set())
            tranche_id = str(row["tranche_id"])
            if tranche_id in tranche_ids:
                raise RuntimeError("legacy active live route contains duplicate tranche IDs")
            tranche_ids.add(tranche_id)
            if len(tranche_ids) > MAX_TRANCHES_PER_LIVE_ROUTE:
                raise RuntimeError("legacy active live route exceeds the tranche limit")
            is_emergency = bool(row["emergency_exclusive"])
            emergency_count += is_emergency
            if not is_emergency:
                stress = LiveOrderJournal._projected_stress(
                    json.loads(str(row["risk_reservation_json"]))
                )
                if stress > MAX_LIVE_ROUTE_STRESS_USDT:
                    raise RuntimeError("legacy active live route exceeds the stress limit")
                stress_by_route[route_identity] = (
                    stress_by_route.get(route_identity, Decimal(0)) + stress
                )
                portfolio_stress += stress
        if len(tranche_ids_by_route) > MAX_ACTIVE_LIVE_ROUTES:
            raise RuntimeError("legacy active live routes exceed the maximum limit")
        if emergency_count and len(rows) != 1:
            raise RuntimeError("legacy emergency live action is not exclusive")
        if any(stress > MAX_LIVE_ROUTE_STRESS_USDT for stress in stress_by_route.values()):
            raise RuntimeError("legacy active live route exceeds the stress limit")
        if portfolio_stress > MAX_LIVE_PORTFOLIO_STRESS_USDT:
            raise RuntimeError("legacy active live portfolio exceeds the stress limit")

    @staticmethod
    def _validate_new_tranche_in_transaction(
        database: sqlite3.Connection,
        route: DirectedRouteKey,
        tranche_id: str,
        risk_reservation: object | None,
    ) -> None:
        emergency = database.execute(
            "SELECT pair_action_id FROM live_pair_actions "
            "WHERE state <> ? AND emergency_exclusive = 1 LIMIT 1",
            (LiveActionState.FLAT.value,),
        ).fetchone()
        if emergency is not None:
            raise RuntimeError("global emergency live action lease is already held")
        if risk_reservation is None:
            return
        new_stress = LiveOrderJournal._admission_stress(risk_reservation)
        if new_stress > MAX_LIVE_ROUTE_STRESS_USDT:
            raise RuntimeError("maximum live route stress reached")
        rows = database.execute(
            "SELECT long_venue, short_venue, tranche_id, risk_reservation_json, "
            "emergency_exclusive FROM live_pair_actions "
            "WHERE state <> ? AND route_base = ?",
            (LiveActionState.FLAT.value, route.base),
        ).fetchall()
        exact_rows = [
            row
            for row in rows
            if str(row["long_venue"]) == route.long_venue.value
            and str(row["short_venue"]) == route.short_venue.value
        ]
        if rows and len(exact_rows) != len(rows):
            raise RuntimeError("one active live route per base is required")
        if any(str(row["tranche_id"]) == tranche_id for row in exact_rows):
            raise RuntimeError("live route tranche ID is already active")
        if len(exact_rows) >= MAX_TRANCHES_PER_LIVE_ROUTE:
            raise RuntimeError("maximum live tranches per route reached")
        route_stress = new_stress + sum(
            (
                LiveOrderJournal._admission_stress(json.loads(str(row["risk_reservation_json"])))
                for row in exact_rows
                if not bool(row["emergency_exclusive"])
            ),
            Decimal(0),
        )
        if route_stress > MAX_LIVE_ROUTE_STRESS_USDT:
            raise RuntimeError("maximum live route stress reached")
        portfolio_stress = new_stress + sum(
            (
                LiveOrderJournal._admission_stress(json.loads(str(row["risk_reservation_json"])))
                for row in database.execute(
                    "SELECT risk_reservation_json, emergency_exclusive "
                    "FROM live_pair_actions "
                    "WHERE state <> ?",
                    (LiveActionState.FLAT.value,),
                ).fetchall()
                if not bool(row["emergency_exclusive"])
            ),
            Decimal(0),
        )
        if portfolio_stress > MAX_LIVE_PORTFOLIO_STRESS_USDT:
            raise RuntimeError("maximum live portfolio stress reached")
        if not exact_rows:
            route_count = int(
                database.execute(
                    "SELECT count(*) FROM (SELECT DISTINCT route_base, long_venue, short_venue "
                    "FROM live_pair_actions WHERE state <> ?)",
                    (LiveActionState.FLAT.value,),
                ).fetchone()[0]
            )
            if route_count >= MAX_ACTIVE_LIVE_ROUTES:
                raise RuntimeError("maximum active live route limit reached")

    @staticmethod
    def _projected_stress(risk_reservation: object) -> Decimal:
        if not isinstance(risk_reservation, dict):
            raise RuntimeError("live risk reservation is invalid")
        try:
            stress = Decimal(str(risk_reservation["projected_stress_usdt"]))
        except (DecimalException, KeyError, ValueError):
            raise RuntimeError("live risk reservation is invalid") from None
        if not stress.is_finite() or stress <= 0:
            raise RuntimeError("live risk reservation is invalid")
        return stress

    @staticmethod
    def _admission_stress(risk_reservation: object) -> Decimal:
        planned = LiveOrderJournal._projected_stress(risk_reservation)
        assert isinstance(risk_reservation, dict)
        actual = risk_reservation.get("actual_fill_risk")
        if not isinstance(actual, dict):
            return planned
        try:
            repriced = Decimal(str(actual["incremental_stress_usdt"]))
        except (DecimalException, KeyError, ValueError):
            raise RuntimeError("live actual-fill risk reservation is invalid") from None
        if not repriced.is_finite() or repriced <= 0:
            raise RuntimeError("live actual-fill risk reservation is invalid")
        return max(planned, repriced)

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
            (f"base:{route.base.strip().upper()}:{pair_action_id}", "BASE"),
            (f"route:{route.value}:{pair_action_id}", "ROUTE"),
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
        risk_reservation = json.loads(str(row["risk_reservation_json"]))
        return LiveJournalAction(
            pair_action_id=str(row["pair_action_id"]),
            route=DirectedRouteKey(
                str(row["route_base"]),
                Venue(str(row["long_venue"])),
                Venue(str(row["short_venue"])),
            ),
            tranche_id=str(row["tranche_id"]),
            state=LiveActionState(str(row["state"])),
            risk_reservation=risk_reservation,
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
            activation_hash=(
                str(risk_reservation["activation_hash"])
                if risk_reservation.get("activation_hash") is not None
                else None
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


def _private_order_from_json(payload_json: str) -> PrivateOrder:
    payload = json.loads(payload_json)
    required = {
        "venue",
        "order_id",
        "client_order_id",
        "symbol",
        "side",
        "status",
        "requested_base_quantity",
        "filled_base_quantity",
        "average_price",
        "fee_usdt",
        "observed_at",
        "limit_price",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise RuntimeError("durable private order event payload is invalid")

    def optional_decimal(key: str) -> Decimal | None:
        value = payload[key]
        return None if value is None else Decimal(str(value))

    return PrivateOrder(
        venue=Venue(str(payload["venue"])),
        order_id=None if payload["order_id"] is None else str(payload["order_id"]),
        client_order_id=str(payload["client_order_id"]),
        symbol=str(payload["symbol"]),
        side=Side(str(payload["side"])),
        status=PrivateOrderStatus(str(payload["status"])),
        requested_base_quantity=Decimal(str(payload["requested_base_quantity"])),
        filled_base_quantity=Decimal(str(payload["filled_base_quantity"])),
        average_price=optional_decimal("average_price"),
        fee_usdt=optional_decimal("fee_usdt"),
        observed_at=datetime.fromisoformat(str(payload["observed_at"])),
        limit_price=optional_decimal("limit_price"),
    )
