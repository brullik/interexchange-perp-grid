from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import cast

from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalReferenceModel,
    ModelEligibility,
    historical_model_sha256,
)
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side

_SCHEMA_VERSION = 1
_LEVEL_COUNT = 5


class GridLevelState(StrEnum):
    ARMED = "ARMED"
    ENTRY_PENDING = "ENTRY_PENDING"
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED_WAIT_REARM = "CLOSED_WAIT_REARM"
    DISABLED = "DISABLED"


@dataclass(frozen=True, slots=True)
class GridLegFill:
    venue: Venue
    symbol: str
    side: Side
    base_quantity: Decimal
    average_price: Decimal
    fee_usdt: Decimal
    funding_usdt: Decimal

    def __post_init__(self) -> None:
        values = (
            self.base_quantity,
            self.average_price,
            self.fee_usdt,
            self.funding_usdt,
        )
        if not self.symbol or any(not value.is_finite() for value in values):
            raise ValueError("grid leg fill is incomplete or non-finite")
        if self.base_quantity <= 0 or self.average_price <= 0 or self.fee_usdt < 0:
            raise ValueError("grid leg fill quantity, price, or fee is invalid")


@dataclass(frozen=True, slots=True)
class GridTrancheOwnership:
    tranche_id: str
    normalized_base_quantity: Decimal
    legs: tuple[GridLegFill, GridLegFill]
    executable_entry_spread_bps: Decimal
    reverse_target_bps: Decimal
    effective_stop_bps: Decimal
    maximum_holding_deadline: datetime
    reserved_stress_usdt: Decimal
    entry_slippage_usdt: Decimal
    realised_pnl_usdt: Decimal
    unrealised_pnl_usdt: Decimal
    opened_at: datetime

    def __post_init__(self) -> None:
        if not self.tranche_id:
            raise ValueError("grid tranche id must not be empty")
        if self.maximum_holding_deadline.tzinfo is None or self.opened_at.tzinfo is None:
            raise ValueError("grid tranche timestamps must be timezone-aware")
        if self.maximum_holding_deadline <= self.opened_at:
            raise ValueError("grid tranche holding deadline must follow open time")
        if self.legs[0].venue == self.legs[1].venue or self.legs[0].side == self.legs[1].side:
            raise ValueError("grid tranche must own paired opposite legs on distinct venues")
        if self.legs[0].base_quantity != self.legs[1].base_quantity:
            raise ValueError("grid tranche legs must share normalized base quantity")
        if self.normalized_base_quantity != self.legs[0].base_quantity:
            raise ValueError("grid tranche normalized quantity must equal both leg fills")
        values = (
            self.normalized_base_quantity,
            self.executable_entry_spread_bps,
            self.reverse_target_bps,
            self.effective_stop_bps,
            self.reserved_stress_usdt,
            self.entry_slippage_usdt,
            self.realised_pnl_usdt,
            self.unrealised_pnl_usdt,
        )
        if any(not value.is_finite() for value in values):
            raise ValueError("grid tranche ownership contains non-finite values")
        if self.normalized_base_quantity <= 0 or self.reserved_stress_usdt < 0:
            raise ValueError("grid tranche quantity or stress is invalid")


@dataclass(frozen=True, slots=True)
class GridLevelRecord:
    route_identity: str
    model_sha256: str
    direction: DivergenceDirection
    level_index: int
    trigger_bps: Decimal
    allocated_weight: Decimal
    rearm_boundary_bps: Decimal
    state: GridLevelState
    reserved_stress_usdt: Decimal
    pending_decision_cycle: int | None
    ownership: GridTrancheOwnership | None
    generation: int
    updated_at: datetime
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if not self.route_identity or not self.model_sha256:
            raise ValueError("grid level identity is incomplete")
        if not 1 <= self.level_index <= _LEVEL_COUNT:
            raise ValueError("grid level index must be within 1..5")
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise ValueError("grid level timestamp must be timezone-aware")
        if (
            self.state in (GridLevelState.OPEN, GridLevelState.EXIT_PENDING)
            and self.ownership is None
        ):
            raise ValueError("open grid state requires tranche ownership")
        if self.state in (GridLevelState.ARMED, GridLevelState.ENTRY_PENDING) and self.ownership:
            raise ValueError("unfilled grid state cannot expose tranche ownership")
        if (self.state == GridLevelState.ENTRY_PENDING) != (
            self.pending_decision_cycle is not None
        ):
            raise ValueError("only entry-pending state may own a decision cycle")
        if self.execution_authorized:
            raise ValueError("grid state never authorizes execution")


@dataclass(frozen=True, slots=True)
class ExternalGridLevelProjection:
    level_index: int
    state: GridLevelState
    reserved_stress_usdt: Decimal
    decision_cycle: int | None = None
    ownership: GridTrancheOwnership | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.level_index <= _LEVEL_COUNT:
            raise ValueError("external grid level index is invalid")
        if self.state not in {
            GridLevelState.ENTRY_PENDING,
            GridLevelState.OPEN,
            GridLevelState.CLOSED_WAIT_REARM,
        }:
            raise ValueError("external grid projection state is invalid")
        if not self.reserved_stress_usdt.is_finite() or self.reserved_stress_usdt < 0:
            raise ValueError("external grid projection stress is invalid")
        if self.state == GridLevelState.ENTRY_PENDING and (
            self.decision_cycle is None or self.ownership is not None
        ):
            raise ValueError("external pending grid projection is invalid")
        if self.state == GridLevelState.OPEN and (
            self.ownership is None or self.decision_cycle is not None
        ):
            raise ValueError("external open grid projection is invalid")
        if self.state == GridLevelState.CLOSED_WAIT_REARM and (
            self.ownership is not None
            or self.decision_cycle is not None
            or self.reserved_stress_usdt != 0
        ):
            raise ValueError("external closed grid projection is invalid")


@dataclass(frozen=True, slots=True)
class FrozenGridSizingPlan:
    route_identity: str
    model_sha256: str
    full_route_base_quantity: Decimal
    tranche_base_quantities: tuple[Decimal, ...]
    tranche_projected_losses_usdt: tuple[Decimal, ...]
    projected_margin_usdt: Decimal
    created_at: datetime

    def __post_init__(self) -> None:
        values = (
            self.full_route_base_quantity,
            *self.tranche_base_quantities,
            *self.tranche_projected_losses_usdt,
            self.projected_margin_usdt,
        )
        if (
            not self.route_identity
            or not self.model_sha256
            or len(self.tranche_base_quantities) != _LEVEL_COUNT
            or len(self.tranche_projected_losses_usdt) != _LEVEL_COUNT
            or self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
            or any(not value.is_finite() or value < 0 for value in values)
            or self.full_route_base_quantity != sum(self.tranche_base_quantities, Decimal(0))
        ):
            raise ValueError("frozen grid sizing plan is invalid")


class AggressiveGridStore:
    """Transactional five-level ownership state; it never performs exchange actions."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def initialise(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as database:
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS aggressive_grid_schema (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS aggressive_grid_routes (
                    route_identity TEXT PRIMARY KEY,
                    model_sha256 TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    last_decision_cycle INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS aggressive_grid_levels (
                    route_identity TEXT NOT NULL,
                    model_sha256 TEXT NOT NULL,
                    level_index INTEGER NOT NULL CHECK (level_index BETWEEN 1 AND 5),
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (route_identity, level_index),
                    FOREIGN KEY (route_identity) REFERENCES aggressive_grid_routes(route_identity)
                );
                CREATE TABLE IF NOT EXISTS aggressive_grid_sizing_plans (
                    route_identity TEXT PRIMARY KEY,
                    model_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY (route_identity) REFERENCES aggressive_grid_routes(route_identity)
                );
                """
            )
            row = database.execute(
                "SELECT schema_version FROM aggressive_grid_schema WHERE singleton = 1"
            ).fetchone()
            if row is None:
                database.execute(
                    "INSERT INTO aggressive_grid_schema(singleton, schema_version) VALUES(1, ?)",
                    (_SCHEMA_VERSION,),
                )
            elif int(row[0]) != _SCHEMA_VERSION:
                raise RuntimeError("unsupported aggressive grid schema version")

    def initialise_route(
        self,
        model: HistoricalReferenceModel,
        direction: DivergenceDirection,
        *,
        now: datetime,
        rearm_retreat_step_fraction: Decimal,
    ) -> tuple[GridLevelRecord, ...]:
        self._require_aware(now)
        if not 0 < rearm_retreat_step_fraction < 1:
            raise ValueError("rearm retreat fraction must be within (0, 1)")
        direction_model = (
            model.positive if direction == DivergenceDirection.POSITIVE else model.negative
        )
        route_identity = (
            model.positive_route
            if direction == DivergenceDirection.POSITIVE
            else model.negative_route
        )
        model_hash = historical_model_sha256(model)
        grid_step = direction_model.range_bps / Decimal(_LEVEL_COUNT)
        initial_state = (
            GridLevelState.DISABLED
            if direction_model.eligibility == ModelEligibility.DISABLED
            else GridLevelState.ARMED
        )
        records = tuple(
            GridLevelRecord(
                route_identity=route_identity,
                model_sha256=model_hash,
                direction=direction,
                level_index=index,
                trigger_bps=trigger,
                allocated_weight=direction_model.tranche_weights[index - 1],
                rearm_boundary_bps=(
                    trigger - rearm_retreat_step_fraction * grid_step
                    if direction == DivergenceDirection.POSITIVE
                    else trigger + rearm_retreat_step_fraction * grid_step
                ),
                state=initial_state,
                reserved_stress_usdt=Decimal(0),
                pending_decision_cycle=None,
                ownership=None,
                generation=0,
                updated_at=now,
            )
            for index, trigger in enumerate(direction_model.levels_bps, start=1)
        )
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            existing = database.execute(
                """
                SELECT model_sha256, direction FROM aggressive_grid_routes
                WHERE route_identity = ?
                """,
                (route_identity,),
            ).fetchone()
            if existing is not None:
                if (str(existing[0]), str(existing[1])) != (model_hash, direction.value):
                    raise RuntimeError("active grid route model identity mismatch")
                persisted = self._levels_locked(database, route_identity)
                if tuple(_geometry(record) for record in persisted) != tuple(
                    _geometry(record) for record in records
                ):
                    raise RuntimeError("persisted grid geometry mismatch")
                database.commit()
                return persisted
            database.execute(
                """
                INSERT INTO aggressive_grid_routes(
                    route_identity, model_sha256, direction, last_decision_cycle, updated_at
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (route_identity, model_hash, direction.value, -1, now.isoformat()),
            )
            for record in records:
                self._write_level_locked(database, record)
            database.commit()
        return records

    def levels(self, route_identity: str) -> tuple[GridLevelRecord, ...]:
        with self._connect() as database:
            levels = self._levels_locked(database, route_identity)
        if len(levels) != _LEVEL_COUNT:
            raise RuntimeError("grid route must contain exactly five levels")
        return levels

    def next_decision_cycle(self, route_identity: str) -> int:
        """Return the next durable cycle value for a single-writer decision loop."""
        with self._connect() as database:
            row = database.execute(
                "SELECT last_decision_cycle FROM aggressive_grid_routes WHERE route_identity = ?",
                (route_identity,),
            ).fetchone()
        if row is None:
            raise RuntimeError("grid route is not initialised")
        return int(row[0]) + 1

    def frozen_sizing_plan(self, route_identity: str) -> FrozenGridSizingPlan | None:
        with self._connect() as database:
            row = database.execute(
                "SELECT payload_json FROM aggressive_grid_sizing_plans WHERE route_identity = ?",
                (route_identity,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row[0]))
        return FrozenGridSizingPlan(
            route_identity=str(payload["route_identity"]),
            model_sha256=str(payload["model_sha256"]),
            full_route_base_quantity=Decimal(str(payload["full_route_base_quantity"])),
            tranche_base_quantities=tuple(
                Decimal(str(value)) for value in payload["tranche_base_quantities"]
            ),
            tranche_projected_losses_usdt=tuple(
                Decimal(str(value)) for value in payload["tranche_projected_losses_usdt"]
            ),
            projected_margin_usdt=Decimal(str(payload["projected_margin_usdt"])),
            created_at=datetime.fromisoformat(str(payload["created_at"])),
        )

    def freeze_sizing_plan(self, plan: FrozenGridSizingPlan) -> FrozenGridSizingPlan:
        encoded = json.dumps(asdict(plan), default=str, sort_keys=True, separators=(",", ":"))
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            route = database.execute(
                "SELECT model_sha256 FROM aggressive_grid_routes WHERE route_identity = ?",
                (plan.route_identity,),
            ).fetchone()
            if route is None or str(route[0]) != plan.model_sha256:
                database.rollback()
                raise RuntimeError("frozen sizing route model identity mismatch")
            existing = database.execute(
                "SELECT payload_json FROM aggressive_grid_sizing_plans WHERE route_identity = ?",
                (plan.route_identity,),
            ).fetchone()
            if existing is not None:
                observed = self.frozen_sizing_plan(plan.route_identity)
                if observed != plan:
                    database.rollback()
                    raise RuntimeError("frozen sizing plan cannot change while route is active")
                database.commit()
                return plan
            database.execute(
                "INSERT INTO aggressive_grid_sizing_plans("
                "route_identity, model_sha256, payload_json) "
                "VALUES(?, ?, ?)",
                (plan.route_identity, plan.model_sha256, encoded),
            )
            database.commit()
        return plan

    def first_unfilled_crossed_level(
        self,
        route_identity: str,
        reference_spread_bps: Decimal,
    ) -> GridLevelRecord | None:
        if not reference_spread_bps.is_finite():
            raise ValueError("reference spread must be finite")
        for record in self.levels(route_identity):
            crossed = (
                reference_spread_bps >= record.trigger_bps
                if record.direction == DivergenceDirection.POSITIVE
                else reference_spread_bps <= record.trigger_bps
            )
            if record.state == GridLevelState.ARMED and crossed:
                return record
        return None

    def reserve_entry(
        self,
        route_identity: str,
        *,
        reference_spread_bps: Decimal,
        decision_cycle: int,
        reserved_stress_usdt: Decimal,
        now: datetime,
    ) -> GridLevelRecord:
        self._require_aware(now)
        if decision_cycle < 0 or not reserved_stress_usdt.is_finite() or reserved_stress_usdt < 0:
            raise ValueError("entry cycle or reserved stress is invalid")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            route = database.execute(
                "SELECT last_decision_cycle FROM aggressive_grid_routes WHERE route_identity = ?",
                (route_identity,),
            ).fetchone()
            if route is None:
                raise RuntimeError("grid route is not initialised")
            if decision_cycle <= int(route[0]):
                raise RuntimeError("decision cycle already consumed or out of order")
            levels = self._levels_locked(database, route_identity)
            selected = _first_crossed(levels, reference_spread_bps)
            if selected is None:
                raise RuntimeError("no armed grid level is crossed")
            pending = _replace_level(
                selected,
                state=GridLevelState.ENTRY_PENDING,
                reserved_stress_usdt=reserved_stress_usdt,
                pending_decision_cycle=decision_cycle,
                now=now,
            )
            self._write_level_locked(database, pending)
            database.execute(
                """
                UPDATE aggressive_grid_routes
                SET last_decision_cycle = ?, updated_at = ?
                WHERE route_identity = ?
                """,
                (decision_cycle, now.isoformat(), route_identity),
            )
            database.commit()
            return pending

    def mark_entry_failed(
        self,
        route_identity: str,
        level_index: int,
        *,
        decision_cycle: int,
        now: datetime,
    ) -> GridLevelRecord:
        return self._transition(
            route_identity,
            level_index,
            expected=GridLevelState.ENTRY_PENDING,
            target=GridLevelState.ARMED,
            now=now,
            ownership=None,
            reserved_stress_usdt=Decimal(0),
            expected_pending_cycle=decision_cycle,
        )

    def synchronize_externally_owned_levels(
        self,
        route_identity: str,
        level_indices: frozenset[int],
        *,
        now: datetime,
    ) -> tuple[GridLevelRecord, ...]:
        """Fence levels already owned/completed by the durable live journal.

        The exchange/journal remains the source of fill ownership. This projection exists only so
        the shared strategy core cannot emit the same live level twice after restart.
        """
        self._require_aware(now)
        if any(not 1 <= level <= _LEVEL_COUNT for level in level_indices):
            raise ValueError("external live level index is invalid")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            records = self._levels_locked(database, route_identity)
            if len(records) != _LEVEL_COUNT:
                database.rollback()
                raise RuntimeError("aggressive live grid route is incomplete")
            updated: list[GridLevelRecord] = []
            for record in records:
                if record.level_index not in level_indices:
                    updated.append(record)
                    continue
                if record.state == GridLevelState.ARMED:
                    record = _replace_level(
                        record,
                        state=GridLevelState.CLOSED_WAIT_REARM,
                        ownership=None,
                        reserved_stress_usdt=Decimal(0),
                        pending_decision_cycle=None,
                        now=now,
                    )
                    self._write_level_locked(database, record)
                elif record.state != GridLevelState.CLOSED_WAIT_REARM:
                    database.rollback()
                    raise RuntimeError("external live level conflicts with local grid ownership")
                updated.append(record)
            database.commit()
        return tuple(updated)

    def synchronize_journal_levels(
        self,
        route_identity: str,
        projections: tuple[ExternalGridLevelProjection, ...],
        *,
        now: datetime,
    ) -> tuple[GridLevelRecord, ...]:
        """Project exact durable journal lifecycle into the strategy ownership grid."""
        self._require_aware(now)
        by_level = {item.level_index: item for item in projections}
        if len(by_level) != len(projections):
            raise RuntimeError("journal grid projection contains duplicate levels")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            records = self._levels_locked(database, route_identity)
            if len(records) != _LEVEL_COUNT:
                database.rollback()
                raise RuntimeError("aggressive live grid route is incomplete")
            updated: list[GridLevelRecord] = []
            for record in records:
                projection = by_level.get(record.level_index)
                if projection is None:
                    updated.append(record)
                    continue
                allowed = {
                    GridLevelState.ARMED: {
                        GridLevelState.ENTRY_PENDING,
                        GridLevelState.OPEN,
                        GridLevelState.CLOSED_WAIT_REARM,
                    },
                    GridLevelState.ENTRY_PENDING: {
                        GridLevelState.ENTRY_PENDING,
                        GridLevelState.OPEN,
                        GridLevelState.CLOSED_WAIT_REARM,
                    },
                    GridLevelState.OPEN: {
                        GridLevelState.OPEN,
                        GridLevelState.CLOSED_WAIT_REARM,
                    },
                    GridLevelState.EXIT_PENDING: {GridLevelState.CLOSED_WAIT_REARM},
                    GridLevelState.CLOSED_WAIT_REARM: {GridLevelState.CLOSED_WAIT_REARM},
                    GridLevelState.DISABLED: set(),
                }
                if projection.state not in allowed[record.state]:
                    database.rollback()
                    raise RuntimeError("journal grid projection conflicts with local ownership")
                if projection.ownership is not None and not _ownership_matches_route(
                    route_identity, projection.ownership
                ):
                    database.rollback()
                    raise RuntimeError("journal grid ownership does not match route")
                record = _replace_level(
                    record,
                    state=projection.state,
                    ownership=projection.ownership,
                    reserved_stress_usdt=projection.reserved_stress_usdt,
                    pending_decision_cycle=projection.decision_cycle,
                    now=now,
                )
                self._write_level_locked(database, record)
                updated.append(record)
            database.commit()
        return tuple(updated)

    def mark_open(
        self,
        route_identity: str,
        level_index: int,
        ownership: GridTrancheOwnership,
        *,
        decision_cycle: int,
        now: datetime,
    ) -> GridLevelRecord:
        if not _ownership_matches_route(route_identity, ownership):
            raise RuntimeError("grid tranche legs do not match directed route ownership")
        return self._transition(
            route_identity,
            level_index,
            expected=GridLevelState.ENTRY_PENDING,
            target=GridLevelState.OPEN,
            now=now,
            ownership=ownership,
            reserved_stress_usdt=ownership.reserved_stress_usdt,
            expected_pending_cycle=decision_cycle,
        )

    def reserve_exit(
        self,
        route_identity: str,
        level_index: int,
        *,
        tranche_id: str,
        now: datetime,
    ) -> GridLevelRecord:
        current = self._level(route_identity, level_index)
        if current.ownership is None:
            raise RuntimeError("open grid level has no tranche ownership")
        return self._transition(
            route_identity,
            level_index,
            expected=GridLevelState.OPEN,
            target=GridLevelState.EXIT_PENDING,
            now=now,
            ownership=current.ownership,
            reserved_stress_usdt=current.reserved_stress_usdt,
            expected_tranche_id=tranche_id,
        )

    def mark_closed(
        self,
        route_identity: str,
        level_index: int,
        ownership: GridTrancheOwnership,
        *,
        now: datetime,
    ) -> GridLevelRecord:
        current = self._level(route_identity, level_index)
        if current.ownership is None or not _same_tranche_identity(current.ownership, ownership):
            raise RuntimeError("closed grid ownership does not match the open tranche")
        return self._transition(
            route_identity,
            level_index,
            expected=GridLevelState.EXIT_PENDING,
            target=GridLevelState.CLOSED_WAIT_REARM,
            now=now,
            ownership=ownership,
            reserved_stress_usdt=Decimal(0),
            expected_tranche_id=ownership.tranche_id,
        )

    def rearm(
        self,
        route_identity: str,
        level_index: int,
        *,
        reference_spread_bps: Decimal,
        stable_flat: bool,
        tranche_id: str,
        now: datetime,
    ) -> GridLevelRecord:
        current = self._level(route_identity, level_index)
        if current.state != GridLevelState.CLOSED_WAIT_REARM:
            raise RuntimeError("grid level is not waiting for rearm")
        retreated = (
            reference_spread_bps <= current.rearm_boundary_bps
            if current.direction == DivergenceDirection.POSITIVE
            else reference_spread_bps >= current.rearm_boundary_bps
        )
        if not stable_flat or not retreated:
            raise RuntimeError("grid level has not satisfied stable-FLAT retreat")
        return self._transition(
            route_identity,
            level_index,
            expected=GridLevelState.CLOSED_WAIT_REARM,
            target=GridLevelState.ARMED,
            now=now,
            ownership=None,
            reserved_stress_usdt=Decimal(0),
            expected_tranche_id=tranche_id,
        )

    def _transition(
        self,
        route_identity: str,
        level_index: int,
        *,
        expected: GridLevelState,
        target: GridLevelState,
        now: datetime,
        ownership: GridTrancheOwnership | None,
        reserved_stress_usdt: Decimal,
        expected_pending_cycle: int | None = None,
        expected_tranche_id: str | None = None,
    ) -> GridLevelRecord:
        self._require_aware(now)
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            current = self._level_locked(database, route_identity, level_index)
            if current.state != expected:
                raise RuntimeError(
                    f"grid level transition requires {expected.value}, got {current.state.value}"
                )
            if (
                expected_pending_cycle is not None
                and current.pending_decision_cycle != expected_pending_cycle
            ):
                raise RuntimeError("grid entry callback decision cycle is stale")
            if expected_tranche_id is not None and (
                current.ownership is None or current.ownership.tranche_id != expected_tranche_id
            ):
                raise RuntimeError("grid tranche callback identity is stale")
            if (
                target == GridLevelState.CLOSED_WAIT_REARM
                and current.ownership is not None
                and ownership is not None
                and not _same_tranche_identity(current.ownership, ownership)
            ):
                raise RuntimeError("closed grid ownership does not match the open tranche")
            updated = _replace_level(
                current,
                state=target,
                reserved_stress_usdt=reserved_stress_usdt,
                ownership=ownership,
                now=now,
            )
            self._write_level_locked(database, updated)
            database.commit()
            return updated

    def _level(self, route_identity: str, level_index: int) -> GridLevelRecord:
        with self._connect() as database:
            return self._level_locked(database, route_identity, level_index)

    def _level_locked(
        self,
        database: sqlite3.Connection,
        route_identity: str,
        level_index: int,
    ) -> GridLevelRecord:
        row = database.execute(
            """
            SELECT payload_json FROM aggressive_grid_levels
            WHERE route_identity = ? AND level_index = ?
            """,
            (route_identity, level_index),
        ).fetchone()
        if row is None:
            raise RuntimeError("grid level does not exist")
        return _record_from_payload(json.loads(str(row[0])))

    def _levels_locked(
        self,
        database: sqlite3.Connection,
        route_identity: str,
    ) -> tuple[GridLevelRecord, ...]:
        rows = database.execute(
            """
            SELECT payload_json FROM aggressive_grid_levels
            WHERE route_identity = ? ORDER BY level_index
            """,
            (route_identity,),
        ).fetchall()
        return tuple(_record_from_payload(json.loads(str(row[0]))) for row in rows)

    def _write_level_locked(
        self,
        database: sqlite3.Connection,
        record: GridLevelRecord,
    ) -> None:
        payload = json.dumps(_record_payload(record), sort_keys=True, separators=(",", ":"))
        database.execute(
            """
            INSERT INTO aggressive_grid_levels(
                route_identity, model_sha256, level_index, payload_json
            ) VALUES(?, ?, ?, ?)
            ON CONFLICT(route_identity, level_index) DO UPDATE SET
                model_sha256 = excluded.model_sha256,
                payload_json = excluded.payload_json
            """,
            (record.route_identity, record.model_sha256, record.level_index, payload),
        )

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.path, timeout=5)
        database.execute("PRAGMA journal_mode=WAL")
        database.execute("PRAGMA foreign_keys=ON")
        return database

    @staticmethod
    def _require_aware(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("grid transition timestamp must be timezone-aware")


def reverse_grid_target_bps(
    direction: DivergenceDirection,
    *,
    actual_entry_spread_bps: Decimal,
    grid_step_bps: Decimal,
    stressed_cost_move_bps: Decimal,
    minimum_profit_move_bps: Decimal,
    normal_low_bps: Decimal,
    normal_high_bps: Decimal,
) -> Decimal:
    move = max(grid_step_bps, stressed_cost_move_bps + minimum_profit_move_bps)
    if any(
        not value.is_finite()
        for value in (
            actual_entry_spread_bps,
            grid_step_bps,
            stressed_cost_move_bps,
            minimum_profit_move_bps,
            normal_low_bps,
            normal_high_bps,
        )
    ):
        raise ValueError("reverse-grid target inputs must be finite")
    if move <= 0:
        raise ValueError("reverse-grid target move must be positive")
    if direction == DivergenceDirection.POSITIVE:
        return max(normal_high_bps, actual_entry_spread_bps - move)
    return min(normal_low_bps, actual_entry_spread_bps + move)


def _first_crossed(
    levels: tuple[GridLevelRecord, ...],
    reference_spread_bps: Decimal,
) -> GridLevelRecord | None:
    if not reference_spread_bps.is_finite():
        raise ValueError("reference spread must be finite")
    for record in levels:
        crossed = (
            reference_spread_bps >= record.trigger_bps
            if record.direction == DivergenceDirection.POSITIVE
            else reference_spread_bps <= record.trigger_bps
        )
        if record.state == GridLevelState.ARMED and crossed:
            return record
    return None


def _replace_level(
    record: GridLevelRecord,
    *,
    state: GridLevelState,
    reserved_stress_usdt: Decimal,
    now: datetime,
    ownership: GridTrancheOwnership | None = None,
    pending_decision_cycle: int | None = None,
) -> GridLevelRecord:
    return GridLevelRecord(
        route_identity=record.route_identity,
        model_sha256=record.model_sha256,
        direction=record.direction,
        level_index=record.level_index,
        trigger_bps=record.trigger_bps,
        allocated_weight=record.allocated_weight,
        rearm_boundary_bps=record.rearm_boundary_bps,
        state=state,
        reserved_stress_usdt=reserved_stress_usdt,
        pending_decision_cycle=pending_decision_cycle,
        ownership=ownership,
        generation=record.generation + 1,
        updated_at=now,
    )


def _geometry(record: GridLevelRecord) -> tuple[object, ...]:
    return (
        record.route_identity,
        record.model_sha256,
        record.direction,
        record.level_index,
        record.trigger_bps,
        record.allocated_weight,
        record.rearm_boundary_bps,
    )


def _same_tranche_identity(
    current: GridTrancheOwnership,
    updated: GridTrancheOwnership,
) -> bool:
    return (
        current.tranche_id,
        current.normalized_base_quantity,
        tuple(
            (leg.venue, leg.symbol, leg.side, leg.base_quantity, leg.average_price)
            for leg in current.legs
        ),
        current.executable_entry_spread_bps,
        current.reverse_target_bps,
        current.effective_stop_bps,
        current.maximum_holding_deadline,
        current.opened_at,
    ) == (
        updated.tranche_id,
        updated.normalized_base_quantity,
        tuple(
            (leg.venue, leg.symbol, leg.side, leg.base_quantity, leg.average_price)
            for leg in updated.legs
        ),
        updated.executable_entry_spread_bps,
        updated.reverse_target_bps,
        updated.effective_stop_bps,
        updated.maximum_holding_deadline,
        updated.opened_at,
    )


def _ownership_matches_route(
    route_identity: str,
    ownership: GridTrancheOwnership,
) -> bool:
    try:
        _, venues = route_identity.split(":", maxsplit=1)
        long_value, short_value = venues.split(">", maxsplit=1)
        long_venue = Venue(long_value)
        short_venue = Venue(short_value)
    except (ValueError, TypeError):
        return False
    return {(leg.venue, leg.side) for leg in ownership.legs} == {
        (long_venue, Side.BUY),
        (short_venue, Side.SELL),
    }


def _record_payload(record: GridLevelRecord) -> dict[str, object]:
    ownership = None
    if record.ownership is not None:
        ownership = asdict(record.ownership)
        ownership["normalized_base_quantity"] = str(record.ownership.normalized_base_quantity)
        ownership["executable_entry_spread_bps"] = str(record.ownership.executable_entry_spread_bps)
        ownership["reverse_target_bps"] = str(record.ownership.reverse_target_bps)
        ownership["effective_stop_bps"] = str(record.ownership.effective_stop_bps)
        ownership["maximum_holding_deadline"] = (
            record.ownership.maximum_holding_deadline.isoformat()
        )
        ownership["reserved_stress_usdt"] = str(record.ownership.reserved_stress_usdt)
        ownership["entry_slippage_usdt"] = str(record.ownership.entry_slippage_usdt)
        ownership["realised_pnl_usdt"] = str(record.ownership.realised_pnl_usdt)
        ownership["unrealised_pnl_usdt"] = str(record.ownership.unrealised_pnl_usdt)
        ownership["opened_at"] = record.ownership.opened_at.isoformat()
        ownership["legs"] = [
            {
                "venue": leg.venue.value,
                "symbol": leg.symbol,
                "side": leg.side.value,
                "base_quantity": str(leg.base_quantity),
                "average_price": str(leg.average_price),
                "fee_usdt": str(leg.fee_usdt),
                "funding_usdt": str(leg.funding_usdt),
            }
            for leg in record.ownership.legs
        ]
    return {
        "schema_version": _SCHEMA_VERSION,
        "route_identity": record.route_identity,
        "model_sha256": record.model_sha256,
        "direction": record.direction.value,
        "level_index": record.level_index,
        "trigger_bps": str(record.trigger_bps),
        "allocated_weight": str(record.allocated_weight),
        "rearm_boundary_bps": str(record.rearm_boundary_bps),
        "state": record.state.value,
        "reserved_stress_usdt": str(record.reserved_stress_usdt),
        "pending_decision_cycle": record.pending_decision_cycle,
        "ownership": ownership,
        "generation": record.generation,
        "updated_at": record.updated_at.isoformat(),
        "execution_authorized": False,
    }


def _record_from_payload(raw: object) -> GridLevelRecord:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise RuntimeError("persisted grid level must be a mapping")
    payload = cast(dict[str, object], raw)
    expected = {
        "schema_version",
        "route_identity",
        "model_sha256",
        "direction",
        "level_index",
        "trigger_bps",
        "allocated_weight",
        "rearm_boundary_bps",
        "state",
        "reserved_stress_usdt",
        "pending_decision_cycle",
        "ownership",
        "generation",
        "updated_at",
        "execution_authorized",
    }
    if set(payload) != expected or payload["schema_version"] != _SCHEMA_VERSION:
        raise RuntimeError("persisted grid level schema is incompatible")
    if payload["execution_authorized"] is not False:
        raise RuntimeError("persisted grid level cannot authorize execution")
    level_index = _payload_int(payload, "level_index")
    generation = _payload_int(payload, "generation")
    pending_cycle_raw = payload["pending_decision_cycle"]
    if pending_cycle_raw is not None and (
        isinstance(pending_cycle_raw, bool)
        or not isinstance(pending_cycle_raw, int)
        or pending_cycle_raw < 0
    ):
        raise RuntimeError("persisted grid pending decision cycle is invalid")
    ownership_raw = payload["ownership"]
    ownership = None if ownership_raw is None else _ownership_from_payload(ownership_raw)
    return GridLevelRecord(
        route_identity=_payload_str(payload, "route_identity"),
        model_sha256=_payload_str(payload, "model_sha256"),
        direction=DivergenceDirection(_payload_str(payload, "direction")),
        level_index=level_index,
        trigger_bps=_payload_decimal(payload, "trigger_bps"),
        allocated_weight=_payload_decimal(payload, "allocated_weight"),
        rearm_boundary_bps=_payload_decimal(payload, "rearm_boundary_bps"),
        state=GridLevelState(_payload_str(payload, "state")),
        reserved_stress_usdt=_payload_decimal(payload, "reserved_stress_usdt"),
        pending_decision_cycle=pending_cycle_raw,
        ownership=ownership,
        generation=generation,
        updated_at=_payload_datetime(payload, "updated_at"),
    )


def _ownership_from_payload(raw: object) -> GridTrancheOwnership:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise RuntimeError("persisted grid ownership must be a mapping")
    payload = cast(dict[str, object], raw)
    expected = {
        "tranche_id",
        "normalized_base_quantity",
        "legs",
        "executable_entry_spread_bps",
        "reverse_target_bps",
        "effective_stop_bps",
        "maximum_holding_deadline",
        "reserved_stress_usdt",
        "entry_slippage_usdt",
        "realised_pnl_usdt",
        "unrealised_pnl_usdt",
        "opened_at",
    }
    if set(payload) != expected:
        raise RuntimeError("persisted grid ownership schema is incompatible")
    legs_raw = payload["legs"]
    if not isinstance(legs_raw, list) or len(legs_raw) != 2:
        raise RuntimeError("persisted grid ownership requires exactly two legs")
    legs = tuple(_leg_from_payload(value) for value in legs_raw)
    return GridTrancheOwnership(
        tranche_id=_payload_str(payload, "tranche_id"),
        normalized_base_quantity=_payload_decimal(payload, "normalized_base_quantity"),
        legs=(legs[0], legs[1]),
        executable_entry_spread_bps=_payload_decimal(payload, "executable_entry_spread_bps"),
        reverse_target_bps=_payload_decimal(payload, "reverse_target_bps"),
        effective_stop_bps=_payload_decimal(payload, "effective_stop_bps"),
        maximum_holding_deadline=_payload_datetime(payload, "maximum_holding_deadline"),
        reserved_stress_usdt=_payload_decimal(payload, "reserved_stress_usdt"),
        entry_slippage_usdt=_payload_decimal(payload, "entry_slippage_usdt"),
        realised_pnl_usdt=_payload_decimal(payload, "realised_pnl_usdt"),
        unrealised_pnl_usdt=_payload_decimal(payload, "unrealised_pnl_usdt"),
        opened_at=_payload_datetime(payload, "opened_at"),
    )


def _leg_from_payload(raw: object) -> GridLegFill:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise RuntimeError("persisted grid leg must be a mapping")
    payload = cast(dict[str, object], raw)
    if set(payload) != {
        "venue",
        "symbol",
        "side",
        "base_quantity",
        "average_price",
        "fee_usdt",
        "funding_usdt",
    }:
        raise RuntimeError("persisted grid leg schema is incompatible")
    return GridLegFill(
        venue=Venue(_payload_str(payload, "venue")),
        symbol=_payload_str(payload, "symbol"),
        side=Side(_payload_str(payload, "side")),
        base_quantity=_payload_decimal(payload, "base_quantity"),
        average_price=_payload_decimal(payload, "average_price"),
        fee_usdt=_payload_decimal(payload, "fee_usdt"),
        funding_usdt=_payload_decimal(payload, "funding_usdt"),
    )


def _payload_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"persisted grid value {key} must be a string")
    return value


def _payload_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"persisted grid value {key} must be a non-negative integer")
    return value


def _payload_decimal(payload: dict[str, object], key: str) -> Decimal:
    try:
        value = Decimal(_payload_str(payload, key))
    except Exception as error:
        raise RuntimeError(f"persisted grid value {key} must be a decimal") from error
    if not value.is_finite():
        raise RuntimeError(f"persisted grid value {key} must be finite")
    return value


def _payload_datetime(payload: dict[str, object], key: str) -> datetime:
    try:
        value = datetime.fromisoformat(_payload_str(payload, key))
    except ValueError as error:
        raise RuntimeError(f"persisted grid value {key} must be a datetime") from error
    if value.tzinfo is None or value.utcoffset() is None:
        raise RuntimeError(f"persisted grid value {key} must be timezone-aware")
    return value.astimezone(UTC)
