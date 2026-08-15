from __future__ import annotations

import threading
from dataclasses import dataclass
from decimal import Decimal

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reason_codes import ReasonCode


@dataclass(frozen=True, slots=True)
class RiskLimits:
    pair_stress_usdt: Decimal
    portfolio_stress_usdt: Decimal
    max_active_routes: int
    max_routes_per_base: int
    max_tranches_per_route: int
    local_free_margin_floor_ratio: Decimal
    effective_leverage_cap: Decimal

    def __post_init__(self) -> None:
        if min(self.pair_stress_usdt, self.portfolio_stress_usdt) <= 0:
            raise ValueError("stress limits must be positive")
        if self.pair_stress_usdt > self.portfolio_stress_usdt:
            raise ValueError("pair stress cannot exceed portfolio stress")
        if not 1 <= self.max_active_routes <= 10:
            raise ValueError("active route limit must be between one and ten")
        if self.max_routes_per_base != 1:
            raise ValueError("only one normal route per base is supported")
        if not 1 <= self.max_tranches_per_route <= 5:
            raise ValueError("tranche limit must be between one and five")
        if not Decimal("0.20") <= self.local_free_margin_floor_ratio < 1:
            raise ValueError("local free-margin floor must be at least 20 percent")
        if not 0 < self.effective_leverage_cap <= 3:
            raise ValueError("effective leverage cap must be at most 3x")


@dataclass(frozen=True, slots=True)
class VenueProjection:
    venue: Venue
    equity_usdt: Decimal
    projected_notional_usdt: Decimal
    projected_margin_used_usdt: Decimal
    venue_stress_usdt: Decimal

    def __post_init__(self) -> None:
        if self.equity_usdt <= 0:
            raise ValueError("venue equity must be positive")
        if (
            min(
                self.projected_notional_usdt,
                self.projected_margin_used_usdt,
                self.venue_stress_usdt,
            )
            < 0
        ):
            raise ValueError("venue projections must be non-negative")

    @property
    def effective_leverage(self) -> Decimal:
        return self.projected_notional_usdt / self.equity_usdt

    @property
    def stressed_free_margin_ratio(self) -> Decimal:
        free = self.equity_usdt - self.projected_margin_used_usdt - self.venue_stress_usdt
        return free / self.equity_usdt


@dataclass(frozen=True, slots=True)
class RiskRequest:
    reservation_id: str
    route_id: str
    base: str
    tranche_id: str
    projected_stress_usdt: Decimal
    venues: tuple[VenueProjection, ...]
    exit_depth_sufficient: bool

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (self.reservation_id, self.route_id, self.base, self.tranche_id)
        ):
            raise ValueError("risk reservation identifiers must be non-empty")
        if self.projected_stress_usdt <= 0:
            raise ValueError("projected stress must be positive")
        if len(self.venues) < 2:
            raise ValueError("paired risk requires at least two venue projections")
        if len({projection.venue for projection in self.venues}) != len(self.venues):
            raise ValueError("venue projections must be unique")


@dataclass(frozen=True, slots=True)
class RiskDecision:
    accepted: bool
    reason: ReasonCode
    breakdown: dict[str, Decimal]


class RiskBook:
    """In-memory atomic reservation book; persistence/reconciliation wraps it in C3."""

    def __init__(self, limits: RiskLimits) -> None:
        self._limits = limits
        self._reservations: dict[str, RiskRequest] = {}
        self._unmatched_exposure = False
        self._unknown_order_state = False
        self._lock = threading.RLock()

    @property
    def reservations(self) -> tuple[RiskRequest, ...]:
        with self._lock:
            return tuple(self._reservations.values())

    def set_execution_block(self, *, unmatched_exposure: bool, unknown_order_state: bool) -> None:
        with self._lock:
            self._unmatched_exposure = unmatched_exposure
            self._unknown_order_state = unknown_order_state

    def reserve(self, request: RiskRequest) -> RiskDecision:
        with self._lock:
            existing = tuple(self._reservations.values())
            route_stress = (
                sum(
                    (
                        reservation.projected_stress_usdt
                        for reservation in existing
                        if reservation.route_id == request.route_id
                    ),
                    Decimal(0),
                )
                + request.projected_stress_usdt
            )
            portfolio_stress = (
                sum(
                    (reservation.projected_stress_usdt for reservation in existing),
                    Decimal(0),
                )
                + request.projected_stress_usdt
            )
            breakdown = {
                "projected_route_stress_usdt": route_stress,
                "projected_portfolio_stress_usdt": portfolio_stress,
                "pair_limit_usdt": self._limits.pair_stress_usdt,
                "portfolio_limit_usdt": self._limits.portfolio_stress_usdt,
            }

            reason = self._rejection_reason(request, existing, route_stress, portfolio_stress)
            if reason is not None:
                return RiskDecision(False, reason, breakdown)
            self._reservations[request.reservation_id] = request
            return RiskDecision(True, ReasonCode.RISK_RESERVED, breakdown)

    def _rejection_reason(
        self,
        request: RiskRequest,
        existing: tuple[RiskRequest, ...],
        route_stress: Decimal,
        portfolio_stress: Decimal,
    ) -> ReasonCode | None:
        if request.reservation_id in self._reservations:
            return ReasonCode.RESERVATION_EXISTS
        if self._unmatched_exposure or self._unknown_order_state:
            return ReasonCode.UNRESOLVED_EXECUTION_STATE
        if not request.exit_depth_sufficient:
            return ReasonCode.DEPTH_INSUFFICIENT
        if route_stress > self._limits.pair_stress_usdt:
            return ReasonCode.PAIR_STRESS_LIMIT
        if portfolio_stress > self._limits.portfolio_stress_usdt:
            return ReasonCode.PORTFOLIO_STRESS_LIMIT

        active_routes = {reservation.route_id for reservation in existing}
        if (
            request.route_id not in active_routes
            and len(active_routes) >= self._limits.max_active_routes
        ):
            return ReasonCode.ROUTE_LIMIT
        base_routes = {
            reservation.route_id for reservation in existing if reservation.base == request.base
        }
        if (
            request.route_id not in base_routes
            and len(base_routes) >= self._limits.max_routes_per_base
        ):
            return ReasonCode.BASE_ROUTE_LIMIT
        route_tranches = {
            reservation.tranche_id
            for reservation in existing
            if reservation.route_id == request.route_id
        }
        if (
            request.tranche_id not in route_tranches
            and len(route_tranches) >= self._limits.max_tranches_per_route
        ):
            return ReasonCode.TRANCHE_LIMIT
        for projection in request.venues:
            if projection.effective_leverage > self._limits.effective_leverage_cap:
                return ReasonCode.EFFECTIVE_LEVERAGE_LIMIT
            if projection.stressed_free_margin_ratio < self._limits.local_free_margin_floor_ratio:
                return ReasonCode.LOCAL_MARGIN_FLOOR
        return None

    def release(self, reservation_id: str) -> RiskRequest:
        with self._lock:
            return self._reservations.pop(reservation_id)

    def reconcile_stress(self, reservation_id: str, actual_stress_usdt: Decimal) -> None:
        if actual_stress_usdt <= 0:
            raise ValueError("actual stress must be positive")
        with self._lock:
            current = self._reservations[reservation_id]
            self._reservations[reservation_id] = RiskRequest(
                reservation_id=current.reservation_id,
                route_id=current.route_id,
                base=current.base,
                tranche_id=current.tranche_id,
                projected_stress_usdt=actual_stress_usdt,
                venues=current.venues,
                exit_depth_sufficient=current.exit_depth_sufficient,
            )

    def totals(self) -> tuple[dict[str, Decimal], Decimal]:
        with self._lock:
            per_route: dict[str, Decimal] = {}
            for reservation in self._reservations.values():
                per_route[reservation.route_id] = (
                    per_route.get(reservation.route_id, Decimal(0))
                    + reservation.projected_stress_usdt
                )
            return per_route, sum(per_route.values(), Decimal(0))
