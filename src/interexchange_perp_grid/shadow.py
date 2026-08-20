from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from collections import deque
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from interexchange_perp_grid.bbo_prefilter import BboPrefilterObservation
from interexchange_perp_grid.candidate_l2 import CandidateL2Result, RouteStableKey
from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import (
    ExecutionIntent,
    OrderPurpose,
    PairActionState,
    PairExecutionCoordinator,
    Side,
    SimulatedOrderResult,
    SimulatedOrderStatus,
    Tranche,
)
from interexchange_perp_grid.live_journal import LiveOrderJournal
from interexchange_perp_grid.observability import get_logger
from interexchange_perp_grid.public_engine import PublicMarketEngine, PublicWorkload, ScanResult
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import (
    RiskBook,
    RiskLimits,
    RiskRequest,
    VenueProjection,
)
from interexchange_perp_grid.route_calibration import (
    PersistentRouteCalibrator,
    RouteCalibrationAssessment,
    RouteCalibrationObservation,
    RouteCalibrationSamplingPolicy,
)
from interexchange_perp_grid.routes import DirectedRouteQuote
from interexchange_perp_grid.state import (
    RuntimeControls,
    StateTransitionDeadlineError,
    acquire_state_recovery_lease,
    clear_persistence_indeterminate,
    delete_tranche,
    initialise_state,
    load_shadow_portfolio,
    mark_persistence_indeterminate,
    persistence_indeterminate_marker_exists,
    read_active_qualification_epoch,
    read_runtime_controls,
    read_shadow_snapshot,
    record_qualification_exception,
    record_qualification_scan,
    release_state_recovery_lease,
    save_shadow_snapshot,
    save_tranche,
    update_runtime_controls,
)
from interexchange_perp_grid.strategy import (
    CostInputs,
    DirectedRouteKey,
    GridParameters,
    SignalDecision,
    evaluate_entry_signal,
)


async def _await_owned_task(task: asyncio.Task[None]) -> bool:
    """Wait through caller cancellation and report whether it was requested."""
    interrupted = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            interrupted = True
        except Exception:
            break
    return interrupted


class WorkClass(StrEnum):
    CLOSE = "CLOSE"
    HEDGE = "HEDGE"
    RECONCILE = "RECONCILE"
    PRIVATE_STREAM = "PRIVATE_STREAM"
    NEW_ENTRY = "NEW_ENTRY"
    CANDIDATE_L2 = "CANDIDATE_L2"
    BROAD_BBO = "BROAD_BBO"


class MarketEngine(Protocol):
    async def scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey] = frozenset(),
        entry_work_admitted: bool = True,
    ) -> ScanResult: ...

    async def scan_candidate_l2(
        self,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[RouteStableKey] = frozenset(),
        candidates_admitted: bool = True,
        prefilter: tuple[BboPrefilterObservation, ...] | None = None,
        preserve_existing_candidates: bool = False,
    ) -> CandidateL2Result: ...

    async def scan_route_calibration_observations(
        self,
        timeout_seconds: int,
        *,
        epoch_id: str | None = None,
    ) -> tuple[RouteCalibrationObservation, ...]: ...

    def public_workload(self) -> PublicWorkload: ...

    async def set_broad_bbo_admitted(self, admitted: bool) -> None: ...

    async def close(self) -> None: ...


CRITICAL_WORK = {
    WorkClass.CLOSE,
    WorkClass.HEDGE,
    WorkClass.RECONCILE,
    WorkClass.PRIVATE_STREAM,
}

WORK_PRIORITY = {
    WorkClass.CLOSE: 0,
    WorkClass.HEDGE: 1,
    WorkClass.RECONCILE: 2,
    WorkClass.PRIVATE_STREAM: 3,
    WorkClass.NEW_ENTRY: 4,
    WorkClass.CANDIDATE_L2: 5,
    WorkClass.BROAD_BBO: 6,
}


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    accepted: bool
    reason: ReasonCode


class OverloadController:
    def __init__(self, pending_limit: int) -> None:
        if pending_limit <= 0:
            raise ValueError("pending work limit must be positive")
        self._pending_limit = pending_limit
        self._pending = 0
        self._pending_by_class: dict[WorkClass, int] = {}

    @property
    def overloaded(self) -> bool:
        return self._pending > self._pending_limit

    def update_pending(self, pending: int) -> None:
        if pending < 0:
            raise ValueError("pending work cannot be negative")
        self._pending = pending
        self._pending_by_class = {}

    def update_pending_by_class(self, pending: dict[WorkClass, int]) -> None:
        if any(count < 0 for count in pending.values()):
            raise ValueError("pending work cannot be negative")
        self._pending_by_class = {work: count for work, count in pending.items() if count}
        self._pending = sum(self._pending_by_class.values())

    def shed_plan(self) -> tuple[WorkClass, ...]:
        excess = max(0, self._pending - self._pending_limit)
        shed: list[WorkClass] = []
        for work in sorted(
            (WorkClass.BROAD_BBO, WorkClass.CANDIDATE_L2),
            key=WORK_PRIORITY.__getitem__,
            reverse=True,
        ):
            count = min(excess, self._pending_by_class.get(work, 0))
            shed.extend((work,) * count)
            excess -= count
        return tuple(shed)

    def admit(self, work: WorkClass) -> AdmissionDecision:
        if work in CRITICAL_WORK:
            return AdmissionDecision(True, ReasonCode.SHADOW_EVALUATED)
        if self._pending + 1 <= self._pending_limit:
            return AdmissionDecision(True, ReasonCode.SHADOW_EVALUATED)
        if work == WorkClass.BROAD_BBO:
            return AdmissionDecision(False, ReasonCode.OVERLOAD_BROAD_SHED)
        if work == WorkClass.CANDIDATE_L2:
            remaining_excess = max(
                0,
                self._pending
                + 1
                - self._pending_limit
                - self._pending_by_class.get(WorkClass.BROAD_BBO, 0),
            )
            if remaining_excess > 0:
                return AdmissionDecision(False, ReasonCode.OVERLOAD_CANDIDATE_SHED)
            return AdmissionDecision(True, ReasonCode.SHADOW_EVALUATED)
        lower_priority_pending = sum(
            self._pending_by_class.get(pending_work, 0)
            for pending_work in (WorkClass.BROAD_BBO, WorkClass.CANDIDATE_L2)
        )
        if (
            not self._pending_by_class
            or self._pending + 1 - lower_priority_pending > self._pending_limit
        ):
            return AdmissionDecision(False, ReasonCode.OVERLOAD_ENTRY_DISABLED)
        return AdmissionDecision(True, ReasonCode.SHADOW_EVALUATED)


ACTIVE_STATES = {
    PairActionState.PARTIALLY_HEDGED,
    PairActionState.HEDGED,
    PairActionState.CLOSING,
    PairActionState.UNKNOWN_ORDER,
    PairActionState.RECOVERING,
    PairActionState.EMERGENCY_HEDGED,
}


class ShadowRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.state_path = Path(settings.storage.sqlite_path)
        self.overload = OverloadController(settings.shadow.overload_pending_limit)
        self.risk = RiskBook(
            RiskLimits(
                pair_stress_usdt=settings.risk.pair_stressed_loss_limit_usdt,
                portfolio_stress_usdt=settings.risk.portfolio_stressed_loss_limit_usdt,
                max_active_routes=settings.risk.max_active_routes,
                max_routes_per_base=settings.risk.max_routes_per_base,
                max_tranches_per_route=settings.risk.max_tranches_per_route,
                local_free_margin_floor_ratio=settings.risk.local_free_margin_floor_ratio,
                effective_leverage_cap=settings.risk.initial_effective_leverage_cap,
            )
        )
        self.tranches: dict[str, Tranche] = {}
        self._started = False
        self._persistence_indeterminate = False
        self._portfolio_restored_consistently = False
        self._portfolio_transition_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._started:
            return
        await initialise_state(self.state_path)
        controls = await read_runtime_controls(self.state_path)
        self._persistence_indeterminate = (
            controls.reconciliation_state == "INDETERMINATE"
            or persistence_indeterminate_marker_exists(self.state_path)
        )
        restored, reservations = await load_shadow_portfolio(self.state_path)
        self.tranches = {tranche.tranche_id: tranche for tranche in restored}
        active = tuple(tranche for tranche in restored if tranche.state in ACTIVE_STATES)
        active_ids = {tranche.tranche_id for tranche in active}
        reservation_ids = {request.tranche_id for request in reservations}
        structurally_matched = active_ids == reservation_ids and all(
            request.reservation_id == request.tranche_id
            and request.tranche_id in self.tranches
            and request.route_id == self.tranches[request.tranche_id].route.value
            and request.base == self.tranches[request.tranche_id].route.base
            and request.projected_stress_usdt
            == self.tranches[request.tranche_id].projected_stress_usdt
            and {projection.venue for projection in request.venues}
            == {
                self.tranches[request.tranche_id].route.long_venue,
                self.tranches[request.tranche_id].route.short_venue,
            }
            for request in reservations
        )
        if structurally_matched:
            try:
                self.risk.restore(reservations)
            except ValueError:
                structurally_matched = False
        self._portfolio_restored_consistently = structurally_matched
        await update_runtime_controls(
            self.state_path,
            reconciliation_state=(
                "INDETERMINATE"
                if self._persistence_indeterminate
                else "PENDING"
                if active or not structurally_matched
                else "CONSISTENT"
            ),
        )
        self._started = True

    async def reconcile(self, observed_active_ids: set[str]) -> ReasonCode:
        expected = {
            tranche.tranche_id
            for tranche in self.tranches.values()
            if tranche.state in ACTIVE_STATES
        }
        structurally_valid = all(
            self._tranche_is_consistent(self.tranches[tranche_id]) for tranche_id in expected
        )
        risk_ids = {request.tranche_id for request in self.risk.reservations}
        consistent = (
            not self._persistence_indeterminate
            and self._portfolio_restored_consistently
            and expected == observed_active_ids
            and expected == risk_ids
            and structurally_valid
        )
        await update_runtime_controls(
            self.state_path,
            reconciliation_state=(
                "CONSISTENT"
                if consistent
                else "INDETERMINATE"
                if self._persistence_indeterminate
                else "INCONSISTENT"
            ),
        )
        return ReasonCode.RECONCILIATION_PASSED if consistent else ReasonCode.RECONCILIATION_FAILED

    @staticmethod
    def _tranche_is_consistent(tranche: Tranche) -> bool:
        if tranche.state == PairActionState.HEDGED:
            return tranche.paired_quantity > 0 and tranche.residual_quantity == 0
        if tranche.state == PairActionState.CLOSING:
            return (
                tranche.paired_quantity > 0
                and tranche.residual_quantity == 0
                and tranche.closed_quantity < tranche.paired_quantity
            )
        if tranche.state == PairActionState.EMERGENCY_HEDGED:
            return tranche.residual_quantity == 0
        return False

    async def controls(self) -> RuntimeControls:
        return await read_runtime_controls(self.state_path)

    async def entry_gate(self) -> AdmissionDecision:
        if self._persistence_indeterminate:
            return AdmissionDecision(False, ReasonCode.RECONCILIATION_REQUIRED)
        controls = await self.controls()
        if controls.killed:
            return AdmissionDecision(False, ReasonCode.KILL_SWITCH_ACTIVE)
        if controls.paused:
            return AdmissionDecision(False, ReasonCode.ENTRY_PAUSED)
        if controls.reconciliation_state != "CONSISTENT":
            return AdmissionDecision(False, ReasonCode.RECONCILIATION_REQUIRED)
        return self.overload.admit(WorkClass.NEW_ENTRY)

    async def pause(self) -> None:
        await update_runtime_controls(self.state_path, paused=True)

    async def resume(self) -> None:
        await update_runtime_controls(self.state_path, paused=False)

    async def kill(self) -> None:
        await update_runtime_controls(self.state_path, killed=True, paused=True)

    async def block_for_indeterminate_persistence(self) -> None:
        self._persistence_indeterminate = True
        mark_persistence_indeterminate(self.state_path)

    async def resolve_indeterminate_persistence(
        self,
        observed_active_ids: set[str],
    ) -> ReasonCode:
        async with self._portfolio_transition_lock:
            return await self._resolve_indeterminate_persistence_locked(observed_active_ids)

    async def _resolve_indeterminate_persistence_locked(
        self,
        observed_active_ids: set[str],
    ) -> ReasonCode:
        if not acquire_state_recovery_lease(self.state_path):
            return ReasonCode.RECONCILIATION_FAILED
        try:
            try:
                durable_tranches, durable_requests = await load_shadow_portfolio(
                    self.state_path,
                    busy_timeout_ms=50,
                )
            except (OSError, sqlite3.Error, ValueError):
                return ReasonCode.RECONCILIATION_FAILED
            expected = {
                tranche.tranche_id
                for tranche in self.tranches.values()
                if tranche.state in ACTIVE_STATES
            }
            risk_ids = {request.tranche_id for request in self.risk.reservations}
            structurally_valid = all(
                self._tranche_is_consistent(self.tranches[tranche_id]) for tranche_id in expected
            )
            if (
                not self._portfolio_restored_consistently
                or tuple(sorted(self.tranches.values(), key=lambda item: item.tranche_id))
                != durable_tranches
                or tuple(sorted(self.risk.reservations, key=lambda item: item.reservation_id))
                != durable_requests
                or expected != observed_active_ids
                or expected != risk_ids
                or not structurally_valid
            ):
                await update_runtime_controls(
                    self.state_path,
                    reconciliation_state="INDETERMINATE",
                )
                return ReasonCode.RECONCILIATION_FAILED
            await update_runtime_controls(self.state_path, reconciliation_state="CONSISTENT")
            clear_persistence_indeterminate(self.state_path)
            self._persistence_indeterminate = False
            return ReasonCode.RECONCILIATION_PASSED
        finally:
            release_state_recovery_lease(self.state_path)

    @property
    def portfolio_restored_consistently(self) -> bool:
        return self._portfolio_restored_consistently

    async def close_all_simulated(self) -> tuple[str, ...]:
        async with self._portfolio_transition_lock:
            return await self._close_all_simulated_locked()

    async def _close_all_simulated_locked(self) -> tuple[str, ...]:
        persisted = await read_shadow_snapshot(self.state_path)
        opportunities = persisted.get("opportunities", []) if persisted is not None else []
        if not isinstance(opportunities, list):
            return ()
        coordinator = PairExecutionCoordinator()
        closed: list[str] = []
        for tranche in self.tranches.values():
            if tranche.state not in {
                PairActionState.HEDGED,
                PairActionState.CLOSING,
                PairActionState.RECOVERING,
            }:
                continue
            remaining = tranche.paired_quantity - tranche.closed_quantity
            quote = next(
                (
                    item
                    for item in opportunities
                    if isinstance(item, dict)
                    and isinstance(item.get("key"), dict)
                    and str(item["key"].get("base")) == tranche.route.base
                    and str(item.get("long_venue")) == tranche.route.long_venue.value
                    and str(item.get("short_venue")) == tranche.route.short_venue.value
                ),
                None,
            )
            if remaining <= 0 or quote is None:
                continue
            long_price_value = quote.get("exit_long_vwap")
            short_price_value = quote.get("exit_short_vwap")
            if long_price_value is None or short_price_value is None:
                continue
            long_price = Decimal(str(long_price_value))
            short_price = Decimal(str(short_price_value))
            long_result = _simulated_close_result(
                tranche,
                tranche.route.long_venue,
                Side.SELL,
                remaining,
                long_price,
                "long",
            )
            short_result = _simulated_close_result(
                tranche,
                tranche.route.short_venue,
                Side.BUY,
                remaining,
                short_price,
                "short",
            )
            coordinator.force_close(tranche, long_result, short_result)
            try:
                await save_tranche(self.state_path, tranche)
            except StateTransitionDeadlineError as error:
                await self.block_for_indeterminate_persistence()
                raise RuntimeError(
                    "shadow close-all persistence outcome is indeterminate"
                ) from error
            except Exception as error:
                await self.block_for_indeterminate_persistence()
                raise RuntimeError(
                    "shadow close-all persistence commit state is unknown"
                ) from error
            with contextlib.suppress(KeyError):
                self.risk.release(tranche.tranche_id)
            closed.append(tranche.tranche_id)
        await self.pause()
        return tuple(closed)

    async def snapshot(self) -> dict[str, object]:
        controls = await self.controls()
        persisted = await read_shadow_snapshot(self.state_path)
        per_route_stress, portfolio_stress = self.risk.totals()
        return {
            "mode": self.settings.app.mode,
            "paused": controls.paused,
            "killed": controls.killed,
            "reconciliation_state": controls.reconciliation_state,
            "overloaded": self.overload.overloaded,
            "persistence_indeterminate": self._persistence_indeterminate,
            "risk": {
                "reservation_count": len(self.risk.reservations),
                "per_route_stress_usdt": {
                    route: str(stress) for route, stress in sorted(per_route_stress.items())
                },
                "portfolio_stress_usdt": str(portfolio_stress),
            },
            "positions": [
                {
                    "tranche_id": tranche.tranche_id,
                    "route": tranche.route.value,
                    "state": tranche.state.value,
                    "paired_quantity": str(tranche.paired_quantity),
                    "residual_quantity": str(tranche.residual_quantity),
                    "net_pnl_usdt": str(tranche.pnl().net_pnl_usdt),
                }
                for tranche in sorted(self.tranches.values(), key=lambda item: item.tranche_id)
            ],
            "market": persisted,
        }


def _simulated_close_result(
    tranche: Tranche,
    venue: Venue,
    side: Side,
    quantity: Decimal,
    price: Decimal,
    suffix: str,
) -> SimulatedOrderResult:
    intent = ExecutionIntent(
        client_order_id=f"shadow-close-{tranche.tranche_id}-{suffix}-{len(tranche.all_fills)}",
        venue=venue,
        side=side,
        purpose=OrderPurpose.EMERGENCY_CLOSE,
        quantity=quantity,
        worst_acceptable_price=price,
    )
    return SimulatedOrderResult(
        intent=intent,
        status=SimulatedOrderStatus.FILLED,
        actual_fill_quantity=quantity,
        fill_price=price,
        fee_usdt=quantity * price * Decimal("0.001"),
    )


class ShadowTrader:
    """Runs the calibrated C2 decision and fill path against live public snapshots."""

    def __init__(self, settings: Settings, runtime: ShadowRuntime) -> None:
        self.settings = settings
        self.runtime = runtime
        self._risk = runtime.risk
        self._coordinator = PairExecutionCoordinator()
        self._managed_ids: set[str] = (
            {
                tranche.tranche_id
                for tranche in runtime.tranches.values()
                if tranche.state in ACTIVE_STATES
            }
            if runtime.portfolio_restored_consistently
            else set()
        )

    async def process(
        self,
        result: ScanResult,
        *,
        decision_deadline: float | None = None,
    ) -> tuple[SignalDecision, ...]:
        await self.close_active(result.quotes)
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            return ()
        gate = await self.runtime.entry_gate()
        if not gate.accepted or (
            decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline
        ):
            return ()
        decisions: list[SignalDecision] = []
        async with self.runtime._portfolio_transition_lock:
            gate = await self.runtime.entry_gate()
            if not gate.accepted:
                return ()
            for quote in result.quotes:
                route = DirectedRouteKey(quote.key.base, quote.long_venue, quote.short_venue)
                matches = tuple(
                    assessment
                    for assessment in result.route_calibration
                    if assessment.route == route
                    and assessment.latest_base_quantity == quote.base_quantity
                )
                if len(matches) != 1:
                    continue
                decision = await self._evaluate_and_open(
                    quote,
                    matches[0],
                    decision_deadline=decision_deadline,
                )
                if decision is not None:
                    decisions.append(decision)
        return tuple(decisions)

    async def close_active(self, quotes: tuple[DirectedRouteQuote, ...]) -> None:
        async with self.runtime._portfolio_transition_lock:
            await self._close_converged(quotes)

    async def _evaluate_and_open(
        self,
        quote: DirectedRouteQuote,
        calibration: RouteCalibrationAssessment,
        *,
        decision_deadline: float | None = None,
    ) -> SignalDecision | None:
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            return None
        values = (
            quote.entry_long_vwap,
            quote.entry_short_vwap,
            quote.exit_long_vwap,
            quote.exit_short_vwap,
            quote.entry_spread_bps,
            quote.four_leg_fee_estimate,
            quote.funding_rate_delta,
        )
        if not quote.eligible or any(value is None for value in values):
            return None
        assert quote.entry_long_vwap is not None
        assert quote.entry_short_vwap is not None
        assert quote.exit_long_vwap is not None
        assert quote.exit_short_vwap is not None
        assert quote.entry_spread_bps is not None
        assert quote.four_leg_fee_estimate is not None
        assert quote.funding_rate_delta is not None
        route = DirectedRouteKey(quote.key.base, quote.long_venue, quote.short_venue)
        if (
            not calibration.ready
            or calibration.parameters is None
            or calibration.route != route
            or calibration.latest_base_quantity != quote.base_quantity
        ):
            return None

        midpoint_notional = (
            quote.base_quantity * (quote.entry_long_vwap + quote.entry_short_vwap) / Decimal(2)
        )
        if midpoint_notional <= 0:
            return None
        calibrated = calibration.parameters
        bucket_convergence_p90 = calibrated.convergence_p90_for_spread(quote.entry_spread_bps)
        if bucket_convergence_p90 is None:
            return None
        parameters = GridParameters(
            route=route,
            size_bucket=quote.base_quantity,
            version=calibrated.version,
            sample_count=calibrated.sample_count,
            median_spread_bps=calibrated.window_24h.median_spread_bps,
            mad_spread_bps=calibrated.window_24h.mad_spread_bps,
            entry_quantile_bps=calibrated.entry_levels_bps[0],
            exit_quantile_bps=calibrated.target_close_bps(quote.entry_spread_bps),
            convergence_p90_seconds=bucket_convergence_p90,
            grid_step_bps=calibrated.grid_step_bps,
            minimum_profit_usdt=calibrated.minimum_profit_usdt,
        )
        expected_funding_cost = max(
            Decimal(0),
            -quote.funding_rate_delta * midpoint_notional,
        )
        calibrated_stress_usdt = (
            midpoint_notional * calibrated.stressed_cost_floor_bps / Decimal(10_000)
        )
        residual_stress = max(
            Decimal(0),
            calibrated_stress_usdt - quote.four_leg_fee_estimate - expected_funding_cost,
        )
        inputs = CostInputs(
            quantity=quote.base_quantity,
            entry_long_price=quote.entry_long_vwap,
            entry_short_price=quote.entry_short_vwap,
            target_exit_long_price=quote.exit_long_vwap,
            target_exit_short_price=quote.exit_short_vwap,
            long_fee_rate=Decimal(0),
            short_fee_rate=Decimal(0),
            entry_impact_usdt=Decimal(0),
            exit_impact_usdt=Decimal(0),
            expected_funding_cost_usdt=expected_funding_cost,
            funding_stress_usdt=Decimal(0),
            latency_reserve_usdt=Decimal(0),
            unmatched_hedge_reserve_usdt=Decimal(0),
            reconciliation_forced_exit_reserve_usdt=Decimal(0),
            liquidation_distance_reserve_usdt=Decimal(0),
            precomputed_four_leg_fee_usdt=quote.four_leg_fee_estimate,
            emergency_hedge_reserve_usdt=residual_stress,
        )
        preliminary = evaluate_entry_signal(
            inputs,
            parameters,
            self.settings.strategy.stressed_cost_multiplier,
            True,
            ReasonCode.RISK_RESERVED,
            {},
        )
        if not preliminary.accepted:
            return preliminary

        tranche_id = uuid4().hex
        projected_stress = preliminary.cost.stressed_total_cost_usdt + midpoint_notional * max(
            Decimal(0), calibrated.route_stop_bps - quote.entry_spread_bps
        ) / Decimal(10_000)
        equity = self.settings.risk.reference_capital_usdt / 2
        venue_stress = projected_stress / 2
        request = RiskRequest(
            reservation_id=tranche_id,
            route_id=route.value,
            base=route.base,
            tranche_id=tranche_id,
            projected_stress_usdt=projected_stress,
            venues=(
                self._venue_projection(
                    quote.long_venue,
                    quote.base_quantity * quote.entry_long_vwap,
                    equity,
                    venue_stress,
                ),
                self._venue_projection(
                    quote.short_venue,
                    quote.base_quantity * quote.entry_short_vwap,
                    equity,
                    venue_stress,
                ),
            ),
            exit_depth_sufficient=True,
        )
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            return None
        risk = self._risk.reserve(request)
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            if risk.accepted:
                self._risk.release(tranche_id)
            return None
        decision = evaluate_entry_signal(
            inputs,
            parameters,
            self.settings.strategy.stressed_cost_multiplier,
            risk.accepted,
            risk.reason,
            risk.breakdown,
        )
        if not decision.accepted:
            if risk.accepted:
                self._risk.release(tranche_id)
            return decision

        tranche = Tranche(
            tranche_id=tranche_id,
            route=route,
            requested_quantity=quote.base_quantity,
            target_close_spread=parameters.exit_quantile_bps,
            stop_spread=calibrated.route_stop_bps,
            projected_stress_usdt=projected_stress,
        )
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            self._risk.release(tranche_id)
            return None
        self._coordinator.precheck_and_reserve(tranche, risk)
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            self._risk.release(tranche_id)
            return None
        entry_fee = quote.four_leg_fee_estimate / 4
        submitted = self._coordinator.submit_open(
            tranche,
            _shadow_fill_result(
                tranche_id,
                "open-long",
                quote.long_venue,
                Side.BUY,
                OrderPurpose.NORMAL_OPEN,
                quote.base_quantity,
                quote.entry_long_vwap,
                entry_fee,
            ),
            _shadow_fill_result(
                tranche_id,
                "open-short",
                quote.short_venue,
                Side.SELL,
                OrderPurpose.NORMAL_OPEN,
                quote.base_quantity,
                quote.entry_short_vwap,
                entry_fee,
            ),
            mutation_guard=(
                None
                if decision_deadline is None
                else lambda: asyncio.get_running_loop().time() < decision_deadline
            ),
        )
        if not submitted:
            self._risk.release(tranche_id)
            return None
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            self._coordinator.rollback_unpublished_open(tranche)
            self._risk.release(tranche_id)
            return None
        persistence = asyncio.create_task(
            save_tranche(
                self.runtime.state_path,
                tranche,
                deadline_monotonic=decision_deadline,
                risk_reservation=request,
            ),
            name=f"shadow-save-tranche-{tranche_id}",
        )
        persistence_interrupted = await _await_owned_task(persistence)
        if persistence.cancelled():
            self._coordinator.rollback_unpublished_open(tranche)
            self._risk.release(tranche_id)
            raise asyncio.CancelledError
        try:
            persistence.result()
        except StateTransitionDeadlineError as error:
            self.runtime.tranches[tranche_id] = tranche
            self._managed_ids.add(tranche_id)
            await self.runtime.block_for_indeterminate_persistence()
            raise RuntimeError("shadow tranche persistence outcome is indeterminate") from error
        except TimeoutError:
            self._coordinator.rollback_unpublished_open(tranche)
            self._risk.release(tranche_id)
            if persistence_interrupted:
                raise asyncio.CancelledError from None
            return None
        except Exception as error:
            # A storage exception can be raised before or after the native
            # transaction commits.  Retain conservative ownership and block
            # all new entries until durable truth is reconciled.
            self.runtime.tranches[tranche_id] = tranche
            self._managed_ids.add(tranche_id)
            await self.runtime.block_for_indeterminate_persistence()
            raise RuntimeError("shadow tranche persistence commit state is unknown") from error
        if persistence_interrupted:
            # Persistence succeeded despite caller cancellation.  Keep it
            # explicitly managed and risk-reserved before propagating cancel.
            self.runtime.tranches[tranche_id] = tranche
            self._managed_ids.add(tranche_id)
            raise asyncio.CancelledError
        if decision_deadline is not None and asyncio.get_running_loop().time() >= decision_deadline:
            deletion = asyncio.create_task(
                delete_tranche(self.runtime.state_path, tranche_id),
                name=f"shadow-delete-tranche-{tranche_id}",
            )
            deletion_interrupted = await _await_owned_task(deletion)
            if deletion.cancelled():
                self.runtime.tranches[tranche_id] = tranche
                self._managed_ids.add(tranche_id)
                raise asyncio.CancelledError
            try:
                deletion.result()
            except StateTransitionDeadlineError as error:
                self.runtime.tranches[tranche_id] = tranche
                self._managed_ids.add(tranche_id)
                await self.runtime.block_for_indeterminate_persistence()
                raise RuntimeError("shadow tranche deletion outcome is indeterminate") from error
            except Exception:
                # Persistence succeeded, so retain explicit in-memory/risk
                # ownership if compensating deletion itself fails.
                self.runtime.tranches[tranche_id] = tranche
                self._managed_ids.add(tranche_id)
                await self.runtime.block_for_indeterminate_persistence()
                raise
            self._coordinator.rollback_unpublished_open(tranche)
            self._risk.release(tranche_id)
            if deletion_interrupted:
                raise asyncio.CancelledError
            return None
        self.runtime.tranches[tranche_id] = tranche
        self._managed_ids.add(tranche_id)
        return decision

    def _venue_projection(
        self,
        venue: Venue,
        new_notional: Decimal,
        equity: Decimal,
        venue_stress: Decimal,
    ) -> VenueProjection:
        current_notional = sum(
            (
                fill.quantity * fill.price
                for tranche in self.runtime.tranches.values()
                if tranche.state in ACTIVE_STATES
                for fill in tranche.entry_long_fills + tranche.entry_short_fills
                if fill.venue == venue
            ),
            Decimal(0),
        )
        projected_notional = current_notional + new_notional
        return VenueProjection(
            venue=venue,
            equity_usdt=equity,
            projected_notional_usdt=projected_notional,
            projected_margin_used_usdt=(
                projected_notional / self.settings.risk.initial_effective_leverage_cap
            ),
            venue_stress_usdt=venue_stress,
        )

    async def _close_converged(
        self,
        quotes: tuple[DirectedRouteQuote, ...],
    ) -> None:
        for tranche in self.runtime.tranches.values():
            if tranche.state not in {PairActionState.HEDGED, PairActionState.CLOSING}:
                continue
            quote = next(
                (
                    candidate
                    for candidate in quotes
                    if candidate.key.base == tranche.route.base
                    and candidate.long_venue == tranche.route.long_venue
                    and candidate.short_venue == tranche.route.short_venue
                ),
                None,
            )
            if (
                quote is None
                or quote.exit_long_vwap is None
                or quote.exit_short_vwap is None
                or quote.four_leg_fee_estimate is None
            ):
                continue
            close_spread_bps = (
                (quote.exit_short_vwap - quote.exit_long_vwap)
                / quote.exit_long_vwap
                * Decimal(10_000)
            )
            if close_spread_bps > tranche.target_close_spread:
                continue
            remaining = tranche.paired_quantity - tranche.closed_quantity
            close_fee = quote.four_leg_fee_estimate / 4
            self._coordinator.close(
                tranche,
                _shadow_fill_result(
                    tranche.tranche_id,
                    "close-long",
                    tranche.route.long_venue,
                    Side.SELL,
                    OrderPurpose.NORMAL_CLOSE,
                    remaining,
                    quote.exit_long_vwap,
                    close_fee,
                ),
                _shadow_fill_result(
                    tranche.tranche_id,
                    "close-short",
                    tranche.route.short_venue,
                    Side.BUY,
                    OrderPurpose.NORMAL_CLOSE,
                    remaining,
                    quote.exit_short_vwap,
                    close_fee,
                ),
            )
            try:
                await save_tranche(self.runtime.state_path, tranche)
            except StateTransitionDeadlineError as error:
                await self.runtime.block_for_indeterminate_persistence()
                raise RuntimeError("shadow close persistence outcome is indeterminate") from error
            except Exception as error:
                await self.runtime.block_for_indeterminate_persistence()
                raise RuntimeError("shadow close persistence commit state is unknown") from error
            with contextlib.suppress(KeyError):
                self._risk.release(tranche.tranche_id)


def _shadow_fill_result(
    tranche_id: str,
    suffix: str,
    venue: Venue,
    side: Side,
    purpose: OrderPurpose,
    quantity: Decimal,
    price: Decimal,
    fee: Decimal,
) -> SimulatedOrderResult:
    return SimulatedOrderResult(
        intent=ExecutionIntent(
            client_order_id=f"shadow-{tranche_id}-{suffix}",
            venue=venue,
            side=side,
            purpose=purpose,
            quantity=quantity,
            worst_acceptable_price=price,
        ),
        status=SimulatedOrderStatus.FILLED,
        actual_fill_quantity=quantity,
        fill_price=price,
        fee_usdt=fee,
    )


def _scan_payload(
    result: ScanResult,
    decisions: tuple[SignalDecision, ...] = (),
) -> dict[str, object]:
    return {
        "evaluated_at": datetime.now(UTC).isoformat(),
        "base": result.base,
        "common_instrument_count": result.common_instrument_count,
        "directed_route_count": result.directed_route_count,
        "bbo_prefilter": [asdict(observation) for observation in result.prefilter],
        "bbo_cache": asdict(result.bbo_cache) if result.bbo_cache is not None else None,
        "prefilter_latency_ms": (
            str(result.prefilter_latency_ms) if result.prefilter_latency_ms is not None else None
        ),
        "candidate_l2": asdict(result.candidate_l2) if result.candidate_l2 is not None else None,
        "route_calibration": [asdict(assessment) for assessment in result.route_calibration],
        "opportunities": [asdict(quote) for quote in result.quotes],
        "data_health": [asdict(quality) for quality in result.data_quality],
        "quarantined": [asdict(record) for record in result.quarantined],
        "decisions": [asdict(decision) for decision in decisions],
    }


class ContinuousShadowEvaluator:
    def __init__(
        self,
        settings: Settings,
        engine: MarketEngine | None = None,
        runtime: ShadowRuntime | None = None,
        trader: ShadowTrader | None = None,
        route_calibrator: PersistentRouteCalibrator | None = None,
    ) -> None:
        self.settings = settings
        self.runtime = runtime or ShadowRuntime(settings)
        self._engine = engine or PublicMarketEngine(settings)
        self._trader = trader or ShadowTrader(settings, self.runtime)
        self._route_calibrator = route_calibrator or PersistentRouteCalibrator(
            Path(settings.storage.sqlite_path),
            minimum_samples=(settings.shadow.qualification_min_synchronised_snapshots_per_venue),
            minimum_observation_period=timedelta(
                seconds=settings.shadow.qualification_min_duration_seconds
            ),
            minimum_profit_usdt=settings.strategy.minimum_profit_usdt or Decimal("0.01"),
            parameter_change_limit_ratio_per_day=(
                settings.strategy.grid_parameter_change_limit_ratio
            ),
            maximum_inter_observation_gap=timedelta(
                seconds=settings.shadow.qualification_max_inter_snapshot_gap_seconds
            ),
            sampling_policy=RouteCalibrationSamplingPolicy(
                settings.strategy.calibration_size_multipliers,
                settings.risk.max_hold_seconds,
                settings.strategy.calibration_funding_refresh_seconds,
                settings.execution.funding_stress_multiplier,
                settings.execution.latency_reserve_bps,
                settings.execution.partial_fill_reserve_bps,
                settings.execution.emergency_hedge_reserve_bps,
                settings.execution.reconciliation_forced_exit_reserve_bps,
            ),
            maximum_l2_age_ms=settings.market_data.max_l2_age_ms,
        )
        self._last_prefilter: tuple[BboPrefilterObservation, ...] = ()
        self._pending_route_calibration: (
            asyncio.Task[tuple[RouteCalibrationAssessment, ...]] | None
        ) = None
        self._pending_route_calibration_observed_at: datetime | None = None
        self._route_calibration_persisted_at: datetime | None = None
        self._route_calibration_persistence_healthy = False
        self._route_calibration_persistence_failure: str | None = None
        self._decision_latency_samples: deque[Decimal] = deque(maxlen=4096)

    def _decision_latency_p95(self) -> Decimal | None:
        if not self._decision_latency_samples:
            return None
        ordered = tuple(sorted(self._decision_latency_samples))
        position = Decimal("0.95") * Decimal(len(ordered) - 1)
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        fraction = position - Decimal(lower)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction

    def _harvest_route_calibration_task(self) -> None:
        task = self._pending_route_calibration
        if task is None or not task.done():
            return
        observed_at = self._pending_route_calibration_observed_at
        try:
            task.result()
        except (asyncio.CancelledError, Exception) as error:
            self._route_calibration_persistence_healthy = False
            self._route_calibration_persistence_failure = f"{type(error).__name__}: {error}"
        else:
            self._route_calibration_persistence_healthy = True
            self._route_calibration_persistence_failure = None
            self._route_calibration_persisted_at = observed_at
        self._pending_route_calibration = None
        self._pending_route_calibration_observed_at = None

    @staticmethod
    def _consume_route_calibration_task(
        task: asyncio.Task[tuple[RouteCalibrationAssessment, ...]],
    ) -> None:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            task.result()

    def _apply_public_workload(
        self,
        active_route_keys: frozenset[RouteStableKey],
    ) -> tuple[bool, bool]:
        workload = self._engine.public_workload()
        self.runtime.overload.update_pending_by_class(
            {
                WorkClass.RECONCILE: max(
                    workload.active_l2_tasks,
                    len(active_route_keys) * 2,
                ),
                WorkClass.CANDIDATE_L2: workload.candidate_l2_demand,
                WorkClass.BROAD_BBO: workload.broad_bbo_demand,
            }
        )
        shed = set(self.runtime.overload.shed_plan())
        return WorkClass.BROAD_BBO not in shed, WorkClass.CANDIDATE_L2 not in shed

    async def run(self, stop_event: asyncio.Event) -> None:
        logger = get_logger()
        await self.runtime.start()
        await self._route_calibrator.initialise()
        active_ids = {
            tranche.tranche_id
            for tranche in self.runtime.tranches.values()
            if tranche.state in ACTIVE_STATES
        }
        await self.runtime.reconcile(active_ids)
        live_journal = LiveOrderJournal(self.runtime.state_path)
        await live_journal.initialise()
        try:
            while not stop_event.is_set():
                try:
                    active_live_actions = await live_journal.active_actions()
                    if active_live_actions:
                        logger.info(
                            "shadow_suspended_for_live_recovery",
                            active_action_count=len(active_live_actions),
                            pair_action_ids=tuple(
                                action.pair_action_id for action in active_live_actions
                            ),
                        )
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(
                                stop_event.wait(),
                                timeout=self.settings.shadow.scan_interval_seconds,
                            )
                        continue
                    active_route_keys = frozenset(
                        (
                            tranche.route.base,
                            tranche.route.long_venue.value,
                            tranche.route.short_venue.value,
                        )
                        for tranche in self.runtime.tranches.values()
                        if tranche.state in ACTIVE_STATES
                    )
                    broad_admitted, candidates_admitted = self._apply_public_workload(
                        active_route_keys
                    )
                    if active_route_keys:
                        await self._engine.scan_candidate_l2(
                            self.settings.shadow.scan_timeout_seconds,
                            active_route_keys=active_route_keys,
                            candidates_admitted=candidates_admitted,
                            prefilter=self._last_prefilter,
                            preserve_existing_candidates=True,
                        )
                    await self._engine.set_broad_bbo_admitted(broad_admitted)
                    entry_work_admitted = self.runtime.overload.admit(WorkClass.NEW_ENTRY).accepted
                    result = await self._engine.scan_once(
                        self.settings.shadow.base,
                        self.settings.shadow.quantity,
                        self.settings.shadow.scan_timeout_seconds,
                        active_route_keys=active_route_keys,
                        entry_work_admitted=entry_work_admitted,
                    )
                    await self._trader.close_active(result.quotes)
                    active_route_keys = frozenset(
                        (
                            tranche.route.base,
                            tranche.route.long_venue.value,
                            tranche.route.short_venue.value,
                        )
                        for tranche in self.runtime.tranches.values()
                        if tranche.state in ACTIVE_STATES
                    )
                    self._last_prefilter = result.prefilter
                    candidate_l2 = await self._engine.scan_candidate_l2(
                        self.settings.shadow.scan_timeout_seconds,
                        active_route_keys=active_route_keys,
                        candidates_admitted=candidates_admitted,
                        prefilter=result.prefilter,
                    )
                    next_broad_admitted, next_candidates_admitted = self._apply_public_workload(
                        active_route_keys
                    )
                    if next_broad_admitted != broad_admitted:
                        await self._engine.set_broad_bbo_admitted(next_broad_admitted)
                    if candidates_admitted and not next_candidates_admitted:
                        candidate_l2 = await self._engine.scan_candidate_l2(
                            self.settings.shadow.scan_timeout_seconds,
                            active_route_keys=active_route_keys,
                            candidates_admitted=False,
                            prefilter=result.prefilter,
                        )
                    candidate_completed_at = asyncio.get_running_loop().time()
                    calibration_observations: tuple[RouteCalibrationObservation, ...] = ()
                    if active_route_keys or next_candidates_admitted:
                        calibration_epoch = await self._route_calibrator.current_epoch_id(
                            datetime.now(UTC)
                        )
                        calibration_observations = (
                            await self._engine.scan_route_calibration_observations(
                                self.settings.shadow.scan_timeout_seconds,
                                epoch_id=calibration_epoch,
                            )
                        )
                    route_calibration: tuple[RouteCalibrationAssessment, ...] = ()
                    self._harvest_route_calibration_task()
                    if self._route_calibration_persistence_failure is not None:
                        logger.error(
                            "route_calibration_persistence_failed",
                            reason=self._route_calibration_persistence_failure,
                        )
                    pending_calibration = self._pending_route_calibration
                    current_observed_at = (
                        max(item.observed_at for item in calibration_observations)
                        if calibration_observations
                        else None
                    )
                    if calibration_observations and pending_calibration is None:
                        calibration_task = asyncio.create_task(
                            self._route_calibrator.record_many(
                                calibration_observations,
                                now=current_observed_at,
                            ),
                            name="shadow-route-calibration",
                        )
                        self._pending_route_calibration = calibration_task
                        self._pending_route_calibration_observed_at = current_observed_at
                    loop = asyncio.get_running_loop()
                    elapsed_after_candidate = loop.time() - candidate_completed_at
                    remaining_decision_budget = max(
                        0.0,
                        0.240
                        - float(candidate_l2.decision_latency_ms) / 1000
                        - elapsed_after_candidate,
                    )
                    if calibration_observations and remaining_decision_budget > 0:
                        point_gate = asyncio.create_task(
                            self._route_calibrator.assess_current(calibration_observations),
                            name="shadow-route-calibration-current-gate",
                        )
                        done, _pending = await asyncio.wait(
                            (point_gate,),
                            timeout=remaining_decision_budget,
                        )
                        if point_gate in done:
                            route_calibration = point_gate.result()
                        else:
                            point_gate.cancel()
                            point_gate.add_done_callback(self._consume_route_calibration_task)
                    self._harvest_route_calibration_task()
                    persistence_fresh = (
                        self._route_calibration_persistence_healthy
                        and current_observed_at is not None
                        and self._route_calibration_persisted_at is not None
                        and current_observed_at >= self._route_calibration_persisted_at
                        and current_observed_at - self._route_calibration_persisted_at
                        <= timedelta(
                            seconds=(
                                self.settings.shadow.qualification_max_inter_snapshot_gap_seconds
                            )
                        )
                    )
                    pre_decision_latency_ms = max(
                        candidate_l2.decision_latency_ms,
                        candidate_l2.decision_latency_ms
                        + Decimal(
                            str((asyncio.get_running_loop().time() - candidate_completed_at) * 1000)
                        ),
                    )
                    receipt_started_at = candidate_completed_at - (
                        float(candidate_l2.decision_latency_ms) / 1000
                    )
                    decision_deadline = receipt_started_at + 0.250
                    if (
                        pre_decision_latency_ms > Decimal(250)
                        or not persistence_fresh
                        or asyncio.get_running_loop().time() >= decision_deadline
                    ):
                        route_calibration = ()
                    candidate_l2 = replace(
                        candidate_l2,
                        decision_latency_ms=pre_decision_latency_ms,
                    )
                    result = replace(
                        result,
                        candidate_l2=candidate_l2,
                        route_calibration=route_calibration,
                    )
                    decisions = await self._trader.process(
                        result,
                        decision_deadline=decision_deadline,
                    )
                    actual_decision_latency_ms = Decimal(
                        str((asyncio.get_running_loop().time() - receipt_started_at) * 1000)
                    )
                    self._decision_latency_samples.append(actual_decision_latency_ms)
                    candidate_l2 = replace(
                        candidate_l2,
                        decision_latency_ms=actual_decision_latency_ms,
                        stats=replace(
                            candidate_l2.stats,
                            decision_latency_p95_ms=self._decision_latency_p95(),
                        ),
                    )
                    result = replace(result, candidate_l2=candidate_l2)
                    await save_shadow_snapshot(
                        self.runtime.state_path,
                        _scan_payload(result, decisions),
                    )
                    epoch = await read_active_qualification_epoch(self.runtime.state_path)
                    if epoch is not None:
                        await record_qualification_scan(
                            self.runtime.state_path,
                            epoch.epoch_id,
                            result.base,
                            result.funding,
                            decisions,
                            tuple(self.runtime.tranches.values()),
                            self.settings.strategy.stressed_cost_multiplier,
                            min(3600, self.settings.risk.max_hold_seconds),
                            self.settings.risk.max_hold_seconds,
                        )
                    logger.info(
                        "shadow_evaluated",
                        base=result.base,
                        routes=len(result.quotes),
                        eligible=sum(1 for quote in result.quotes if quote.eligible),
                        quarantined=len(result.quarantined),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    epoch = await read_active_qualification_epoch(self.runtime.state_path)
                    if epoch is not None:
                        await record_qualification_exception(
                            self.runtime.state_path,
                            epoch.epoch_id,
                            type(error).__name__,
                        )
                    logger.error(
                        "shadow_evaluation_failed",
                        reason=type(error).__name__,
                    )
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self.settings.shadow.scan_interval_seconds,
                    )
                except TimeoutError:
                    continue
        finally:
            failures: list[str] = []
            try:
                await self._engine.close()
            except Exception as error:
                failures.append(f"public engine: {type(error).__name__}: {error}")
            pending_calibration = self._pending_route_calibration
            if pending_calibration is not None:
                if not pending_calibration.done():
                    done, _pending = await asyncio.wait((pending_calibration,), timeout=1)
                    if pending_calibration not in done:
                        pending_calibration.cancel()
                        cancelled, _still_pending = await asyncio.wait(
                            (pending_calibration,),
                            timeout=0.1,
                        )
                        if pending_calibration in cancelled:
                            if pending_calibration.cancelled():
                                self._pending_route_calibration = None
                                self._pending_route_calibration_observed_at = None
                            else:
                                self._harvest_route_calibration_task()
                        else:
                            self._pending_route_calibration = None
                            self._pending_route_calibration_observed_at = None
                            pending_calibration.add_done_callback(
                                self._consume_route_calibration_task
                            )
                            failures.append(
                                "route calibration persistence: shutdown deadline exceeded"
                            )
                if pending_calibration.done():
                    self._harvest_route_calibration_task()
            try:
                await self._route_calibrator.close()
            except Exception as error:
                failures.append(f"route calibrator: {type(error).__name__}: {error}")
            if self._route_calibration_persistence_failure is not None:
                failures.append(
                    f"route calibration persistence: {self._route_calibration_persistence_failure}"
                )
            if failures:
                raise RuntimeError(f"shadow shutdown failed: {'; '.join(failures)}")
