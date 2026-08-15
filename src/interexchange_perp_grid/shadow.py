from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

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
from interexchange_perp_grid.public_engine import PublicMarketEngine, ScanResult
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import (
    RiskBook,
    RiskLimits,
    RiskRequest,
    VenueProjection,
)
from interexchange_perp_grid.routes import DirectedRouteQuote
from interexchange_perp_grid.state import (
    RuntimeControls,
    initialise_state,
    load_tranches,
    read_active_qualification_epoch,
    read_runtime_controls,
    read_shadow_snapshot,
    record_qualification_exception,
    record_qualification_scan,
    save_shadow_snapshot,
    save_tranche,
    update_runtime_controls,
)
from interexchange_perp_grid.strategy import (
    AdaptiveGridCalibrator,
    CalibrationObservation,
    CostInputs,
    DirectedRouteKey,
    SignalDecision,
    evaluate_entry_signal,
)


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
    ) -> ScanResult: ...

    async def close(self) -> None: ...


CRITICAL_WORK = {
    WorkClass.CLOSE,
    WorkClass.HEDGE,
    WorkClass.RECONCILE,
    WorkClass.PRIVATE_STREAM,
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

    @property
    def overloaded(self) -> bool:
        return self._pending > self._pending_limit

    def update_pending(self, pending: int) -> None:
        if pending < 0:
            raise ValueError("pending work cannot be negative")
        self._pending = pending

    def admit(self, work: WorkClass) -> AdmissionDecision:
        if not self.overloaded or work in CRITICAL_WORK:
            return AdmissionDecision(True, ReasonCode.SHADOW_EVALUATED)
        return AdmissionDecision(False, ReasonCode.OVERLOAD_ENTRY_DISABLED)


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
        self.tranches: dict[str, Tranche] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        await initialise_state(self.state_path)
        restored = await load_tranches(self.state_path)
        self.tranches = {tranche.tranche_id: tranche for tranche in restored}
        active = tuple(tranche for tranche in restored if tranche.state in ACTIVE_STATES)
        await update_runtime_controls(
            self.state_path,
            reconciliation_state="PENDING" if active else "CONSISTENT",
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
        consistent = expected == observed_active_ids and structurally_valid
        await update_runtime_controls(
            self.state_path,
            reconciliation_state="CONSISTENT" if consistent else "INCONSISTENT",
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

    async def close_all_simulated(self) -> tuple[str, ...]:
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
            await save_tranche(self.state_path, tranche)
            closed.append(tranche.tranche_id)
        await self.pause()
        return tuple(closed)

    async def snapshot(self) -> dict[str, object]:
        controls = await self.controls()
        persisted = await read_shadow_snapshot(self.state_path)
        return {
            "mode": self.settings.app.mode,
            "paused": controls.paused,
            "killed": controls.killed,
            "reconciliation_state": controls.reconciliation_state,
            "overloaded": self.overload.overloaded,
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
        self._calibrator = AdaptiveGridCalibrator(
            minimum_samples=5,
            parameter_change_limit_ratio=settings.strategy.grid_parameter_change_limit_ratio,
        )
        self._risk = RiskBook(
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
        self._coordinator = PairExecutionCoordinator()
        self._observations: dict[
            tuple[DirectedRouteKey, Decimal], list[CalibrationObservation]
        ] = {}
        self._managed_ids: set[str] = set()

    async def process(self, result: ScanResult) -> tuple[SignalDecision, ...]:
        await self._close_converged(result.quotes)
        gate = await self.runtime.entry_gate()
        if not gate.accepted:
            return ()
        restored_active = any(
            tranche.state in ACTIVE_STATES and tranche.tranche_id not in self._managed_ids
            for tranche in self.runtime.tranches.values()
        )
        if restored_active:
            return ()
        decisions: list[SignalDecision] = []
        for quote in result.quotes:
            decision = await self._evaluate_and_open(quote)
            if decision is not None:
                decisions.append(decision)
        return tuple(decisions)

    async def _evaluate_and_open(
        self,
        quote: DirectedRouteQuote,
    ) -> SignalDecision | None:
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
        calibration_key = (route, quote.base_quantity)
        window = self._observations.setdefault(calibration_key, [])
        window.append(CalibrationObservation(quote.entry_spread_bps, None))
        del window[:-500]
        if len(window) < 5:
            return None

        midpoint_notional = (
            quote.base_quantity * (quote.entry_long_vwap + quote.entry_short_vwap) / Decimal(2)
        )
        if midpoint_notional <= 0:
            return None
        cost_floor_bps = quote.four_leg_fee_estimate / midpoint_notional * Decimal(10_000)
        minimum_profit = self.settings.strategy.minimum_profit_usdt or Decimal("0.01")
        parameters = self._calibrator.calibrate(
            route,
            quote.base_quantity,
            tuple(window),
            cost_floor_bps,
            Decimal("0.5"),
            minimum_profit,
        )
        reserve_unit = midpoint_notional * Decimal("0.0002")
        expected_funding_cost = max(
            Decimal(0),
            -quote.funding_rate_delta * midpoint_notional,
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
            funding_stress_usdt=abs(quote.funding_rate_delta) * midpoint_notional * 2,
            latency_reserve_usdt=reserve_unit,
            unmatched_hedge_reserve_usdt=reserve_unit * 2,
            reconciliation_forced_exit_reserve_usdt=reserve_unit,
            liquidation_distance_reserve_usdt=reserve_unit,
            precomputed_four_leg_fee_usdt=quote.four_leg_fee_estimate,
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
        projected_stress = (
            preliminary.cost.stressed_total_cost_usdt
            + midpoint_notional * parameters.grid_step_bps * 5 / Decimal(10_000)
        )
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
        risk = self._risk.reserve(request)
        decision = evaluate_entry_signal(
            inputs,
            parameters,
            self.settings.strategy.stressed_cost_multiplier,
            risk.accepted,
            risk.reason,
            risk.breakdown,
        )
        if not decision.accepted:
            return decision

        tranche = Tranche(
            tranche_id=tranche_id,
            route=route,
            requested_quantity=quote.base_quantity,
            target_close_spread=parameters.exit_quantile_bps,
            stop_spread=quote.entry_spread_bps + parameters.grid_step_bps * 5,
            projected_stress_usdt=projected_stress,
        )
        self._coordinator.precheck_and_reserve(tranche, risk)
        entry_fee = quote.four_leg_fee_estimate / 4
        self._coordinator.submit_open(
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
        )
        self.runtime.tranches[tranche_id] = tranche
        self._managed_ids.add(tranche_id)
        await save_tranche(self.runtime.state_path, tranche)
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
            await save_tranche(self.runtime.state_path, tranche)
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
    ) -> None:
        self.settings = settings
        self.runtime = runtime or ShadowRuntime(settings)
        self._engine = engine or PublicMarketEngine(settings)
        self._trader = trader or ShadowTrader(settings, self.runtime)

    async def run(self, stop_event: asyncio.Event) -> None:
        logger = get_logger()
        await self.runtime.start()
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
                    active_live_action = await live_journal.active()
                    if active_live_action is not None:
                        logger.info(
                            "shadow_suspended_for_live_recovery",
                            pair_action_id=active_live_action.pair_action_id,
                            state=active_live_action.state.value,
                        )
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(
                                stop_event.wait(),
                                timeout=self.settings.shadow.scan_interval_seconds,
                            )
                        continue
                    result = await self._engine.scan_once(
                        self.settings.shadow.base,
                        self.settings.shadow.quantity,
                        self.settings.shadow.scan_timeout_seconds,
                    )
                    decisions = await self._trader.process(result)
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
            await self._engine.close()
