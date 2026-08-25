from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, localcontext
from enum import StrEnum

from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveDecisionPolicy,
    AggressiveEconomicDecision,
    AggressiveEntryReason,
    CostReserves,
    CrossingConfirmationTracker,
    GridSizingResult,
    HybridEntryInput,
    evaluate_hybrid_entry,
    size_aggressive_grid,
)
from interexchange_perp_grid.aggressive_grid import AggressiveGridStore, GridLevelState
from interexchange_perp_grid.aggressive_model import (
    DirectionHistoricalModel,
    DivergenceDirection,
    HistoricalReferenceModel,
    ModelEligibility,
    historical_model_sha256,
)

_BPS = Decimal("10000")


class AggressiveRuntimeMode(StrEnum):
    REPLAY = "REPLAY"
    SHADOW = "SHADOW"
    LIVE = "LIVE"


class ReplayMinuteOutcome(StrEnum):
    NONE = "NONE"
    TARGET = "TARGET"
    STOP = "STOP"


@dataclass(frozen=True, slots=True)
class AggressiveSizingInput:
    quantity_step: Decimal
    minimum_base_quantity: Decimal
    minimum_notional_usdt: Decimal
    per_full_base_reserve_usdt: Decimal
    existing_route_loss_usdt: Decimal
    existing_portfolio_loss_usdt: Decimal
    free_margin_usdt: Decimal


@dataclass(frozen=True, slots=True)
class AggressiveStrategyRequest:
    mode: AggressiveRuntimeMode
    model: HistoricalReferenceModel
    proposal: HybridEntryInput
    sizing: AggressiveSizingInput
    reserves_per_base: CostReserves
    effective_stop_bps: Decimal
    decision_cycle: int
    runtime_manifest_sha256: str
    state_reconciled: bool = True
    reference_identity_valid: bool = True

    def __post_init__(self) -> None:
        if self.decision_cycle < 0:
            raise ValueError("aggressive decision cycle must be non-negative")
        if not self.runtime_manifest_sha256:
            raise ValueError("aggressive runtime manifest identity is required")


@dataclass(frozen=True, slots=True)
class AggressiveTrancheIntent:
    base: str
    route_identity: str
    direction: DivergenceDirection
    level_index: int
    decision_cycle: int
    quantity: Decimal
    long_venue: str
    short_venue: str
    long_symbol: str
    short_symbol: str
    reference_trigger_bps: Decimal
    reference_spread_bps: Decimal
    executable_entry_spread_bps: Decimal
    reverse_target_bps: Decimal
    effective_stop_bps: Decimal
    long_entry_vwap: Decimal
    short_entry_vwap: Decimal
    projected_route_loss_usdt: Decimal
    projected_portfolio_loss_usdt: Decimal
    expected_net_pnl_usdt: Decimal
    model_sha256: str
    strategy_profile_sha256: str
    source_manifest_sha256: str
    reference_manifest_sha256: str
    runtime_manifest_sha256: str
    contract_metadata_version_a: str
    contract_metadata_version_b: str
    decided_at: datetime
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.decided_at.tzinfo is None or self.decided_at.utcoffset() is None:
            raise ValueError("aggressive intent timestamp must be timezone-aware")
        if not 1 <= self.level_index <= 5 or self.quantity <= 0:
            raise ValueError("aggressive intent level or quantity is invalid")
        identities = (
            self.base,
            self.route_identity,
            self.long_venue,
            self.short_venue,
            self.long_symbol,
            self.short_symbol,
            self.model_sha256,
            self.strategy_profile_sha256,
            self.source_manifest_sha256,
            self.reference_manifest_sha256,
            self.runtime_manifest_sha256,
            self.contract_metadata_version_a,
            self.contract_metadata_version_b,
        )
        if any(not value for value in identities):
            raise ValueError("aggressive intent identity is incomplete")
        numbers = (
            self.quantity,
            self.reference_trigger_bps,
            self.reference_spread_bps,
            self.executable_entry_spread_bps,
            self.reverse_target_bps,
            self.effective_stop_bps,
            self.long_entry_vwap,
            self.short_entry_vwap,
            self.projected_route_loss_usdt,
            self.projected_portfolio_loss_usdt,
            self.expected_net_pnl_usdt,
        )
        if any(not value.is_finite() for value in numbers):
            raise ValueError("aggressive intent contains a non-finite value")
        if self.execution_authorized:
            raise ValueError("strategy intent never authorizes exchange execution")

    @property
    def intent_id(self) -> str:
        material = "|".join(
            (
                self.route_identity,
                str(self.level_index),
                str(self.decision_cycle),
                str(self.quantity),
                self.model_sha256,
                self.runtime_manifest_sha256,
            )
        )
        return f"aggressive-{hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]}"


@dataclass(frozen=True, slots=True)
class AggressiveStrategyDecision:
    accepted: bool
    reason: AggressiveEntryReason
    economics: AggressiveEconomicDecision
    sizing: GridSizingResult
    intent: AggressiveTrancheIntent | None
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.accepted != (self.intent is not None):
            raise ValueError("accepted aggressive decision must own exactly one intent")
        if self.execution_authorized:
            raise ValueError("strategy decision never authorizes exchange execution")


@dataclass(frozen=True, slots=True)
class ActualFillRiskInput:
    direction: DivergenceDirection
    base_quantity: Decimal
    long_fill_price: Decimal
    short_fill_price: Decimal
    actual_fees_usdt: Decimal
    adverse_funding_usdt: Decimal
    other_reserves_usdt: Decimal
    effective_stop_bps: Decimal
    existing_route_loss_usdt: Decimal
    existing_portfolio_loss_usdt: Decimal


@dataclass(frozen=True, slots=True)
class ActualFillRiskResult:
    accepted: bool
    projected_route_loss_usdt: Decimal
    projected_portfolio_loss_usdt: Decimal
    actual_entry_spread_bps: Decimal


class AggressiveDecisionCore:
    """One deterministic decision core shared by replay, shadow and live intent creation."""

    def __init__(self, policy: AggressiveDecisionPolicy) -> None:
        self.policy = policy
        self._confirmations = CrossingConfirmationTracker(
            policy.confirmation_snapshots,
            policy.confirmation_minimum_elapsed_ms,
        )

    def evaluate(self, request: AggressiveStrategyRequest) -> AggressiveStrategyDecision:
        direction_model = _direction_model(request)
        expected_route = (
            request.model.positive_route
            if request.proposal.direction == DivergenceDirection.POSITIVE
            else request.model.negative_route
        )
        level_index = request.proposal.level_index
        reference_price = Decimal(1)
        if request.proposal.long_book.asks and request.proposal.short_book.bids:
            reference_price = (
                request.proposal.long_book.asks[0].price + request.proposal.short_book.bids[0].price
            ) / Decimal(2)
        sizing = size_aggressive_grid(
            direction_levels_bps=direction_model.levels_bps,
            tranche_weights=direction_model.tranche_weights,
            effective_stop_bps=request.effective_stop_bps,
            reference_price=reference_price,
            quantity_step=request.sizing.quantity_step,
            minimum_base_quantity=request.sizing.minimum_base_quantity,
            minimum_notional_usdt=request.sizing.minimum_notional_usdt,
            per_full_base_reserve_usdt=request.sizing.per_full_base_reserve_usdt,
            existing_route_loss_usdt=request.sizing.existing_route_loss_usdt,
            existing_portfolio_loss_usdt=request.sizing.existing_portfolio_loss_usdt,
            free_margin_usdt=request.sizing.free_margin_usdt,
            policy=self.policy,
        )
        quantity = (
            sizing.tranche_base_quantities[level_index - 1]
            if sizing.accepted and 1 <= level_index <= len(sizing.tranche_base_quantities)
            else request.proposal.quantity
        )
        grid_step = direction_model.range_bps / Decimal(5)
        geometry_matches = (
            1 <= level_index <= 5
            and request.proposal.route_identity == expected_route
            and request.proposal.reference_trigger_bps
            == direction_model.levels_bps[level_index - 1]
            and request.proposal.grid_step_bps == grid_step
            and request.proposal.normal_low_bps == request.model.normal_low_bps
            and request.proposal.normal_high_bps == request.model.normal_high_bps
        )
        live_eligible = direction_model.eligibility == ModelEligibility.LIVE_ELIGIBLE
        model_eligible = (
            direction_model.eligibility != ModelEligibility.DISABLED
            and (request.mode != AggressiveRuntimeMode.LIVE or live_eligible)
            and geometry_matches
            and request.reference_identity_valid
        )
        proposal = replace(
            request.proposal,
            quantity=quantity,
            reserves=_scale_reserves(request.reserves_per_base, quantity),
            state_reconciled=request.state_reconciled,
            historical_model_eligible=model_eligible,
            regime_ready=not direction_model.regime_drift_blocked,
        )
        economics = evaluate_hybrid_entry(
            proposal,
            policy=self.policy,
            confirmations=self._confirmations,
        )
        if not economics.accepted or not sizing.accepted:
            reason = (
                AggressiveEntryReason.RISK_REJECTED if not sizing.accepted else economics.reason
            )
            return AggressiveStrategyDecision(False, reason, economics, sizing, None)
        assert economics.executable_entry_spread_bps is not None
        assert economics.reverse_target_bps is not None
        assert economics.long_entry_vwap is not None
        assert economics.short_entry_vwap is not None
        intent = AggressiveTrancheIntent(
            base=request.model.base,
            route_identity=expected_route,
            direction=proposal.direction,
            level_index=level_index,
            decision_cycle=request.decision_cycle,
            quantity=quantity,
            long_venue=proposal.long_venue.value,
            short_venue=proposal.short_venue.value,
            long_symbol=proposal.long_book.symbol,
            short_symbol=proposal.short_book.symbol,
            reference_trigger_bps=proposal.reference_trigger_bps,
            reference_spread_bps=proposal.reference_spread_bps,
            executable_entry_spread_bps=economics.executable_entry_spread_bps,
            reverse_target_bps=economics.reverse_target_bps,
            effective_stop_bps=request.effective_stop_bps,
            long_entry_vwap=economics.long_entry_vwap,
            short_entry_vwap=economics.short_entry_vwap,
            projected_route_loss_usdt=sizing.projected_route_loss_usdt,
            projected_portfolio_loss_usdt=sizing.projected_portfolio_loss_usdt,
            expected_net_pnl_usdt=economics.expected_net_pnl_usdt,
            model_sha256=historical_model_sha256(request.model),
            strategy_profile_sha256=request.model.strategy_profile_sha256,
            source_manifest_sha256=request.model.source_manifest_sha256,
            reference_manifest_sha256=request.model.reference_manifest_sha256,
            runtime_manifest_sha256=request.runtime_manifest_sha256,
            contract_metadata_version_a=request.model.contract_metadata_version_a,
            contract_metadata_version_b=request.model.contract_metadata_version_b,
            decided_at=proposal.now,
        )
        return AggressiveStrategyDecision(
            True, AggressiveEntryReason.ACCEPTED, economics, sizing, intent
        )

    @staticmethod
    def reserve(store: AggressiveGridStore, decision: AggressiveStrategyDecision) -> None:
        if not decision.accepted or decision.intent is None:
            raise RuntimeError("only an accepted aggressive intent may reserve grid ownership")
        intent = decision.intent
        selected = store.first_unfilled_crossed_level(
            intent.route_identity,
            intent.reference_spread_bps,
        )
        if selected is None or selected.level_index != intent.level_index:
            raise RuntimeError("aggressive grid changed after decision")
        pending = store.reserve_entry(
            intent.route_identity,
            reference_spread_bps=intent.reference_spread_bps,
            decision_cycle=intent.decision_cycle,
            reserved_stress_usdt=intent.projected_route_loss_usdt,
            now=intent.decided_at,
        )
        if pending.state != GridLevelState.ENTRY_PENDING:
            raise RuntimeError("aggressive grid reservation did not become entry-pending")


def recompute_actual_fill_risk(
    fill: ActualFillRiskInput,
    policy: AggressiveDecisionPolicy,
) -> ActualFillRiskResult:
    values = (
        fill.base_quantity,
        fill.long_fill_price,
        fill.short_fill_price,
        fill.actual_fees_usdt,
        fill.adverse_funding_usdt,
        fill.other_reserves_usdt,
        fill.effective_stop_bps,
        fill.existing_route_loss_usdt,
        fill.existing_portfolio_loss_usdt,
    )
    if any(not value.is_finite() for value in values):
        raise ValueError("actual-fill risk contains a non-finite value")
    if (
        fill.base_quantity <= 0
        or fill.long_fill_price <= 0
        or fill.short_fill_price <= 0
        or min(fill.actual_fees_usdt, fill.adverse_funding_usdt, fill.other_reserves_usdt) < 0
    ):
        raise ValueError("actual-fill risk contains an invalid amount")
    with localcontext() as context:
        context.prec = 50
        spread = (fill.short_fill_price / fill.long_fill_price).ln() * _BPS
    average_price = (fill.long_fill_price + fill.short_fill_price) / Decimal(2)
    market_loss = fill.base_quantity * average_price * abs(fill.effective_stop_bps - spread) / _BPS
    incremental = (
        market_loss + fill.actual_fees_usdt + fill.adverse_funding_usdt + fill.other_reserves_usdt
    )
    route = fill.existing_route_loss_usdt + incremental
    portfolio = fill.existing_portfolio_loss_usdt + incremental
    return ActualFillRiskResult(
        accepted=(
            route <= policy.route_modelled_loss_limit_usdt
            and route < policy.route_hard_projected_loss_limit_usdt
            and portfolio <= policy.portfolio_modelled_loss_limit_usdt
            and portfolio < policy.portfolio_hard_projected_loss_limit_usdt
        ),
        projected_route_loss_usdt=route,
        projected_portfolio_loss_usdt=portfolio,
        actual_entry_spread_bps=spread,
    )


def worst_case_replay_minute(
    direction: DivergenceDirection,
    *,
    minute_high_bps: Decimal,
    minute_low_bps: Decimal,
    reverse_target_bps: Decimal,
    effective_stop_bps: Decimal,
) -> ReplayMinuteOutcome:
    values = (minute_high_bps, minute_low_bps, reverse_target_bps, effective_stop_bps)
    if any(not value.is_finite() for value in values) or minute_low_bps > minute_high_bps:
        raise ValueError("replay minute envelope is invalid")
    if direction == DivergenceDirection.POSITIVE:
        stop_hit = minute_high_bps >= effective_stop_bps
        target_hit = minute_low_bps <= reverse_target_bps
    else:
        stop_hit = minute_low_bps <= effective_stop_bps
        target_hit = minute_high_bps >= reverse_target_bps
    if stop_hit:
        return ReplayMinuteOutcome.STOP
    if target_hit:
        return ReplayMinuteOutcome.TARGET
    return ReplayMinuteOutcome.NONE


def validate_live_intent(
    intent: AggressiveTrancheIntent,
    *,
    expected_model_sha256: str,
    expected_profile_sha256: str,
    expected_runtime_manifest_sha256: str,
) -> AggressiveTrancheIntent:
    if (
        intent.model_sha256 != expected_model_sha256
        or intent.strategy_profile_sha256 != expected_profile_sha256
        or intent.runtime_manifest_sha256 != expected_runtime_manifest_sha256
    ):
        raise RuntimeError("aggressive live intent identity mismatch")
    if intent.execution_authorized:
        raise RuntimeError("aggressive strategy intent cannot authorize live execution")
    return intent


def _direction_model(request: AggressiveStrategyRequest) -> DirectionHistoricalModel:
    return (
        request.model.positive
        if request.proposal.direction == DivergenceDirection.POSITIVE
        else request.model.negative
    )


def _scale_reserves(reserves: CostReserves, quantity: Decimal) -> CostReserves:
    return replace(
        reserves,
        entry_impact_usdt=reserves.entry_impact_usdt * quantity,
        exit_impact_usdt=reserves.exit_impact_usdt * quantity,
        entry_slippage_usdt=reserves.entry_slippage_usdt * quantity,
        exit_slippage_usdt=reserves.exit_slippage_usdt * quantity,
        latency_usdt=reserves.latency_usdt * quantity,
        partial_fill_unmatched_usdt=reserves.partial_fill_unmatched_usdt * quantity,
        emergency_hedge_usdt=reserves.emergency_hedge_usdt * quantity,
        reconciliation_forced_exit_usdt=(reserves.reconciliation_forced_exit_usdt * quantity),
        liquidation_distance_usdt=reserves.liquidation_distance_usdt * quantity,
    )
